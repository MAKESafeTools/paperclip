"""
Transparent proxy between Claude Code CLI and api.anthropic.com that adds 1h
cache_control breakpoints on system + tools and the extended-cache-ttl beta header.

Pointed at by the CLI via ANTHROPIC_BASE_URL. Auth headers from the incoming request
(Authorization / x-api-key / anthropic-auth-token) pass through verbatim so OAuth /
Max-subscription billing is preserved.

Captured /traffic exchanges include a top-level `session_id` extracted from the
incoming request (`x-claude-code-session-id` header, with metadata.user_id fallback).
This is consumed by sibling services like viz-sidecar to group calls by session.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

TARGET_URL = os.environ.get("TARGET_URL", "https://api.anthropic.com").rstrip("/")
CACHE_TTL = os.environ.get("CACHE_TTL", "1h")
INJECT_CACHE_CONTROL = os.environ.get("INJECT_CACHE_CONTROL", "false").lower() == "true"
INJECT_SYSTEM = os.environ.get("INJECT_SYSTEM", "true").lower() == "true"
INJECT_TOOLS = os.environ.get("INJECT_TOOLS", "true").lower() == "true"
MIN_SYSTEM_TOKENS = int(os.environ.get("MIN_SYSTEM_TOKENS", "1024"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BETA_FLAG = "extended-cache-ttl-2025-04-11"
TRAFFIC_BUFFER_SIZE = int(os.environ.get("TRAFFIC_BUFFER_SIZE", "500"))
TRAFFIC_MAX_BODY_BYTES = int(os.environ.get("TRAFFIC_MAX_BODY_BYTES", "2000000"))
REDACTED_HEADERS = {"authorization", "x-api-key", "anthropic-auth-token", "cookie", "proxy-authorization"}
HOP_BY_HOP = {"host", "content-length", "connection", "transfer-encoding"}
# We negotiate compression with upstream ourselves (identity) so the body bytes we
# forward to the client are never mislabeled. See _filter_headers().

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cache-proxy")

app = FastAPI()
_client: httpx.AsyncClient | None = None

# Snapshot of the most recent tools array observed on /v1/messages, plus a
# rolling per-(model,tool_count) capture so curling /tools-snapshot returns
# something useful even if the very last request was unrepresentative.
_last_snapshot: dict[str, Any] = {}
_snapshots_by_shape: dict[tuple[str, int], dict[str, Any]] = {}


def _extract_tool_names(tools: Any) -> list[str]:
    if not isinstance(tools, list):
        return []
    names: list[str] = []
    for tool in tools:
        if isinstance(tool, dict):
            name = tool.get("name")
            if isinstance(name, str) and name:
                names.append(name)
    return names


def _group_tools_by_server(names: list[str]) -> dict[str, list[str]]:
    """Group tool names by MCP server prefix. Built-ins land under 'builtin'.
    MCP names are 'mcp__<server>__<tool>' — group key is the <server> part."""
    grouped: dict[str, list[str]] = {}
    for name in names:
        if name.startswith("mcp__"):
            rest = name[len("mcp__"):]
            sep = rest.find("__")
            server = rest[:sep] if sep > 0 else rest
        else:
            server = "builtin"
        grouped.setdefault(server, []).append(name)
    for server in grouped:
        grouped[server].sort()
    return dict(sorted(grouped.items(), key=lambda kv: (kv[0] != "builtin", kv[0])))


@app.on_event("startup")
async def _startup() -> None:
    global _client
    _client = httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=10.0))
    log.info(
        "cache-proxy starting target=%s inject=%s ttl=%s system=%s tools=%s min_sys_tokens=%d",
        TARGET_URL, INJECT_CACHE_CONTROL, CACHE_TTL, INJECT_SYSTEM, INJECT_TOOLS, MIN_SYSTEM_TOKENS,
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _client is not None:
        await _client.aclose()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tools-snapshot")
async def tools_snapshot(shape: str | None = None, format: str = "json") -> Response:
    """Returns the most recent tool list seen on /v1/messages, grouped by MCP server.

    Query params:
      - shape: "<model>:<tool_count>" to fetch a specific previously-seen snapshot
               (e.g. "claude-sonnet-4-6:328"). Useful when the most recent request
               was an unrepresentative small/probe call.
      - format: "json" (default) or "text" for a sorted human-readable listing.
    """
    if shape:
        try:
            model_key, count_str = shape.rsplit(":", 1)
            key = (model_key, int(count_str))
        except (ValueError, IndexError):
            return JSONResponse({"error": "shape must be '<model>:<tool_count>'"}, status_code=400)
        snapshot = _snapshots_by_shape.get(key)
    else:
        snapshot = _last_snapshot

    if not snapshot:
        msg = "no snapshot yet — fire a heartbeat through the proxy first"
        return JSONResponse({"error": msg}, status_code=404)

    if format == "text":
        lines = [
            f"# captured_at={snapshot['captured_at']}",
            f"# model={snapshot['model']} tool_count={snapshot['tool_count']}",
            "",
        ]
        for server, names in snapshot["by_server"].items():
            lines.append(f"## {server} ({len(names)})")
            for name in names:
                lines.append(name)
            lines.append("")
        available = sorted(_snapshots_by_shape.keys(), key=lambda k: (k[0], k[1]))
        if available:
            lines.append("# other observed shapes (use ?shape=model:count):")
            for m, n in available:
                lines.append(f"#   {m}:{n}")
        return Response("\n".join(lines), media_type="text/plain")

    return JSONResponse({
        **snapshot,
        "available_shapes": [f"{m}:{n}" for m, n in sorted(_snapshots_by_shape.keys())],
    })


def _filter_headers(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP}


def _force_identity_encoding(headers: dict[str, str]) -> dict[str, str]:
    """Override Accept-Encoding to identity so upstream never gzips. Removes any
    existing accept-encoding header case-insensitively."""
    out = {k: v for k, v in headers.items() if k.lower() != "accept-encoding"}
    out["accept-encoding"] = "identity"
    return out


def _strip_response_encoding(headers: dict[str, str]) -> dict[str, str]:
    """Drop content-encoding from response headers — we forced identity upstream,
    but be defensive in case upstream ignores it. Also drop content-length since
    streaming bodies don't have a fixed length and buffered bodies may have changed."""
    return {k: v for k, v in headers.items()
            if k.lower() not in {"content-encoding", "content-length"}}


def _merge_beta(headers: dict[str, str]) -> dict[str, str]:
    out = dict(headers)
    existing = None
    for k in list(out.keys()):
        if k.lower() == "anthropic-beta":
            existing = out.pop(k)
    if existing:
        parts = [p.strip() for p in existing.split(",") if p.strip()]
        if BETA_FLAG not in parts:
            parts.append(BETA_FLAG)
        out["anthropic-beta"] = ",".join(parts)
    else:
        out["anthropic-beta"] = BETA_FLAG
    return out


def _set_cache_control(block: dict[str, Any]) -> None:
    block["cache_control"] = {"type": "ephemeral", "ttl": CACHE_TTL}


def _approx_tokens(text: str) -> int:
    return len(text) // 4


def _system_size(system: Any) -> int:
    if isinstance(system, str):
        return _approx_tokens(system)
    if isinstance(system, list):
        total = 0
        for b in system:
            if isinstance(b, dict):
                t = b.get("text") or ""
                if isinstance(t, str):
                    total += _approx_tokens(t)
        return total
    return 0


def _had_existing_cache_control(body: dict[str, Any]) -> bool:
    sys = body.get("system")
    if isinstance(sys, list):
        for b in sys:
            if isinstance(b, dict) and "cache_control" in b:
                return True
    tools = body.get("tools")
    if isinstance(tools, list):
        for t in tools:
            if isinstance(t, dict) and "cache_control" in t:
                return True
    return False


def _upgrade_existing(body: dict[str, Any]) -> tuple[int, int]:
    """Upgrade every existing cache_control marker to the configured TTL.
    Returns (system_upgraded, tools_upgraded) for logging.

    Strategy: don't add new breakpoints — Anthropic caps at 4 per request and
    Claude Code already places its own (typically at the limit). Just extend the
    TTL on what's there, which is exactly the win we want (5m -> 1h amortization)
    without risking the 4-breakpoint cap.
    """
    sys_n = 0
    tools_n = 0

    sys = body.get("system")
    if isinstance(sys, list):
        for block in sys:
            if isinstance(block, dict) and "cache_control" in block:
                _set_cache_control(block)
                sys_n += 1

    tools = body.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            if isinstance(tool, dict) and "cache_control" in tool:
                _set_cache_control(tool)
                tools_n += 1

    return sys_n, tools_n


def _seed_breakpoints(body: dict[str, Any]) -> tuple[str, str]:
    """Fallback when no cache_control markers exist. Adds at most 2 (system[-1]
    and tools[-1]) subject to MIN_SYSTEM_TOKENS. Only called when had_cc=False."""
    sys_status = "skipped-disabled"
    tools_status = "skipped-disabled"

    if INJECT_SYSTEM:
        sys = body.get("system")
        if isinstance(sys, list) and sys:
            if _system_size(sys) < MIN_SYSTEM_TOKENS:
                sys_status = f"skipped<{MIN_SYSTEM_TOKENS}toks"
            else:
                _set_cache_control(sys[-1])
                sys_status = f"seeded_last_block ttl={CACHE_TTL}"
        elif isinstance(sys, str):
            sys_status = "skipped-string-form"
        else:
            sys_status = "skipped-no-system"

    if INJECT_TOOLS:
        tools = body.get("tools")
        if isinstance(tools, list) and tools and isinstance(tools[-1], dict):
            _set_cache_control(tools[-1])
            tools_status = f"seeded_last_tool ttl={CACHE_TTL}"
        else:
            tools_status = "skipped-no-tools"

    return sys_status, tools_status


def _extract_usage(obj: Any) -> dict[str, Any] | None:
    if not isinstance(obj, dict):
        return None
    usage = obj.get("usage")
    if isinstance(usage, dict):
        return {
            "in": usage.get("input_tokens"),
            "out": usage.get("output_tokens"),
            "cache_read": usage.get("cache_read_input_tokens"),
            "cache_create": usage.get("cache_creation_input_tokens"),
        }
    return None


def _extract_session_id(req_headers: dict[str, str], req_body_json: Any) -> str | None:
    """Pull the Claude Code session identifier out of an incoming request.
    Tries the dedicated header first, then the metadata.user_id blob the SDK
    embeds in the body. Returned value is the raw session UUID."""
    for k, v in req_headers.items():
        if k.lower() == "x-claude-code-session-id" and isinstance(v, str) and v:
            return v
    if isinstance(req_body_json, dict):
        meta = req_body_json.get("metadata")
        if isinstance(meta, dict):
            uid = meta.get("user_id")
            if isinstance(uid, str):
                try:
                    parsed = json.loads(uid)
                except (json.JSONDecodeError, ValueError):
                    parsed = None
                if isinstance(parsed, dict):
                    sid = parsed.get("session_id")
                    if isinstance(sid, str) and sid:
                        return sid
    return None


# ---------------------------------------------------------------------------
# Traffic capture
# ---------------------------------------------------------------------------

_exchanges: deque = deque(maxlen=TRAFFIC_BUFFER_SIZE)


def _redact(headers: dict[str, str]) -> dict[str, str]:
    return {k: ("***redacted***" if k.lower() in REDACTED_HEADERS else v) for k, v in headers.items()}


def _truncate_text(data: bytes | str, limit: int = TRAFFIC_MAX_BODY_BYTES) -> tuple[str, bool]:
    if isinstance(data, bytes):
        text = data.decode("utf-8", errors="replace")
    else:
        text = data
    if len(text) > limit:
        return text[:limit] + f"\n...[truncated {len(text) - limit} bytes]", True
    return text, False


def _try_parse_json(data: bytes | str) -> Any:
    if isinstance(data, bytes):
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError:
            return None
    try:
        return json.loads(data)
    except (json.JSONDecodeError, ValueError):
        return None


def _reconstruct_assistant_text(sse_text: str) -> str:
    """Walk SSE events and concatenate text deltas so the UI can show a
    readable assistant reply instead of just raw event-stream chunks."""
    parts: list[str] = []
    for line in sse_text.split("\n"):
        line = line.rstrip("\r")
        if not line.startswith("data: "):
            continue
        payload = line[6:].strip()
        if not payload or payload == "[DONE]":
            continue
        try:
            evt = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            continue
        if evt.get("type") == "content_block_delta":
            delta = evt.get("delta") or {}
            t = delta.get("text") or delta.get("partial_json") or ""
            if isinstance(t, str) and t:
                parts.append(t)
    return "".join(parts)


class Recorder:
    """Captures one request/response exchange and appends it to the ring buffer."""

    def __init__(self, *, method: str, path: str, req_headers: dict[str, str],
                 req_body: bytes, model: Any, streaming: bool) -> None:
        self.id = uuid.uuid4().hex[:12]
        self.ts = datetime.now(timezone.utc).isoformat()
        self.t0 = time.perf_counter()
        self.method = method
        self.path = path
        self.req_headers = _redact(req_headers)
        body_text, truncated = _truncate_text(req_body)
        self.req_body_text = body_text
        self.req_body_truncated = truncated
        self.req_body_json = _try_parse_json(req_body)
        self.model = model
        self.streaming = streaming
        # Extract before redaction (the session header is non-secret but lives
        # alongside auth headers, so easier to grab from the raw bag).
        self.session_id = _extract_session_id(req_headers, self.req_body_json)

    def finish(self, *, status: int | None, resp_headers: dict[str, str] | None,
               resp_body: bytes | None = None, sse_text: str | None = None,
               usage: dict[str, Any] | None = None, error: str | None = None) -> None:
        duration_ms = int((time.perf_counter() - self.t0) * 1000)
        resp_body_text = ""
        resp_body_truncated = False
        resp_body_json: Any = None
        assistant_text = ""
        if sse_text is not None:
            resp_body_text, resp_body_truncated = _truncate_text(sse_text)
            assistant_text = _reconstruct_assistant_text(sse_text)
        elif resp_body is not None:
            resp_body_text, resp_body_truncated = _truncate_text(resp_body)
            resp_body_json = _try_parse_json(resp_body)
        record = {
            "id": self.id,
            "ts": self.ts,
            "method": self.method,
            "path": self.path,
            "model": self.model,
            "streaming": self.streaming,
            "status": status,
            "duration_ms": duration_ms,
            "usage": usage,
            "error": error,
            "session_id": self.session_id,
            "request": {
                "headers": self.req_headers,
                "body_json": self.req_body_json,
                "body_text": self.req_body_text if self.req_body_json is None else None,
                "truncated": self.req_body_truncated,
            },
            "response": {
                "headers": dict(resp_headers or {}),
                "body_json": resp_body_json,
                "body_text": resp_body_text if resp_body_json is None else None,
                "assistant_text": assistant_text or None,
                "truncated": resp_body_truncated,
            },
        }
        _exchanges.appendleft(record)


TRAFFIC_HTML = """<!doctype html>
<html><head><meta charset=\"utf-8\"><title>cache-proxy traffic</title>
<style>
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 0; background: #0f1115; color: #ddd; }
.bar { padding: 8px 12px; background: #181a20; border-bottom: 1px solid #2a2d36; display: flex; gap: 16px; align-items: center; position: sticky; top: 0; z-index: 10; }
.bar h1 { font-size: 13px; margin: 0; font-weight: 600; letter-spacing: 0.5px; }
.bar .meta { color: #888; font-size: 12px; }
.bar label { color: #aaa; font-size: 12px; cursor: pointer; user-select: none; }
.bar input[type=text] { background: #0a0c11; border: 1px solid #2a2d36; color: #ddd; padding: 4px 8px; border-radius: 3px; font: inherit; font-size: 12px; width: 180px; }
.list { padding: 0; }
.exch { border-bottom: 1px solid #1d1f26; }
.exch > summary { padding: 6px 12px; cursor: pointer; display: grid; grid-template-columns: 70px 50px 110px 1fr 240px 70px; gap: 12px; align-items: center; font-size: 12px; list-style: none; }
.exch > summary::-webkit-details-marker { display: none; }
.exch > summary:hover { background: #161821; }
.exch[open] > summary { background: #14161e; }
.col-time { color: #888; }
.col-status.s-2 { color: #4ade80; }
.col-status.s-4, .col-status.s-5 { color: #f87171; }
.col-method { color: #60a5fa; }
.tag { font-size: 10px; padding: 1px 5px; border-radius: 2px; background: #2a2d36; color: #bbb; margin-left: 4px; }
.col-path { color: #ddd; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.col-path .model { color: #c4b5fd; margin-left: 8px; font-size: 11px; }
.col-usage { color: #888; font-size: 11px; text-align: right; }
.col-duration { color: #888; text-align: right; }
.detail { padding: 12px 20px 16px; background: #0c0e14; border-top: 1px solid #1d1f26; }
.detail h3 { margin: 14px 0 4px; font-size: 10px; text-transform: uppercase; letter-spacing: 0.6px; color: #999; font-weight: 600; }
.detail h3:first-child { margin-top: 0; }
.detail pre { background: #06080c; padding: 10px 12px; border-radius: 4px; overflow: auto; max-height: 500px; font-size: 11px; margin: 0; line-height: 1.45; white-space: pre-wrap; word-break: break-word; }
.empty { padding: 40px; text-align: center; color: #666; font-size: 13px; }
</style>
</head><body>
<div class=\"bar\">
  <h1>CACHE-PROXY TRAFFIC</h1>
  <span class=\"meta\" id=\"meta\">loading…</span>
  <input type=\"text\" id=\"filter\" placeholder=\"filter (model, path, status)…\">
  <label><input type=\"checkbox\" id=\"live\" checked> live</label>
  <label><input type=\"checkbox\" id=\"autoexpand\"> auto-expand newest</label>
</div>
<div class=\"list\" id=\"list\"></div>
<script>
const $list = document.getElementById('list');
const $meta = document.getElementById('meta');
const $live = document.getElementById('live');
const $filter = document.getElementById('filter');
const $autoexpand = document.getElementById('autoexpand');
const expanded = new Set();
let knownIds = new Set();

const fmt = n => (n == null ? '·' : Number(n).toLocaleString());
const escapeHtml = s => String(s == null ? '' : s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const statusCls = s => s ? 's-' + String(s)[0] : '';

function matchesFilter(r, q) {
  if (!q) return true;
  q = q.toLowerCase();
  return [r.path, r.model, r.status, r.method].some(v => String(v ?? '').toLowerCase().includes(q));
}

async function tick() {
  if (!$live.checked) return;
  try {
    const r = await fetch('/traffic/exchanges');
    const data = await r.json();
    $meta.textContent = `${data.count} captured · buffer ${data.buffer_size}`;
    render(data.exchanges);
  } catch (e) {
    $meta.textContent = 'error: ' + e;
  }
}

function render(rows) {
  const q = $filter.value.trim();
  const filtered = rows.filter(r => matchesFilter(r, q));
  const newIds = new Set(filtered.map(r => r.id));
  const firstNew = filtered.find(r => !knownIds.has(r.id));
  $list.innerHTML = '';
  if (filtered.length === 0) {
    $list.innerHTML = '<div class=\"empty\">no exchanges captured yet · fire a request through the proxy</div>';
    knownIds = newIds;
    return;
  }
  for (const r of filtered) {
    const det = document.createElement('details');
    det.className = 'exch';
    det.dataset.id = r.id;
    if (expanded.has(r.id)) det.open = true;
    det.addEventListener('toggle', () => {
      if (det.open) {
        expanded.add(r.id);
        if (!det.dataset.loaded) loadDetail(r.id, det);
      } else {
        expanded.delete(r.id);
      }
    });
    const time = (r.ts || '').replace('T', ' ').slice(11, 19);
    const usage = r.usage
      ? `in ${fmt(r.usage.in)} · out ${fmt(r.usage.out)} · cR ${fmt(r.usage.cache_read)} · cC ${fmt(r.usage.cache_create)}`
      : '·';
    det.innerHTML = `
      <summary>
        <span class=\"col-time\">${time}</span>
        <span class=\"col-status ${statusCls(r.status)}\">${r.status ?? '…'}</span>
        <span class=\"col-method\">${escapeHtml(r.method)}${r.streaming ? '<span class=\"tag\">SSE</span>' : ''}</span>
        <span class=\"col-path\">${escapeHtml(r.path)}<span class=\"model\">${escapeHtml(r.model ?? '')}</span></span>
        <span class=\"col-usage\">${usage}</span>
        <span class=\"col-duration\">${r.duration_ms ?? '·'} ms</span>
      </summary>
      <div class=\"detail\">loading…</div>
    `;
    $list.appendChild(det);
  }
  if ($autoexpand.checked && firstNew && knownIds.size > 0) {
    const node = $list.querySelector(`[data-id=\"${firstNew.id}\"]`);
    if (node && !node.open) node.open = true;
  }
  knownIds = newIds;
}

async function loadDetail(id, det) {
  const slot = det.querySelector('.detail');
  try {
    const r = await fetch('/traffic/exchanges/' + id);
    if (!r.ok) { slot.textContent = 'not found'; return; }
    const e = await r.json();
    const reqBody = e.request.body_json != null
      ? JSON.stringify(e.request.body_json, null, 2)
      : (e.request.body_text || '');
    const respBody = e.response.body_json != null
      ? JSON.stringify(e.response.body_json, null, 2)
      : (e.response.body_text || '');
    const parts = [];
    if (e.response.assistant_text) {
      parts.push(`<h3>Assistant reply (reconstructed)</h3><pre>${escapeHtml(e.response.assistant_text)}</pre>`);
    }
    parts.push(`<h3>Request headers</h3><pre>${escapeHtml(JSON.stringify(e.request.headers, null, 2))}</pre>`);
    parts.push(`<h3>Request body${e.request.truncated ? ' (truncated)' : ''}</h3><pre>${escapeHtml(reqBody)}</pre>`);
    parts.push(`<h3>Response headers</h3><pre>${escapeHtml(JSON.stringify(e.response.headers, null, 2))}</pre>`);
    parts.push(`<h3>Response body${e.response.truncated ? ' (truncated)' : ''}</h3><pre>${escapeHtml(respBody)}</pre>`);
    if (e.error) parts.push(`<h3>Error</h3><pre>${escapeHtml(e.error)}</pre>`);
    slot.innerHTML = parts.join('');
    det.dataset.loaded = '1';
  } catch (err) {
    slot.textContent = 'error: ' + err;
  }
}

$filter.addEventListener('input', tick);
tick();
setInterval(tick, 2000);
</script>
</body></html>
"""


@app.get("/traffic", include_in_schema=False)
async def traffic_ui() -> Response:
    return HTMLResponse(TRAFFIC_HTML)


@app.get("/traffic/exchanges", include_in_schema=False)
async def traffic_list() -> JSONResponse:
    summaries = [
        {
            "id": e["id"],
            "ts": e["ts"],
            "method": e["method"],
            "path": e["path"],
            "model": e["model"],
            "streaming": e["streaming"],
            "status": e["status"],
            "duration_ms": e["duration_ms"],
            "usage": e["usage"],
            "session_id": e.get("session_id"),
        }
        for e in _exchanges
    ]
    return JSONResponse({"count": len(summaries), "buffer_size": TRAFFIC_BUFFER_SIZE, "exchanges": summaries})


@app.get("/traffic/exchanges/{ex_id}", include_in_schema=False)
async def traffic_detail(ex_id: str) -> JSONResponse:
    for e in _exchanges:
        if e["id"] == ex_id:
            return JSONResponse(e)
    return JSONResponse({"error": "not found"}, status_code=404)


@app.post("/v1/messages")
async def messages(request: Request) -> Response:
    raw = await request.body()
    try:
        body: dict[str, Any] = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        log.warning("non-json body on /v1/messages, passing through unchanged")
        return await _passthrough(request, raw)

    sys = body.get("system")
    tools = body.get("tools") or []
    sys_shape = "array" if isinstance(sys, list) else ("string" if isinstance(sys, str) else "none")
    sys_blocks = len(sys) if isinstance(sys, list) else 0
    sys_size = _system_size(sys)
    tool_count = len(tools) if isinstance(tools, list) else 0
    had_cc = _had_existing_cache_control(body)
    streaming = bool(body.get("stream"))
    model = body.get("model")

    tool_names = _extract_tool_names(tools)
    if tool_names:
        snapshot = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "model": model,
            "tool_count": tool_count,
            "tools": tool_names,
            "by_server": _group_tools_by_server(tool_names),
        }
        global _last_snapshot
        _last_snapshot = snapshot
        _snapshots_by_shape[(str(model or ""), tool_count)] = snapshot

    sys_inj = "disabled"
    tools_inj = "disabled"
    out_body = body
    if INJECT_CACHE_CONTROL:
        out_body = copy.deepcopy(body)
        if had_cc:
            sys_n, tools_n = _upgrade_existing(out_body)
            sys_inj = f"upgraded={sys_n} ttl={CACHE_TTL}"
            tools_inj = f"upgraded={tools_n} ttl={CACHE_TTL}"
        else:
            sys_inj, tools_inj = _seed_breakpoints(out_body)

    out_bytes = json.dumps(out_body).encode("utf-8") if INJECT_CACHE_CONTROL else raw
    headers = _force_identity_encoding(_merge_beta(_filter_headers(dict(request.headers))))
    headers["content-type"] = "application/json"

    url = f"{TARGET_URL}/v1/messages"
    base_log = (
        "POST /v1/messages model=%s stream=%s system=%s blocks=%d sys~toks=%d "
        "tools=%d had_cc=%s inject=%s sys_inj=%s tools_inj=%s"
    ) % (
        model, streaming, sys_shape, sys_blocks, sys_size, tool_count,
        had_cc, INJECT_CACHE_CONTROL, sys_inj, tools_inj,
    )

    recorder = Recorder(
        method=request.method, path=request.url.path,
        req_headers=dict(request.headers), req_body=out_bytes,
        model=model, streaming=streaming,
    )
    assert _client is not None
    if streaming:
        return await _stream_forward(url, headers, out_bytes, base_log, recorder)
    return await _buffered_forward(url, headers, out_bytes, base_log, recorder)


async def _buffered_forward(url: str, headers: dict[str, str], body: bytes, base_log: str,
                            recorder: Recorder | None = None) -> Response:
    assert _client is not None
    try:
        resp = await _client.post(url, headers=headers, content=body)
    except httpx.HTTPError as e:
        log.error("%s upstream-error=%s", base_log, e)
        if recorder:
            recorder.finish(status=502, resp_headers=None, error=str(e))
        return JSONResponse({"error": {"type": "proxy_error", "message": str(e)}}, status_code=502)

    usage = None
    try:
        usage = _extract_usage(resp.json())
    except (json.JSONDecodeError, ValueError):
        pass
    log.info("%s status=%d usage=%s", base_log, resp.status_code, usage)

    resp_headers = _strip_response_encoding(_filter_headers(dict(resp.headers)))
    if recorder:
        recorder.finish(status=resp.status_code, resp_headers=resp_headers,
                        resp_body=resp.content, usage=usage)
    return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers,
                    media_type=resp.headers.get("content-type"))


async def _stream_forward(url: str, headers: dict[str, str], body: bytes, base_log: str,
                          recorder: Recorder | None = None) -> Response:
    assert _client is not None
    # Open the upstream stream, peek the status. If it's non-2xx, drain and return
    # a regular Response so the CLI sees the correct HTTP code. Only stream when 2xx.
    captured_usage: dict[str, Any] = {}

    try:
        cm = _client.stream("POST", url, headers=headers, content=body)
        upstream = await cm.__aenter__()
    except httpx.HTTPError as e:
        log.error("%s upstream-connect-error=%s", base_log, e)
        if recorder:
            recorder.finish(status=502, resp_headers=None, error=str(e))
        return JSONResponse({"error": {"type": "proxy_error", "message": str(e)}}, status_code=502)

    status = upstream.status_code
    if status >= 400:
        try:
            err_body = await upstream.aread()
        finally:
            await cm.__aexit__(None, None, None)
        log.error("%s status=%d upstream-body=%s", base_log, status, err_body[:500])
        resp_headers = _strip_response_encoding(_filter_headers(dict(upstream.headers)))
        if recorder:
            recorder.finish(status=status, resp_headers=resp_headers, resp_body=err_body)
        return Response(
            content=err_body,
            status_code=status,
            media_type=upstream.headers.get("content-type"),
            headers=resp_headers,
        )

    captured_chunks: list[bytes] = []
    captured_bytes = 0

    async def iterator():
        nonlocal captured_bytes
        # Buffer partial SSE lines across chunks. The `message_delta` event carrying
        # final usage is short and often arrives split at byte boundaries.
        sse_buf = ""
        stream_error: str | None = None
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
                if recorder and captured_bytes < TRAFFIC_MAX_BODY_BYTES:
                    captured_chunks.append(chunk)
                    captured_bytes += len(chunk)
                try:
                    sse_buf += chunk.decode("utf-8", errors="ignore")
                    while "\n" in sse_buf:
                        line, sse_buf = sse_buf.split("\n", 1)
                        line = line.rstrip("\r")
                        if not line.startswith("data: "):
                            continue
                        payload = line[6:].strip()
                        if not payload or payload == "[DONE]":
                            continue
                        try:
                            evt = json.loads(payload)
                        except (json.JSONDecodeError, ValueError):
                            continue
                        u = _extract_usage(evt) or _extract_usage(evt.get("message", {}))
                        if u:
                            for k, v in u.items():
                                if v is not None:
                                    captured_usage[k] = v
                except Exception:  # noqa: BLE001 - scanning is best-effort
                    pass
        except httpx.HTTPError as e:
            stream_error = str(e)
            log.error("%s stream-error=%s", base_log, e)
        finally:
            await cm.__aexit__(None, None, None)
            log.info("%s stream-complete usage=%s", base_log, captured_usage or None)
            if recorder:
                sse_text = b"".join(captured_chunks).decode("utf-8", errors="replace")
                recorder.finish(
                    status=status,
                    resp_headers=_strip_response_encoding(_filter_headers(dict(upstream.headers))),
                    sse_text=sse_text,
                    usage=captured_usage or None,
                    error=stream_error,
                )

    return StreamingResponse(
        iterator(),
        status_code=status,
        media_type=upstream.headers.get("content-type", "text/event-stream"),
        headers=_strip_response_encoding(_filter_headers(dict(upstream.headers))),
    )


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def catchall(path: str, request: Request) -> Response:
    raw = await request.body()
    return await _passthrough(request, raw)


async def _passthrough(request: Request, raw: bytes) -> Response:
    assert _client is not None
    url = f"{TARGET_URL}{request.url.path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"
    headers = _force_identity_encoding(_filter_headers(dict(request.headers)))
    recorder = Recorder(
        method=request.method, path=request.url.path,
        req_headers=dict(request.headers), req_body=raw,
        model=None, streaming=False,
    )
    try:
        resp = await _client.request(request.method, url, headers=headers, content=raw)
    except httpx.HTTPError as e:
        log.error("passthrough %s %s error=%s", request.method, request.url.path, e)
        recorder.finish(status=502, resp_headers=None, error=str(e))
        return JSONResponse({"error": {"type": "proxy_error", "message": str(e)}}, status_code=502)
    log.info("passthrough %s %s status=%d", request.method, request.url.path, resp.status_code)
    resp_headers = _strip_response_encoding(_filter_headers(dict(resp.headers)))
    recorder.finish(status=resp.status_code, resp_headers=resp_headers, resp_body=resp.content)
    return Response(content=resp.content, status_code=resp.status_code,
                    headers=resp_headers,
                    media_type=resp.headers.get("content-type"))
