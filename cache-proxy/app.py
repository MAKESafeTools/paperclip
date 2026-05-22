"""
Transparent proxy between Claude Code CLI and api.anthropic.com that adds 1h
cache_control breakpoints on system + tools and the extended-cache-ttl beta header.

Pointed at by the CLI via ANTHROPIC_BASE_URL. Auth headers from the incoming request
(Authorization / x-api-key / anthropic-auth-token) pass through verbatim so OAuth /
Max-subscription billing is preserved.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

TARGET_URL = os.environ.get("TARGET_URL", "https://api.anthropic.com").rstrip("/")
CACHE_TTL = os.environ.get("CACHE_TTL", "1h")
INJECT_CACHE_CONTROL = os.environ.get("INJECT_CACHE_CONTROL", "false").lower() == "true"
INJECT_SYSTEM = os.environ.get("INJECT_SYSTEM", "true").lower() == "true"
INJECT_TOOLS = os.environ.get("INJECT_TOOLS", "true").lower() == "true"
MIN_SYSTEM_TOKENS = int(os.environ.get("MIN_SYSTEM_TOKENS", "1024"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BETA_FLAG = "extended-cache-ttl-2025-04-11"
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

    assert _client is not None
    if streaming:
        return await _stream_forward(url, headers, out_bytes, base_log)
    return await _buffered_forward(url, headers, out_bytes, base_log)


async def _buffered_forward(url: str, headers: dict[str, str], body: bytes, base_log: str) -> Response:
    assert _client is not None
    try:
        resp = await _client.post(url, headers=headers, content=body)
    except httpx.HTTPError as e:
        log.error("%s upstream-error=%s", base_log, e)
        return JSONResponse({"error": {"type": "proxy_error", "message": str(e)}}, status_code=502)

    usage = None
    try:
        usage = _extract_usage(resp.json())
    except (json.JSONDecodeError, ValueError):
        pass
    log.info("%s status=%d usage=%s", base_log, resp.status_code, usage)

    resp_headers = _strip_response_encoding(_filter_headers(dict(resp.headers)))
    return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers,
                    media_type=resp.headers.get("content-type"))


async def _stream_forward(url: str, headers: dict[str, str], body: bytes, base_log: str) -> Response:
    assert _client is not None
    # Open the upstream stream, peek the status. If it's non-2xx, drain and return
    # a regular Response so the CLI sees the correct HTTP code. Only stream when 2xx.
    captured_usage: dict[str, Any] = {}

    try:
        cm = _client.stream("POST", url, headers=headers, content=body)
        upstream = await cm.__aenter__()
    except httpx.HTTPError as e:
        log.error("%s upstream-connect-error=%s", base_log, e)
        return JSONResponse({"error": {"type": "proxy_error", "message": str(e)}}, status_code=502)

    status = upstream.status_code
    if status >= 400:
        try:
            err_body = await upstream.aread()
        finally:
            await cm.__aexit__(None, None, None)
        log.error("%s status=%d upstream-body=%s", base_log, status, err_body[:500])
        return Response(
            content=err_body,
            status_code=status,
            media_type=upstream.headers.get("content-type"),
            headers=_strip_response_encoding(_filter_headers(dict(upstream.headers))),
        )

    async def iterator():
        # Buffer partial SSE lines across chunks. The `message_delta` event carrying
        # final usage is short and often arrives split at byte boundaries.
        sse_buf = ""
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
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
            log.error("%s stream-error=%s", base_log, e)
        finally:
            await cm.__aexit__(None, None, None)
            log.info("%s stream-complete usage=%s", base_log, captured_usage or None)

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
    try:
        resp = await _client.request(request.method, url, headers=headers, content=raw)
    except httpx.HTTPError as e:
        log.error("passthrough %s %s error=%s", request.method, request.url.path, e)
        return JSONResponse({"error": {"type": "proxy_error", "message": str(e)}}, status_code=502)
    log.info("passthrough %s %s status=%d", request.method, request.url.path, resp.status_code)
    return Response(content=resp.content, status_code=resp.status_code,
                    headers=_strip_response_encoding(_filter_headers(dict(resp.headers))),
                    media_type=resp.headers.get("content-type"))
