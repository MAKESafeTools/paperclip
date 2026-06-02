"""
viz-sidecar: persists cache-proxy traffic to SQLite and streams it to a UI via SSE.

Stage 1 MVP — read-only visualization for paperclip's cache-proxy.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import paperclip_client

CACHE_PROXY_URL = os.environ.get("CACHE_PROXY_URL", "http://cache-proxy:8000").rstrip("/")
DB_PATH = os.environ.get("DB_PATH", "/data/viz.sqlite")
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "1.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
STATIC_DIR = Path(__file__).parent / "static"

# Retention: prune exchanges older than RETENTION_DAYS so the DB stays bounded.
# 0/negative disables pruning. paperclip_runs/agents are tiny and not pruned.
RETENTION_DAYS = float(os.environ.get("RETENTION_DAYS", "7"))
RETENTION_INTERVAL_SECONDS = float(os.environ.get("RETENTION_INTERVAL_SECONDS", "3600"))

# Anthropic public pricing (USD per million tokens) as of late 2025/early 2026.
# Update as needed; entries are best-effort, used only for a banner estimate.
MODEL_PRICING: dict[str, dict[str, float]] = {
    # model substring (case-insensitive) -> {input, output, cache_read, cache_write_1h}
    "opus-4": {"input": 15.0, "output": 75.0, "cache_read": 1.5, "cache_write": 18.75},
    "sonnet-4": {"input": 3.0, "output": 15.0, "cache_read": 0.3, "cache_write": 3.75},
    "haiku-4": {"input": 1.0, "output": 5.0, "cache_read": 0.1, "cache_write": 1.25},
    "haiku-3": {"input": 0.25, "output": 1.25, "cache_read": 0.03, "cache_write": 0.30},
}


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("viz-sidecar")

app = FastAPI()

# ---------- DB ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS exchanges (
    id                  TEXT PRIMARY KEY,
    ts                  TEXT NOT NULL,
    fetched_at          TEXT NOT NULL,
    method              TEXT,
    path                TEXT,
    model               TEXT,
    streaming           INTEGER,
    status              INTEGER,
    duration_ms         INTEGER,
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    cache_read_tokens   INTEGER,
    cache_creation_tokens INTEGER,
    error               TEXT,
    session_id          TEXT,
    detail_json         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_exchanges_ts ON exchanges(ts);
CREATE INDEX IF NOT EXISTS idx_exchanges_fetched ON exchanges(fetched_at);
-- idx_exchanges_session_ts is created in _init_db() *after* the column migration
-- so it doesn't fail on legacy DBs missing the session_id column.

CREATE TABLE IF NOT EXISTS paperclip_runs (
    id                  TEXT PRIMARY KEY,
    agent_id            TEXT,
    status              TEXT,
    started_at          TEXT,
    finished_at         TEXT,
    session_id_before   TEXT,
    session_id_after    TEXT,
    invocation_source   TEXT,
    exit_code           INTEGER,
    error_code          TEXT,
    fetched_at          TEXT NOT NULL,
    raw_json            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pcr_session_after  ON paperclip_runs(session_id_after);
CREATE INDEX IF NOT EXISTS idx_pcr_session_before ON paperclip_runs(session_id_before);
CREATE INDEX IF NOT EXISTS idx_pcr_started_at     ON paperclip_runs(started_at);

CREATE TABLE IF NOT EXISTS paperclip_agents (
    id                  TEXT PRIMARY KEY,
    name                TEXT,
    title               TEXT,
    role                TEXT,
    icon                TEXT,
    status              TEXT,
    url_key             TEXT,
    reports_to          TEXT,
    adapter_type        TEXT,
    fetched_at          TEXT NOT NULL,
    raw_json            TEXT NOT NULL
);
"""


def _maybe_add_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        log.info("added column %s.%s", table, column)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # Enable incremental auto-vacuum so pruned space can be reclaimed without a
    # full (locking) VACUUM. Takes effect for a fresh DB immediately; an existing
    # DB needs a one-time VACUUM to switch modes.
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _init_db() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        conn.executescript(SCHEMA)
        _maybe_add_column(conn, "exchanges", "session_id", "TEXT")
        _maybe_add_column(conn, "exchanges", "req_block_types", "TEXT")
        _maybe_add_column(conn, "exchanges", "resp_block_types", "TEXT")
        _maybe_add_column(conn, "exchanges", "req_summary", "TEXT")
        _maybe_add_column(conn, "exchanges", "resp_summary", "TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exchanges_session_ts ON exchanges(session_id, ts)"
        )


def _backfill_derived_columns() -> tuple[int, int, int, int]:
    """Re-derive columns from detail_json for rows missing them.
    Cheap idempotent backfill — runs at startup, scans rows missing any derived
    column. No-ops once converged."""
    tok_fixed = 0
    sid_fixed = 0
    block_fixed = 0
    sum_fixed = 0
    with _connect() as conn:
        rows = conn.execute(
            """SELECT id, detail_json
                 FROM exchanges
                WHERE (input_tokens IS NULL AND output_tokens IS NULL)
                   OR session_id IS NULL
                   OR (req_block_types IS NULL AND resp_block_types IS NULL)
                   OR (req_summary IS NULL AND resp_summary IS NULL)"""
        ).fetchall()
        for row in rows:
            try:
                detail = json.loads(row["detail_json"])
            except Exception:
                continue
            usage = detail.get("usage") or {}
            if isinstance(usage, dict) and usage:
                conn.execute(
                    """UPDATE exchanges SET
                           input_tokens = ?, output_tokens = ?,
                           cache_read_tokens = ?, cache_creation_tokens = ?
                       WHERE id = ?""",
                    (
                        _usage_field(usage, "in"),
                        _usage_field(usage, "out"),
                        _usage_field(usage, "cache_read"),
                        _usage_field(usage, "cache_create"),
                        row["id"],
                    ),
                )
                tok_fixed += 1
            sid = _session_id_from_record(detail)
            if sid:
                conn.execute(
                    "UPDATE exchanges SET session_id = ? WHERE id = ?",
                    (sid, row["id"]),
                )
                sid_fixed += 1
            blocks = _exchange_block_summary(detail)
            if blocks["req"] or blocks["resp"]:
                conn.execute(
                    "UPDATE exchanges SET req_block_types = ?, resp_block_types = ? WHERE id = ?",
                    (",".join(blocks["req"]) or None, ",".join(blocks["resp"]) or None, row["id"]),
                )
                block_fixed += 1
            sums = _exchange_summaries(detail)
            if sums["req_summary"] or sums["resp_summary"]:
                conn.execute(
                    "UPDATE exchanges SET req_summary = ?, resp_summary = ? WHERE id = ?",
                    (sums["req_summary"], sums["resp_summary"], row["id"]),
                )
                sum_fixed += 1
    return tok_fixed, sid_fixed, block_fixed, sum_fixed


# ---------- SSE subscribers ----------

_subscribers: set[asyncio.Queue[str]] = set()


async def _broadcast(event: str, payload: dict[str, Any]) -> None:
    msg = f"event: {event}\ndata: {json.dumps(payload)}\n\n"
    dead: list[asyncio.Queue[str]] = []
    for q in _subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _subscribers.discard(q)


# ---------- poller ----------

_poll_task: asyncio.Task[None] | None = None
_paperclip_poll_task: asyncio.Task[None] | None = None
_retention_task: asyncio.Task[None] | None = None
_seen_ids: set[str] = set()

PAPERCLIP_POLL_INTERVAL_SECONDS = float(os.environ.get("PAPERCLIP_POLL_INTERVAL_SECONDS", "15.0"))


def _usage_field(usage: dict[str, Any] | None, key: str) -> int | None:
    if not isinstance(usage, dict):
        return None
    v = usage.get(key)
    return int(v) if isinstance(v, (int, float)) else None


def _session_id_from_record(record: dict[str, Any]) -> str | None:
    """Prefer the top-level session_id (new cache-proxy). Fall back to digging
    the header / metadata.user_id out of detail — covers older records."""
    sid = record.get("session_id")
    if isinstance(sid, str) and sid:
        return sid
    req = record.get("request") or {}
    headers = req.get("headers") or {}
    if isinstance(headers, dict):
        for k, v in headers.items():
            if k.lower() == "x-claude-code-session-id" and isinstance(v, str) and v:
                return v
    body = req.get("body_json")
    if isinstance(body, dict):
        meta = body.get("metadata")
        if isinstance(meta, dict):
            uid = meta.get("user_id")
            if isinstance(uid, str):
                try:
                    parsed = json.loads(uid)
                except (json.JSONDecodeError, ValueError):
                    parsed = None
                if isinstance(parsed, dict):
                    s = parsed.get("session_id")
                    if isinstance(s, str) and s:
                        return s
    return None


def _summarize_user_message(content: Any, max_len: int = 140) -> str | None:
    """Short readable summary of a user-role message's content blocks.
    Used for the per-row 'sent' column — captures the new content the turn
    contributed to the conversation (typically a tool_result for mid-session
    turns, the initial prompt for the first turn)."""
    if isinstance(content, str):
        t = content.strip().replace("\n", " ")
        return (t[: max_len - 1] + "…") if len(t) > max_len else (t or None)
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        bt = block.get("type")
        if bt == "text":
            t = block.get("text")
            if isinstance(t, str) and t.strip():
                parts.append(t.strip())
        elif bt == "tool_result":
            sub = block.get("content")
            err = " err" if block.get("is_error") else ""
            if isinstance(sub, str):
                parts.append(f"[tool_result{err}] {sub.strip()}")
            elif isinstance(sub, list):
                for sb in sub:
                    if isinstance(sb, dict) and sb.get("type") == "text":
                        parts.append(f"[tool_result{err}] {(sb.get('text') or '').strip()}")
                        break
            else:
                parts.append(f"[tool_result{err}]")
        elif bt == "image":
            parts.append("[image]")
        elif bt == "document":
            parts.append("[document]")
        elif isinstance(bt, str):
            parts.append(f"[{bt}]")
    text = " ".join(parts).strip().replace("\n", " ")
    if not text:
        return None
    return (text[: max_len - 1] + "…") if len(text) > max_len else text


def _summarize_response(detail: dict[str, Any], max_len: int = 140) -> str | None:
    """Short summary of the assistant's response: text first, then tool_use
    names. Handles both buffered (body_json.content) and streaming (cache-proxy's
    reconstructed assistant_text plus SSE event scan for tool_use)."""
    resp = detail.get("response") or {}
    parts: list[str] = []
    body = resp.get("body_json")
    if isinstance(body, dict):
        content = body.get("content")
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                bt = block.get("type")
                if bt == "text":
                    t = block.get("text")
                    if isinstance(t, str) and t.strip():
                        parts.append(t.strip())
                elif bt == "tool_use":
                    name = block.get("name") or "?"
                    parts.append(f"[tool_use: {name}]")
                elif bt == "thinking":
                    parts.append("[thinking]")
                elif isinstance(bt, str):
                    parts.append(f"[{bt}]")
    if not parts:
        at = resp.get("assistant_text")
        if isinstance(at, str) and at.strip():
            parts.append(at.strip())
        body_text = resp.get("body_text") or ""
        if "tool_use" in body_text:
            for line in body_text.split("\n"):
                if not line.startswith("data: "):
                    continue
                payload = line[6:].strip()
                if not payload or payload == "[DONE]":
                    continue
                try:
                    evt = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    continue
                if evt.get("type") == "content_block_start":
                    cb = evt.get("content_block") or {}
                    if cb.get("type") == "tool_use":
                        parts.append(f"[tool_use: {cb.get('name','?')}]")
    text = " ".join(parts).strip().replace("\n", " ")
    if not text:
        return None
    return (text[: max_len - 1] + "…") if len(text) > max_len else text


def _exchange_summaries(detail: dict[str, Any]) -> dict[str, str | None]:
    """Pulls 'last user message' + 'response' snippets for the row display."""
    req = detail.get("request") or {}
    body = req.get("body_json") or {}
    req_summary: str | None = None
    if isinstance(body, dict):
        messages = body.get("messages") or []
        if isinstance(messages, list):
            last_user = next(
                (m for m in reversed(messages) if isinstance(m, dict) and m.get("role") == "user"),
                None,
            )
            if last_user is not None:
                req_summary = _summarize_user_message(last_user.get("content"))
    return {"req_summary": req_summary, "resp_summary": _summarize_response(detail)}


def _content_block_types(content: Any) -> list[str]:
    """Return the ordered list of block types in a message-content array.
    Strings collapse to 'text'; absent content returns []."""
    if isinstance(content, str):
        return ["text"]
    if not isinstance(content, list):
        return []
    out: list[str] = []
    for block in content:
        if isinstance(block, dict):
            t = block.get("type")
            if isinstance(t, str):
                out.append(t)
    return out


def _exchange_block_summary(detail: dict[str, Any]) -> dict[str, Any]:
    """Pull a summary of block types for the last user message and the response.
    Stored at persist time so we don't re-parse on every list query."""
    req = detail.get("request") or {}
    body = req.get("body_json") or {}
    req_types: list[str] = []
    if isinstance(body, dict):
        messages = body.get("messages") or []
        if isinstance(messages, list) and messages:
            last_user = next((m for m in reversed(messages) if isinstance(m, dict) and m.get("role") == "user"), None)
            if last_user:
                req_types = _content_block_types(last_user.get("content"))

    resp = detail.get("response") or {}
    resp_types: list[str] = []
    resp_body_json = resp.get("body_json")
    if isinstance(resp_body_json, dict):
        resp_types = _content_block_types(resp_body_json.get("content"))
    else:
        # streaming: reconstruct from assistant_text fallback isn't great. Try parsing
        # SSE text for content_block_start events with their declared types.
        text = resp.get("body_text") or ""
        if text:
            for line in text.split("\n"):
                if not line.startswith("data: "):
                    continue
                payload = line[6:].strip()
                if not payload or payload == "[DONE]":
                    continue
                try:
                    evt = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    continue
                if evt.get("type") == "content_block_start":
                    block = evt.get("content_block") or {}
                    t = block.get("type")
                    if isinstance(t, str):
                        resp_types.append(t)
    return {"req": req_types, "resp": resp_types}


def _first_user_prompt_preview(detail: dict[str, Any], max_len: int = 160) -> str | None:
    """Pull a short preview from the first user-role message in a request body."""
    req = detail.get("request") or {}
    body = req.get("body_json")
    if not isinstance(body, dict):
        return None
    messages = body.get("messages") or []
    if not isinstance(messages, list):
        return None
    for m in messages:
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        content = m.get("content")
        text: str | None = None
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    t = block.get("text")
                    if isinstance(t, str) and t.strip():
                        text = t
                        break
        if text:
            text = text.strip().replace("\n", " ")
            return text[: max_len - 1] + "…" if len(text) > max_len else text
    return None


def _persist(record: dict[str, Any]) -> dict[str, Any]:
    usage = record.get("usage") or {}
    blocks = _exchange_block_summary(record)
    summaries = _exchange_summaries(record)
    row = {
        "id": record["id"],
        "ts": record["ts"],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "method": record.get("method"),
        "path": record.get("path"),
        "model": record.get("model"),
        "streaming": 1 if record.get("streaming") else 0,
        "status": record.get("status"),
        "duration_ms": record.get("duration_ms"),
        # cache-proxy normalizes usage to short keys: in/out/cache_read/cache_create
        # (see cache-proxy/app.py:_extract_usage). See STAGE0 + README for shape.
        "input_tokens": _usage_field(usage, "in"),
        "output_tokens": _usage_field(usage, "out"),
        "cache_read_tokens": _usage_field(usage, "cache_read"),
        "cache_creation_tokens": _usage_field(usage, "cache_create"),
        "error": record.get("error"),
        "session_id": _session_id_from_record(record),
        "req_block_types": ",".join(blocks["req"]) or None,
        "resp_block_types": ",".join(blocks["resp"]) or None,
        "req_summary": summaries["req_summary"],
        "resp_summary": summaries["resp_summary"],
        "detail_json": json.dumps(record),
    }
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO exchanges
            (id, ts, fetched_at, method, path, model, streaming, status, duration_ms,
             input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, error,
             session_id, req_block_types, resp_block_types, req_summary, resp_summary, detail_json)
            VALUES (:id, :ts, :fetched_at, :method, :path, :model, :streaming, :status, :duration_ms,
                    :input_tokens, :output_tokens, :cache_read_tokens, :cache_creation_tokens, :error,
                    :session_id, :req_block_types, :resp_block_types, :req_summary, :resp_summary, :detail_json)
            """,
            row,
        )
    return row


def _persist_paperclip_run(run: dict[str, Any]) -> None:
    row = {
        "id": run.get("id"),
        "agent_id": run.get("agentId"),
        "status": run.get("status"),
        "started_at": run.get("startedAt"),
        "finished_at": run.get("finishedAt"),
        "session_id_before": run.get("sessionIdBefore"),
        "session_id_after": run.get("sessionIdAfter"),
        "invocation_source": run.get("invocationSource"),
        "exit_code": run.get("exitCode"),
        "error_code": run.get("errorCode"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "raw_json": json.dumps(run),
    }
    if not row["id"]:
        return
    with _connect() as conn:
        # Skip no-op writes: the poll loop re-fetches the same runs every cycle,
        # so only write when raw_json actually changed. Avoids constant WAL churn.
        existing = conn.execute(
            "SELECT raw_json FROM paperclip_runs WHERE id = ?", (row["id"],)
        ).fetchone()
        if existing is not None and existing["raw_json"] == row["raw_json"]:
            return
        conn.execute(
            """
            INSERT OR REPLACE INTO paperclip_runs
                (id, agent_id, status, started_at, finished_at, session_id_before,
                 session_id_after, invocation_source, exit_code, error_code, fetched_at, raw_json)
            VALUES (:id, :agent_id, :status, :started_at, :finished_at, :session_id_before,
                    :session_id_after, :invocation_source, :exit_code, :error_code, :fetched_at, :raw_json)
            """,
            row,
        )


def _persist_paperclip_agent(agent: dict[str, Any]) -> None:
    if not agent.get("id"):
        return
    row = {
        "id": agent.get("id"),
        "name": agent.get("name"),
        "title": agent.get("title"),
        "role": agent.get("role"),
        "icon": agent.get("icon"),
        "status": agent.get("status"),
        "url_key": agent.get("urlKey"),
        "reports_to": agent.get("reportsTo"),
        "adapter_type": agent.get("adapterType"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "raw_json": json.dumps(agent),
    }
    with _connect() as conn:
        # Skip no-op writes (see _persist_paperclip_run).
        existing = conn.execute(
            "SELECT raw_json FROM paperclip_agents WHERE id = ?", (row["id"],)
        ).fetchone()
        if existing is not None and existing["raw_json"] == row["raw_json"]:
            return
        conn.execute(
            """
            INSERT OR REPLACE INTO paperclip_agents
                (id, name, title, role, icon, status, url_key, reports_to,
                 adapter_type, fetched_at, raw_json)
            VALUES (:id, :name, :title, :role, :icon, :status, :url_key, :reports_to,
                    :adapter_type, :fetched_at, :raw_json)
            """,
            row,
        )


async def _paperclip_poll_loop() -> None:
    """Background fetch of paperclip metadata. Silently no-ops if not configured."""
    if not paperclip_client.is_configured():
        log.info("paperclip enrichment disabled (set PAPERCLIP_COMPANY_ID + PAPERCLIP_AGENT_ID)")
        return
    log.info("paperclip poller starting interval=%.1fs", PAPERCLIP_POLL_INTERVAL_SECONDS)
    client = paperclip_client.PaperclipClient()
    try:
        while True:
            try:
                agents = await client.list_agents()
                if agents:
                    for a in agents:
                        await asyncio.to_thread(_persist_paperclip_agent, a)
                runs = await client.list_heartbeat_runs(limit=500)
                if runs:
                    for r in runs:
                        await asyncio.to_thread(_persist_paperclip_run, r)
                # live-runs include ones currently in flight; sometimes they overlap
                # with heartbeat-runs but cheap to also persist for freshness.
                live = await client.list_live_runs(limit=50)
                if live:
                    for r in live:
                        await asyncio.to_thread(_persist_paperclip_run, r)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("paperclip poll error: %s", e)
            await asyncio.sleep(PAPERCLIP_POLL_INTERVAL_SECONDS)
    finally:
        await client.aclose()


def _prune_old_exchanges() -> int:
    """Delete exchanges older than RETENTION_DAYS. Returns rows deleted.
    Runs in a thread (see _retention_loop) so the big DELETE never blocks the
    event loop. Also drops the pruned ids from _seen_ids so memory stays bounded."""
    if RETENTION_DAYS <= 0:
        return 0
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    with _connect() as conn:
        doomed = [r["id"] for r in conn.execute(
            "SELECT id FROM exchanges WHERE ts < ?", (cutoff,)
        )]
        if not doomed:
            return 0
        conn.execute("DELETE FROM exchanges WHERE ts < ?", (cutoff,))
        # Reclaim freelist space incrementally; full VACUUM would lock the DB.
        conn.execute("PRAGMA incremental_vacuum")
    for ex_id in doomed:
        _seen_ids.discard(ex_id)
    return len(doomed)


async def _retention_loop() -> None:
    """Periodically prune old exchanges so the DB stays bounded."""
    if RETENTION_DAYS <= 0:
        log.info("retention disabled (RETENTION_DAYS<=0)")
        return
    log.info(
        "retention loop starting keep=%.1fd interval=%.0fs",
        RETENTION_DAYS, RETENTION_INTERVAL_SECONDS,
    )
    while True:
        try:
            deleted = await asyncio.to_thread(_prune_old_exchanges)
            if deleted:
                log.info("retention pruned %d exchanges older than %.1fd", deleted, RETENTION_DAYS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("retention error: %s", e)
        await asyncio.sleep(RETENTION_INTERVAL_SECONDS)


async def _poll_loop() -> None:
    """Poll cache-proxy /traffic/exchanges, persist new records, broadcast via SSE."""
    log.info("poll loop starting target=%s interval=%.2fs", CACHE_PROXY_URL, POLL_INTERVAL_SECONDS)
    # Hydrate _seen_ids from disk so a restart doesn't replay everything currently
    # still in the cache-proxy ring buffer. Full-table scan → run off the loop.
    def _hydrate() -> set[str]:
        with _connect() as conn:
            return {row["id"] for row in conn.execute("SELECT id FROM exchanges")}
    _seen_ids.update(await asyncio.to_thread(_hydrate))
    log.info("hydrated seen_ids count=%d from sqlite", len(_seen_ids))

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=3.0)) as client:
        while True:
            try:
                r = await client.get(f"{CACHE_PROXY_URL}/traffic/exchanges")
                r.raise_for_status()
                data = r.json()
                summaries = data.get("exchanges") or []
                # Cache-proxy returns newest-first; we want to insert oldest-first
                # so SSE events arrive in real-world order.
                new_summaries = [s for s in summaries if s["id"] not in _seen_ids]
                for s in reversed(new_summaries):
                    ex_id = s["id"]
                    try:
                        detail_r = await client.get(f"{CACHE_PROXY_URL}/traffic/exchanges/{ex_id}")
                        detail_r.raise_for_status()
                        record = detail_r.json()
                    except Exception as e:
                        log.warning("failed to fetch detail for %s: %s", ex_id, e)
                        continue
                    row = await asyncio.to_thread(_persist, record)
                    _seen_ids.add(ex_id)
                    await _broadcast("exchange", _summary_from_row(row))
            except httpx.HTTPError as e:
                log.warning("poll error: %s", e)
            except asyncio.CancelledError:
                log.info("poll loop cancelled")
                raise
            except Exception as e:
                log.exception("unexpected poll error: %s", e)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


def _summary_from_row(row: dict[str, Any] | sqlite3.Row) -> dict[str, Any]:
    d = dict(row) if isinstance(row, sqlite3.Row) else row
    req_blocks = d.get("req_block_types")
    resp_blocks = d.get("resp_block_types")
    return {
        "id": d["id"],
        "ts": d["ts"],
        "method": d.get("method"),
        "path": d.get("path"),
        "model": d.get("model"),
        "streaming": bool(d.get("streaming")),
        "status": d.get("status"),
        "duration_ms": d.get("duration_ms"),
        "input_tokens": d.get("input_tokens"),
        "output_tokens": d.get("output_tokens"),
        "cache_read_tokens": d.get("cache_read_tokens"),
        "cache_creation_tokens": d.get("cache_creation_tokens"),
        "error": d.get("error"),
        "session_id": d.get("session_id"),
        "req_block_types": req_blocks.split(",") if req_blocks else [],
        "resp_block_types": resp_blocks.split(",") if resp_blocks else [],
        "req_summary": d.get("req_summary"),
        "resp_summary": d.get("resp_summary"),
        # context_delta is only present when the row came through list_exchanges
        # (which adds it via a window function); for SSE-prepended rows it's None
        # and the client refetches the page to fill it in.
        "context_delta": d.get("context_delta"),
    }


# ---------- time-range filter ----------

_RANGE_SECONDS = {
    "10m": 10 * 60,
    "1h": 60 * 60,
    "6h": 6 * 60 * 60,
    "24h": 24 * 60 * 60,
    "3d": 3 * 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
}


def _since_iso(range_param: str | None) -> str | None:
    if not range_param or range_param == "all":
        return None
    secs = _RANGE_SECONDS.get(range_param)
    if not secs:
        return None
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(seconds=secs)).isoformat()


# ---------- lifecycle ----------

@app.on_event("startup")
async def _startup() -> None:
    _init_db()
    tok, sid, blk, sums = _backfill_derived_columns()
    if tok or sid or blk or sums:
        log.info("backfilled rows: tokens=%d session_id=%d blocks=%d summaries=%d", tok, sid, blk, sums)
    global _poll_task, _paperclip_poll_task, _retention_task
    _poll_task = asyncio.create_task(_poll_loop())
    _paperclip_poll_task = asyncio.create_task(_paperclip_poll_loop())
    _retention_task = asyncio.create_task(_retention_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    for t in (_poll_task, _paperclip_poll_task, _retention_task):
        if t is not None:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t


# ---------- routes ----------

@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "cache_proxy_url": CACHE_PROXY_URL,
        "db_path": DB_PATH,
        "tracked_exchanges": len(_seen_ids),
        "paperclip": paperclip_client.status(),
    }


@app.get("/api/paperclip/probe")
async def paperclip_probe() -> JSONResponse:
    """Smoke-test endpoint. Tries each paperclip read endpoint and reports
    HTTP status. Useful for diagnosing JWT/company_id misconfig."""
    out: dict[str, Any] = {"configured": paperclip_client.is_configured()}
    if not out["configured"]:
        return JSONResponse(out)
    client = paperclip_client.PaperclipClient()
    try:
        agents = await client.list_agents()
        runs = await client.list_heartbeat_runs(limit=5)
        live = await client.list_live_runs(limit=5)
        out["agents"] = {
            "ok": agents is not None,
            "count": len(agents) if isinstance(agents, list) else None,
            "sample": agents[:2] if isinstance(agents, list) else None,
        }
        out["heartbeat_runs"] = {
            "ok": runs is not None,
            "count": len(runs) if isinstance(runs, list) else None,
            "sample_keys": list(runs[0].keys()) if isinstance(runs, list) and runs else None,
        }
        out["live_runs"] = {
            "ok": live is not None,
            "count": len(live) if isinstance(live, list) else None,
        }
    finally:
        await client.aclose()
    return JSONResponse(out)


@app.get("/api/exchanges")
async def list_exchanges(
    limit: int = 200,
    before_ts: str | None = None,
    session_id: str | None = None,
    range: str | None = None,
) -> JSONResponse:
    limit = max(1, min(limit, 1000))
    since = _since_iso(range)

    # context_delta = (this row's input context) - (prior row's input context in
    # the same session), via LAG(). The window must be computed BEFORE the outer
    # ORDER BY/LIMIT, but scoping the inner scan to the time range / session keeps
    # it from re-reading the entire (multi-GB) table on every poll. The first row
    # in a window gets a NULL prior → NULL delta (rendered as "—" client-side).
    inner_clauses: list[str] = []
    inner_params: list[Any] = []
    if session_id:
        inner_clauses.append("session_id = ?")
        inner_params.append(session_id)
    if since:
        inner_clauses.append("ts >= ?")
        inner_params.append(since)
    inner_where = ("WHERE " + " AND ".join(inner_clauses)) if inner_clauses else ""

    outer_clauses: list[str] = []
    outer_params: list[Any] = []
    if before_ts:
        outer_clauses.append("ts < ?")
        outer_params.append(before_ts)
    outer_where = ("WHERE " + " AND ".join(outer_clauses)) if outer_clauses else ""

    # Select only the columns _summary_from_row needs — crucially NOT detail_json,
    # which is a large (~hundreds of KB) blob per row. Pulling it through the
    # window function was dragging hundreds of MB through the CTE on every call.
    _COLS = ("id, ts, method, path, model, streaming, status, duration_ms, "
             "input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, "
             "error, session_id, req_block_types, resp_block_types, req_summary, resp_summary")
    sql = f"""
        WITH with_delta AS (
            SELECT {_COLS},
                   (COALESCE(input_tokens,0) + COALESCE(cache_read_tokens,0) + COALESCE(cache_creation_tokens,0))
                   - LAG(COALESCE(input_tokens,0) + COALESCE(cache_read_tokens,0) + COALESCE(cache_creation_tokens,0))
                       OVER (PARTITION BY session_id ORDER BY ts)
                   AS context_delta
              FROM exchanges
              {inner_where}
        )
        SELECT * FROM with_delta
        {outer_where}
        ORDER BY ts DESC LIMIT ?
    """
    params = [*inner_params, *outer_params, limit]

    def _run() -> list[dict[str, Any]]:
        with _connect() as conn:
            return [_summary_from_row(r) for r in conn.execute(sql, params).fetchall()]

    rows = await asyncio.to_thread(_run)
    return JSONResponse({"count": len(rows), "exchanges": rows})


@app.get("/api/sessions")
async def list_sessions(range: str | None = None, limit: int = 200) -> JSONResponse:
    """Aggregated session list. Each entry is one Claude-Code session_id with
    rollup counters and a prompt preview from its earliest exchange."""
    limit = max(1, min(limit, 1000))
    since = _since_iso(range)
    clauses = ["session_id IS NOT NULL"]
    params: list[Any] = []
    sql = f"""
        SELECT session_id,
               MIN(ts)                 AS first_ts,
               MAX(ts)                 AS last_ts,
               COUNT(*)                AS exchange_count,
               COALESCE(SUM(input_tokens),0)        AS input_tokens,
               COALESCE(SUM(output_tokens),0)       AS output_tokens,
               COALESCE(SUM(cache_read_tokens),0)   AS cache_read_tokens,
               COALESCE(SUM(cache_creation_tokens),0) AS cache_creation_tokens,
               COALESCE(SUM(duration_ms),0)         AS total_duration_ms,
               MAX(model)              AS model_any,
               SUM(CASE WHEN status >= 400 OR error IS NOT NULL THEN 1 ELSE 0 END) AS error_count
          FROM exchanges
         WHERE {' AND '.join(clauses)}
         GROUP BY session_id
    """
    if since:
        sql += " HAVING last_ts >= ?"
        params.append(since)
    sql += " ORDER BY last_ts DESC LIMIT ?"
    params.append(limit)

    now = datetime.now(timezone.utc)

    def _run() -> tuple[list[dict[str, Any]], float]:
      sessions: list[dict[str, Any]] = []
      total_cost = 0.0
      with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        for r in rows:
            row = dict(r)
            # earliest exchange — pull its body to build the prompt preview
            first = conn.execute(
                "SELECT detail_json FROM exchanges WHERE session_id = ? ORDER BY ts ASC LIMIT 1",
                (row["session_id"],),
            ).fetchone()
            preview: str | None = None
            if first:
                try:
                    preview = _first_user_prompt_preview(json.loads(first["detail_json"]))
                except Exception:
                    preview = None
            # Cost estimate using the dominant model.
            pricing = _model_pricing(row.get("model_any"))
            cost: float | None = None
            if pricing:
                cost = (
                    row["input_tokens"] * pricing["input"]
                    + row["output_tokens"] * pricing["output"]
                    + row["cache_read_tokens"] * pricing["cache_read"]
                    + row["cache_creation_tokens"] * pricing["cache_write"]
                ) / 1_000_000
                total_cost += cost
            # "Active" if the most recent exchange landed in the last 60s.
            active = False
            try:
                last_ts = datetime.fromisoformat(row["last_ts"])
                active = (now - last_ts).total_seconds() < 60
            except Exception:
                pass
            # Join: paperclip runs reference Claude session_id via sessionIdAfter
            # (the value the heartbeat exited with) or sessionIdBefore (the value
            # it inherited). Prefer the most recent matching paperclip run.
            pc_run_row = conn.execute(
                """SELECT pr.*, pa.name AS agent_name, pa.title AS agent_title,
                          pa.icon AS agent_icon, pa.url_key AS agent_url_key
                     FROM paperclip_runs pr
                LEFT JOIN paperclip_agents pa ON pa.id = pr.agent_id
                    WHERE pr.session_id_after = ? OR pr.session_id_before = ?
                    ORDER BY pr.started_at DESC LIMIT 1""",
                (row["session_id"], row["session_id"]),
            ).fetchone()
            paperclip_info: dict[str, Any] | None = None
            if pc_run_row:
                p = dict(pc_run_row)
                paperclip_info = {
                    "run_id": p["id"],
                    "agent_id": p["agent_id"],
                    "agent_name": p.get("agent_name"),
                    "agent_title": p.get("agent_title"),
                    "agent_icon": p.get("agent_icon"),
                    "agent_url_key": p.get("agent_url_key"),
                    "status": p["status"],
                    "invocation_source": p["invocation_source"],
                    "started_at": p["started_at"],
                    "finished_at": p["finished_at"],
                    "exit_code": p["exit_code"],
                    "error_code": p["error_code"],
                }
            sessions.append({
                "session_id": row["session_id"],
                "first_ts": row["first_ts"],
                "last_ts": row["last_ts"],
                "exchange_count": row["exchange_count"],
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "cache_read_tokens": row["cache_read_tokens"],
                "cache_creation_tokens": row["cache_creation_tokens"],
                "total_duration_ms": row["total_duration_ms"],
                "error_count": row["error_count"],
                "model": row["model_any"],
                "estimated_cost_usd": cost,
                "active": active,
                "prompt_preview": preview,
                "paperclip": paperclip_info,
            })
      return sessions, total_cost

    sessions, total_cost = await asyncio.to_thread(_run)
    active_count = sum(1 for s in sessions if s["active"])
    return JSONResponse({
        "count": len(sessions),
        "active_count": active_count,
        "estimated_total_cost_usd": round(total_cost, 4),
        "sessions": sessions,
    })


@app.get("/api/exchanges/{ex_id}")
async def get_exchange(ex_id: str) -> JSONResponse:
    def _run() -> sqlite3.Row | None:
        with _connect() as conn:
            return conn.execute(
                "SELECT detail_json FROM exchanges WHERE id = ?", (ex_id,)
            ).fetchone()

    row = await asyncio.to_thread(_run)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return JSONResponse(json.loads(row["detail_json"]))


def _model_pricing(model: str | None) -> dict[str, float] | None:
    if not model:
        return None
    m = model.lower()
    for key, prices in MODEL_PRICING.items():
        if key in m:
            return prices
    return None


@app.get("/api/stats")
async def stats(range: str | None = None) -> JSONResponse:
    since = _since_iso(range)
    where = "WHERE ts >= ?" if since else ""
    params: list[Any] = [since] if since else []

    def _run() -> tuple[list[sqlite3.Row], sqlite3.Row]:
        with _connect() as conn:
            rows = conn.execute(
                f"""
                SELECT model,
                       COUNT(*) AS n,
                       COALESCE(SUM(input_tokens),0) AS input_tokens,
                       COALESCE(SUM(output_tokens),0) AS output_tokens,
                       COALESCE(SUM(cache_read_tokens),0) AS cache_read_tokens,
                       COALESCE(SUM(cache_creation_tokens),0) AS cache_creation_tokens
                  FROM exchanges
                  {where}
                 GROUP BY model
                """,
                params,
            ).fetchall()
            total_row = conn.execute(
                f"SELECT COUNT(*) AS n FROM exchanges {where}", params
            ).fetchone()
        return rows, total_row

    rows, total_row = await asyncio.to_thread(_run)

    by_model: list[dict[str, Any]] = []
    total_cost = 0.0
    totals = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_creation_tokens": 0,
    }
    for r in rows:
        d = dict(r)
        pricing = _model_pricing(d.get("model"))
        cost: float | None = None
        if pricing:
            cost = (
                d["input_tokens"] * pricing["input"]
                + d["output_tokens"] * pricing["output"]
                + d["cache_read_tokens"] * pricing["cache_read"]
                + d["cache_creation_tokens"] * pricing["cache_write"]
            ) / 1_000_000
            total_cost += cost
        for k in totals:
            totals[k] += d[k]
        by_model.append({**d, "estimated_cost_usd": cost})
    return JSONResponse({
        "total_exchanges": total_row["n"],
        "totals": totals,
        "estimated_total_cost_usd": round(total_cost, 4),
        "by_model": by_model,
    })


@app.get("/api/events")
async def sse_events() -> StreamingResponse:
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
    _subscribers.add(queue)
    log.info("SSE subscriber connected, total=%d", len(_subscribers))

    async def gen() -> AsyncIterator[bytes]:
        # initial hello so the client knows the stream is alive
        yield b"event: hello\ndata: {}\n\n"
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield msg.encode("utf-8")
                except asyncio.TimeoutError:
                    # keep-alive comment
                    yield b": ping\n\n"
        finally:
            _subscribers.discard(queue)
            log.info("SSE subscriber disconnected, total=%d", len(_subscribers))

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/")
async def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
