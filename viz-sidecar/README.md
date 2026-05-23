# viz-sidecar

Stage 1 MVP visualization for paperclip's `cache-proxy`. Polls cache-proxy traffic, persists to SQLite, streams new exchanges to a browser UI via SSE.

See [STAGE0.md](STAGE0.md) for architecture rationale.

## Running

The service is gated behind a `viz` compose profile so the default `docker compose up` is unchanged.

```bash
# from paperclip repo root
docker compose --profile viz up -d viz-sidecar

# UI
open http://127.0.0.1:8013/
```

To stop:
```bash
docker compose --profile viz stop viz-sidecar
```

To rebuild after code changes:
```bash
docker compose --profile viz build viz-sidecar
docker compose --profile viz up -d viz-sidecar
```

## Endpoints

| Route | Purpose |
|---|---|
| `GET /` | Single-page UI |
| `GET /health` | Liveness, target URL, tracked exchange count |
| `GET /api/exchanges?limit=N&before_ts=ISO&session_id=&range=` | Paged exchange summaries, newest first. `range` ∈ `10m`/`1h`/`6h`/`24h`/`3d`/`7d`/`all`. |
| `GET /api/exchanges/{id}` | Full exchange record (raw cache-proxy payload) |
| `GET /api/sessions?range=&limit=N` | Aggregated session list with prompt previews, token rollups, cost estimates, active flag |
| `GET /api/stats?range=` | Cumulative tokens + est. cost, broken down by model |
| `GET /api/events` | SSE stream emitting `exchange` events as new traffic is captured |

## Environment

| Var | Default | Notes |
|---|---|---|
| `CACHE_PROXY_URL` | `http://cache-proxy:8000` | Source of truth |
| `DB_PATH` | `/data/viz.sqlite` | Persisted in `viz-data` volume |
| `POLL_INTERVAL_SECONDS` | `1.0` | Cache-proxy poll cadence |
| `LOG_LEVEL` | `INFO` | |

## Data model

```sql
CREATE TABLE exchanges (
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
    detail_json         TEXT NOT NULL  -- raw cache-proxy detail payload
);
```

`detail_json` stores the full cache-proxy `/traffic/exchanges/{id}` payload verbatim so we can change indexed columns later without re-fetching.

## Next stages

- **Stage 2 ✓:** session correlation via Claude Code's `x-claude-code-session-id`. Cache-proxy now emits a top-level `session_id` field on every exchange; viz-sidecar groups, aggregates, and presents in a sidebar.
- **Stage 3:** React + React Flow graph view (vertical message spine + branching runs/tools, ported from `my-claude-viz`). Parse Task `tool_use` blocks within request bodies to infer sub-agent boundaries.
- **Stage 4:** auto-expand active runs, stale detection, cache hit-rate badges per session, paperclip-server JWT integration to attach run/agent/task metadata to sessions.
- **Stage 5:** search, export, diff viewer, replay, design-token theming.
