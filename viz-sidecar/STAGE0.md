# Stage 0 — Data Flow & Architecture Decisions

## Goal
Build a richer visualization for Paperclip without modifying paperclip core. Sit alongside `cache-proxy` as a sibling container.

## Data sources
1. **cache-proxy** (`http://cache-proxy:8000` on `paperclip-net`)
   - `GET /traffic/exchanges` → summaries `{id, ts, method, path, model, streaming, status, duration_ms, usage}`, newest-first, ring-buffered (default 500)
   - `GET /traffic/exchanges/{id}` → full record incl. request body/headers (auth redacted), response body or reconstructed `assistant_text`, `usage`
   - No SSE/WS on cache-proxy → **poll**
2. **paperclip server** (`http://paperclip:3100/api/...`) — **deferred to Stage 2** for run/agent/session correlation.

## Correlation key (deferred)
- Cache-proxy already captures the full incoming header bag at [`cache-proxy/app.py:663`](../cache-proxy/app.py), then redacts auth headers at [`cache-proxy/app.py:307`](../cache-proxy/app.py) before storing.
- Any custom `X-Paperclip-Run-Id`-style header set by the process adapter would survive redaction and land in `request.headers`.
- Decision deferred. Stage 1 stores raw exchanges; Stage 2 picks a strategy (custom header vs. JWT-claim extraction vs. timestamp-window join).

## Transport choices
| Hop | Choice | Why |
|---|---|---|
| cache-proxy → viz | HTTP poll (1s) | cache-proxy has no push channel |
| viz → frontend | SSE | proven in my-claude-viz; simpler than WS; one-way fine |
| viz persistence | SQLite in `viz-data` volume | survives ring-buffer rotation; zero-ops |
| backend stack | Python/FastAPI | mirrors cache-proxy; team familiarity |
| MVP frontend | single-file vanilla JS | defers build chain until graph view (Stage 3) needs React Flow |

## Container topology
```
                                  ┌────────────┐
host:8012 ◄────────────────────── │ cache-proxy│ ──► api.anthropic.com
                                  │   :8000    │
                                  └────┬───────┘
                                       │ poll /traffic/exchanges
                                  ┌────▼───────┐
host:8013 ◄────────────────────── │ viz-sidecar│
                  (UI + SSE)      │   :8000    │
                                  └────┬───────┘
                                       │ (Stage 2) GET /api/...
                                  ┌────▼───────┐
host:3100 ◄────────────────────── │ paperclip  │
                                  └────────────┘
```

## Docker compose
New service under a `viz` profile so the default `docker compose up` is unchanged:
```yaml
docker compose --profile viz up viz-sidecar
```

## Non-goals for Stage 1
- Run/agent correlation
- Graph rendering (React Flow)
- Sub-agent inference from Task tool_use blocks
- Auth — viz UI is `127.0.0.1`-bound only

## Exit criterion
Run an agent inside paperclip. Open `http://127.0.0.1:8013/` and see every API call appear live with request/response detail and a cumulative-token banner. Stop the container, restart, see history persisted.
