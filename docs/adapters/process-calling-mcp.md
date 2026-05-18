---
title: Calling MCP Tools from a Process Agent
summary: Use plain HTTP from a bash/python process agent to call self-hosted MCP servers, reusing existing per-server auth.
---

A process agent is a shell command — it doesn't have an LLM-driven tool loop, so it can't talk to MCP servers through the usual model integration. But if your MCP servers are self-hosted and speak Streamable HTTP, the agent can call them directly with `curl` + a JSON-RPC `tools/call` request.

## Prerequisites

- The process agent runs alongside your MCP servers on a shared Docker network (so it can reach them by service name).
- Each MCP server you want to call already has its own bearer-token auth — you'll pass that token through.
- The paperclip server has `PAPERCLIP_AGENT_JWT_SECRET` (or `BETTER_AUTH_SECRET`) set, so the process adapter auto-injects `PAPERCLIP_API_KEY` for talking back to paperclip itself.

## Configure the agent's environment

Set one URL and one token env var per MCP server in the agent's `adapterConfig.env`:

```json
{
  "adapterType": "process",
  "adapterConfig": {
    "command": "bash /workspace/agent.sh",
    "env": {
      "WORKSPACE_MCP_URL": "http://workspace-mcp:8080/mcp",
      "WORKSPACE_MCP_TOKEN": "${SECRET:workspace_mcp_token}",
      "DONALD_MCP_URL": "http://donald-mcp:8080/mcp",
      "DONALD_MCP_TOKEN": "${SECRET:donald_mcp_token}"
    }
  }
}
```

`PAPERCLIP_API_KEY`, `PAPERCLIP_API_URL`, `PAPERCLIP_AGENT_ID`, `PAPERCLIP_COMPANY_ID`, and `PAPERCLIP_RUN_ID` are injected automatically by paperclip — you don't need to set those.

## The recipe

Drop this `mcp_call` helper near the top of your agent script:

```bash
mcp_call() {
  local url="$1" token="$2" tool="$3" args="$4"
  curl -fsS -X POST "$url" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d "$(jq -nc --arg t "$tool" --argjson a "$args" \
          '{jsonrpc:"2.0",id:1,method:"tools/call",params:{name:$t,arguments:$a}}')" \
    | jq -r '.result.content[0].text // .'
}
```

Then call MCP tools with three arguments — URL, token, tool name — plus a JSON args object:

```bash
mcp_call "$WORKSPACE_MCP_URL" "$WORKSPACE_MCP_TOKEN" \
  search_gmail_messages '{"query":"label:inbox newer_than:1d"}'
```

The function returns the tool's `result.content[0].text` on stdout, or falls back to the full JSON-RPC response if the tool returned a non-text payload. `curl -fsS` makes the script exit non-zero on HTTP errors (4xx/5xx), so a failed MCP call propagates as a failed run.

## Talking back to paperclip itself

The same agent can manage issues, post comments, and read its assignments using the injected `PAPERCLIP_API_KEY`:

```bash
# Who am I?
curl -fsS -H "Authorization: Bearer $PAPERCLIP_API_KEY" \
  "$PAPERCLIP_API_URL/api/agents/me"

# Post a comment on an issue
curl -fsS -X POST \
  -H "Authorization: Bearer $PAPERCLIP_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"body":"Found 3 unread inbox threads"}' \
  "$PAPERCLIP_API_URL/api/issues/$ISSUE_ID/comments"
```

`PAPERCLIP_API_KEY` is a short-lived JWT minted per run; the MCP tokens are separate, longer-lived secrets you control.

## Auth model

- **Per MCP server**: one shared bearer token, set in the agent's env. Any agent with that token can call any tool on that server. Trade-off: no per-agent attribution on the MCP side.
- **For paperclip itself**: a fresh per-run JWT identifies the calling agent and its company; the auth middleware sets `req.actor.agentId` accordingly.

If you ever need per-agent attribution on the MCP side, either provision per-agent MCP tokens or add a middleware on the MCP server that verifies the agent's `PAPERCLIP_API_KEY` against paperclip's `/api/agents/me` endpoint before serving tool calls.

## Worked example

A bash agent that summarizes recent inbox and reports back via a paperclip comment:

```bash
#!/usr/bin/env bash
set -euo pipefail

mcp_call() {
  local url="$1" token="$2" tool="$3" args="$4"
  curl -fsS -X POST "$url" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d "$(jq -nc --arg t "$tool" --argjson a "$args" \
          '{jsonrpc:"2.0",id:1,method:"tools/call",params:{name:$t,arguments:$a}}')" \
    | jq -r '.result.content[0].text // .'
}

threads=$(mcp_call "$WORKSPACE_MCP_URL" "$WORKSPACE_MCP_TOKEN" \
  search_gmail_messages '{"query":"label:inbox newer_than:1d","max_results":20}')

summary=$(printf 'Found inbox activity:\n%s' "$threads")

curl -fsS -X POST \
  -H "Authorization: Bearer $PAPERCLIP_API_KEY" \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg b "$summary" '{body:$b}')" \
  "$PAPERCLIP_API_URL/api/issues/$PAPERCLIP_RUN_ID/comments"
```
