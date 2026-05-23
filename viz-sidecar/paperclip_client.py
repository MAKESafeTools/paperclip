"""
Lightweight client that mints a local-agent JWT and reads run/issue metadata
from the paperclip server. Optional — enabled only if the relevant env vars
are set. Failures degrade gracefully (logged warning, no enrichment).

JWT contract mirrors paperclip/server/src/agent-auth-jwt.ts:
  HS256-signed, claims: sub (agent_id), company_id, adapter_type,
  run_id, iat, exp, iss=paperclip, aud=paperclip-api.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any

import httpx
import jwt as pyjwt

log = logging.getLogger("viz-sidecar.paperclip")

BASE_URL = os.environ.get("PAPERCLIP_BASE_URL", "http://paperclip:3100").rstrip("/")
# Same secret paperclip uses for verifyLocalAgentJwt (BETTER_AUTH_SECRET fallback).
JWT_SECRET = os.environ.get("PAPERCLIP_JWT_SECRET") or os.environ.get("BETTER_AUTH_SECRET")
COMPANY_ID = os.environ.get("PAPERCLIP_COMPANY_ID")
AGENT_ID = os.environ.get("PAPERCLIP_AGENT_ID", "viz-sidecar-readonly")
JWT_TTL_SECONDS = int(os.environ.get("PAPERCLIP_JWT_TTL_SECONDS", "3600"))
JWT_ISSUER = os.environ.get("PAPERCLIP_JWT_ISSUER", "paperclip")
JWT_AUDIENCE = os.environ.get("PAPERCLIP_JWT_AUDIENCE", "paperclip-api")


def is_configured() -> bool:
    return bool(JWT_SECRET and COMPANY_ID)


def status() -> dict[str, Any]:
    return {
        "configured": is_configured(),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "agent_id": AGENT_ID,
        "has_secret": bool(JWT_SECRET),
    }


_cached_token: str | None = None
_cached_exp: float = 0.0


def _mint_token() -> str | None:
    if not is_configured():
        return None
    global _cached_token, _cached_exp
    now = time.time()
    if _cached_token and now < _cached_exp - 60:
        return _cached_token
    payload = {
        "sub": AGENT_ID,
        "company_id": COMPANY_ID,
        "adapter_type": "viz-sidecar",
        "run_id": f"viz-{uuid.uuid4().hex[:8]}",
        "iat": int(now),
        "exp": int(now) + JWT_TTL_SECONDS,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
    }
    token = pyjwt.encode(payload, JWT_SECRET, algorithm="HS256")
    _cached_token = token if isinstance(token, str) else token.decode()
    _cached_exp = now + JWT_TTL_SECONDS
    return _cached_token


class PaperclipClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        token = _mint_token()
        if not token:
            return None
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{BASE_URL}{path}"
        r = await self._client.get(url, headers=headers, params=params)
        if r.status_code >= 400:
            log.warning("paperclip GET %s -> %d %s", path, r.status_code, r.text[:200])
            return None
        try:
            return r.json()
        except Exception as e:
            log.warning("paperclip GET %s: non-json (%s)", path, e)
            return None

    async def list_heartbeat_runs(self, limit: int = 200) -> list[dict[str, Any]] | None:
        data = await self._get(
            f"/api/companies/{COMPANY_ID}/heartbeat-runs", params={"limit": limit}
        )
        if data is None:
            return None
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("runs", "heartbeatRuns", "items"):
                v = data.get(k)
                if isinstance(v, list):
                    return v
        return None

    async def list_live_runs(self, limit: int = 50) -> list[dict[str, Any]] | None:
        data = await self._get(
            f"/api/companies/{COMPANY_ID}/live-runs", params={"limit": limit}
        )
        if data is None:
            return None
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("runs", "liveRuns", "items"):
                v = data.get(k)
                if isinstance(v, list):
                    return v
        return None

    async def list_agents(self) -> list[dict[str, Any]] | None:
        data = await self._get(f"/api/companies/{COMPANY_ID}/agents")
        if data is None:
            return None
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("agents", "items"):
                v = data.get(k)
                if isinstance(v, list):
                    return v
        return None
