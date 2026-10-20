"""Context reads: REST /v1 and MCP, side by side (the parity exercise)."""

from __future__ import annotations

from typing import Any

import httpx


class ContextReader:
    """Reads one user's context from REST, then the identical body from MCP."""

    def __init__(self, rest_base: str, mcp_base: str, token: str, *, timeout_s: float = 10.0) -> None:
        self._rest_base = rest_base.rstrip("/")
        self._mcp_base = mcp_base.rstrip("/")
        self._token = token
        self._timeout_s = timeout_s

    def rest_context(self, user: str, capability: str = "skills") -> dict[str, Any]:
        response = httpx.get(
            f"{self._rest_base}/v1/context/{user}/{capability}",
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=self._timeout_s,
        )
        response.raise_for_status()
        return response.json()

    def mcp_context(self, user: str, capability: str = "skills") -> dict[str, Any]:
        """Stateless JSON mode: initialize then tools/call, two raw POSTs."""
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        with httpx.Client(timeout=self._timeout_s) as client:
            init = client.post(
                f"{self._mcp_base}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2026-07-28",
                        "capabilities": {},
                        "clientInfo": {"name": "poc-demo", "version": "0.1.0"},
                    },
                },
                headers=headers,
            )
            init.raise_for_status()
            call = client.post(
                f"{self._mcp_base}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "get_context", "arguments": {"user": user, "capability": capability}},
                },
                headers=headers,
            )
            call.raise_for_status()
            body = call.json()
        return body

    def parity(self, user: str, capability: str = "skills") -> tuple[dict[str, Any], dict[str, Any], bool]:
        rest = self.rest_context(user, capability)
        mcp = self.mcp_context(user, capability)
        mcp_payload = mcp["result"]["structuredContent"]["payload"]
        return rest["payload"], mcp_payload, mcp_payload == rest["payload"]
