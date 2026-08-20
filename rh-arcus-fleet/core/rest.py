"""Async REST client: bootstrap reads + kill-switch writes.

Hot-path order flow goes over the WebSocket. REST is used for:
  * market discovery, fee tiers, server time
  * venue-truth reconcile (account / positions / open orders) every 30s
  * cancelAllOrders / setLeverage (scheme-2 signed)
  * createApiKey (setup-key)
"""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from .config import BASE_URL
from .logging_utils import activity
from .signing import (
    Ed25519PrivateKey,
    auth_headers,
    canonical_json,
    now_ns,
    public_key_hex,
    sign_scheme2,
)


class RestError(Exception):
    def __init__(self, status: int, body: Any) -> None:
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class ArcusRest:
    def __init__(
        self,
        base_url: str = BASE_URL,
        *,
        private_key: Ed25519PrivateKey | None = None,
        address: str = "",
        account_index: int = 0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.priv = private_key
        self.api_key = public_key_hex(private_key) if private_key else ""
        self.address = address
        self.account_index = account_index
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"Content-Type": "application/json"},
            )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        body: dict | None = None,
        headers: dict | None = None,
        retries: int = 2,
    ) -> Any:
        if self._session is None:
            await self.start()
        assert self._session is not None
        url = f"{self.base_url}{path}"
        data = canonical_json(body).encode() if body is not None else None
        for attempt in range(retries + 1):
            async with self._session.request(
                method, url, params=params, data=data, headers=headers
            ) as resp:
                if resp.status == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    if attempt < retries:
                        activity.warn("REST", "", f"429 on {path} — backing off {retry_after:.1f}s")
                        await asyncio.sleep(retry_after)
                        continue
                try:
                    payload = await resp.json(content_type=None)
                except Exception:
                    payload = await resp.text()
                if resp.status >= 400:
                    raise RestError(resp.status, payload)
                return payload
        raise RestError(429, {"error": "rate limited after retries"})

    async def get(self, path: str, **params: Any) -> Any:
        clean = {k: v for k, v in params.items() if v is not None}
        return await self._request("GET", path, params=clean or None)

    async def health(self) -> Any:
        return await self.get("/health")

    async def time_ns(self) -> int:
        data = await self.get("/v1/time")
        if isinstance(data, dict):
            return int(data.get("timeNs") or data.get("time") or data.get("serverTime") or 0)
        return int(data)

    async def markets(self) -> list[dict]:
        data = await self.get("/v1/markets")
        return data.get("markets", data if isinstance(data, list) else [])

    async def fee_tiers(self) -> list[dict]:
        data = await self.get("/v1/feeTiers")
        return data.get("tiers", data if isinstance(data, list) else [])

    async def account(self, address: str | None = None, account_index: int | None = None) -> dict:
        return await self.get(
            "/v1/account",
            address=address or self.address,
            accountIndex=account_index if account_index is not None else self.account_index,
        )

    async def positions(self, address: str | None = None, account_index: int | None = None) -> list[dict]:
        data = await self.get(
            "/v1/positions",
            address=address or self.address,
            accountIndex=account_index if account_index is not None else self.account_index,
        )
        if isinstance(data, list):
            return data
        return data.get("positions") or []

    async def open_orders(self, address: str | None = None, account_index: int | None = None) -> list[dict]:
        data = await self.get(
            "/v1/openOrders",
            address=address or self.address,
            accountIndex=account_index if account_index is not None else self.account_index,
        )
        if isinstance(data, list):
            return data
        return data.get("orders") or []

    async def rate_limit(self, address: str | None = None, account_index: int | None = None) -> dict:
        return await self.get(
            "/v1/rateLimit",
            address=address or self.address,
            account_index=account_index if account_index is not None else self.account_index,
        )

    async def api_keys(self, address: str) -> list[dict]:
        data = await self.get("/v1/apiKeys", address=address)
        return data.get("apiKeys", data if isinstance(data, list) else [])

    async def create_api_key(self, body: dict) -> Any:
        return await self._request("POST", "/v1/createApiKey", body=body)

    def _signed_headers(self, signature: str, ts: int) -> dict[str, str]:
        if not self.priv:
            raise RuntimeError("API key required for mutating REST")
        return auth_headers(self.api_key, ts, signature)

    async def cancel_all(self, market_id: int | None = None) -> Any:
        """Kill-switch. Scheme 2. Charges the cancel pool a flat 1,000."""
        if not self.priv:
            raise RuntimeError("API key required")
        ts = now_ns()
        body: dict[str, Any] = {
            "address": self.address,
            "accountIndex": self.account_index,
        }
        if market_id is not None:
            body["marketId"] = market_id
        sig = sign_scheme2(self.priv, ts, "cancelAllOrders", body)
        return await self._request(
            "POST",
            "/v1/cancelAllOrders",
            body=body,
            headers=self._signed_headers(sig, ts),
        )

    async def set_leverage(self, market_id: int, leverage: int) -> Any:
        if not self.priv:
            raise RuntimeError("API key required")
        ts = now_ns()
        body = {
            "address": self.address,
            "accountIndex": self.account_index,
            "marketId": market_id,
            "leverage": int(leverage),
        }
        sig = sign_scheme2(self.priv, ts, "setLeverage", body)
        return await self._request(
            "POST",
            "/v1/setLeverage",
            body=body,
            headers=self._signed_headers(sig, ts),
        )
