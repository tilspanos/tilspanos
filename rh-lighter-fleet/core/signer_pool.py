"""SignerClient pool with per-key nonce isolation.

The venue keeps an independent nonce per API key. To let dozens of market
workers sign in parallel without nonce contention, each market is pinned to
one API key (round-robin) and every sign call for that key is serialised
behind a per-key asyncio lock. The SDK's optimistic nonce manager handles
increments; on InvalidNonce (21104) callers resync via `resync_nonce`.
"""

from __future__ import annotations

import asyncio

import lighter

from .config import BASE_URL, CHAIN_ID, FleetConfig
from .logging_utils import activity


class SignerPool:
    def __init__(self, cfg: FleetConfig, account_index: int) -> None:
        if not cfg.keys:
            raise SystemExit("FATAL: SignerPool requires at least one API key.")
        self.cfg = cfg
        self.account_index = account_index
        self.client = lighter.SignerClient(
            url=BASE_URL,
            account_index=account_index,
            api_private_keys={k.api_key_index: k.private_key for k in cfg.keys},
            chain_id=CHAIN_ID,
        )
        self.key_indices: list[int] = [k.api_key_index for k in cfg.keys]
        self._locks: dict[int, asyncio.Lock] = {i: asyncio.Lock() for i in self.key_indices}
        self._market_key: dict[int, int] = {}
        self._rr = 0

    # ── key assignment ────────────────────────────────────────────────────────
    def key_for_market(self, market_id: int) -> int:
        """Pin each market to one API key (round-robin on first sight)."""
        if market_id not in self._market_key:
            self._market_key[market_id] = self.key_indices[self._rr % len(self.key_indices)]
            self._rr += 1
        return self._market_key[market_id]

    def lock_for_key(self, api_key_index: int) -> asyncio.Lock:
        return self._locks[api_key_index]

    def lock_for_market(self, market_id: int) -> asyncio.Lock:
        return self._locks[self.key_for_market(market_id)]

    # ── auth ─────────────────────────────────────────────────────────────────
    def create_auth_token(self, deadline_s: int = 3600) -> str:
        """Auth token for authenticated REST/WS channels (max 8h expiry)."""
        token, err = self.client.create_auth_token_with_expiry(
            deadline=deadline_s,
            api_key_index=self.key_indices[0],
        )
        if err is not None:
            raise RuntimeError(f"create_auth_token failed: {err}")
        return token

    # ── nonce recovery ───────────────────────────────────────────────────────
    async def resync_nonce(self, api_key_index: int) -> None:
        """Refetch nextNonce after an InvalidNonce (21104) rejection."""
        try:
            nm = getattr(self.client, "nonce_manager", None)
            if nm is not None and hasattr(nm, "hard_refresh_nonce"):
                await nm.hard_refresh_nonce(api_key_index)
            elif nm is not None and hasattr(nm, "refresh_nonce"):
                await nm.refresh_nonce(api_key_index)
            activity.warn("NONCE", "", f"key {api_key_index}: nonce resynced via nextNonce")
        except Exception as exc:  # pragma: no cover - venue hiccup
            activity.err("NONCE", "", f"key {api_key_index}: nonce resync failed: {exc}")

    async def close(self) -> None:
        await self.client.close()


async def discover_account_index(l1_address: str) -> int:
    """Step 1 of the official get-started flow: accountsByL1Address."""
    api_client = lighter.ApiClient(lighter.Configuration(host=BASE_URL))
    try:
        try:
            resp = await lighter.AccountApi(api_client).accounts_by_l1_address(l1_address=l1_address)
        except lighter.ApiException as exc:
            raise SystemExit(
                f"FATAL: no Robinhood Lighter account found for {l1_address} "
                f"(venue returned {exc.status}). Make sure this wallet has been "
                "onboarded at https://robinhoodchain.lighter.xyz and holds USDG."
            ) from exc
        subs = resp.sub_accounts
        if not subs:
            raise SystemExit(f"FATAL: no Lighter account found for {l1_address} on production.")
        return subs[0].index
    finally:
        await api_client.close()
