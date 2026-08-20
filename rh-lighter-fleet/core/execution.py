"""Order execution against production: create / cancel / modify / batch.

Everything goes through the venue's sendTx / sendTxBatch. Every response is
logged (code, tx_hash, quota). A sendTx code=200 is NOT treated as an open
order — order state is confirmed by the WebSocket account stream (ws_hub) and
tracked per-worker.
"""

from __future__ import annotations

import asyncio
import itertools
import time

from .config import MAX_BATCH_TX_REST
from .logging_utils import activity
from .market_registry import Market
from .signer_pool import SignerPool

# ── L2 tx types (docs/signing-transactions.md) ────────────────────────────────
TX_TYPE_CREATE_ORDER = 14
TX_TYPE_CANCEL_ORDER = 15
TX_TYPE_CANCEL_ALL_ORDERS = 16
TX_TYPE_MODIFY_ORDER = 17

# ── Error codes the fleet actively handles (docs/...constants-and-errors.md) ──
ERROR_CODES = {
    21733: "FatFingerPrice",
    21734: "PriceTooFarFromMarkPrice",
    21739: "NotEnoughOrderMargin",
    21717: "MaxOrdersPerAccount",
    21718: "MaxOrdersPerMarket",
    21705: "PostOnlyWouldCross",
    21728: "ClientOrderIndexExists",
    21104: "InvalidNonce",
    21120: "InvalidSignature",
    23000: "TooManyRequests",
    21507: "BelowMaintenanceMargin",
}

# Cancel-reason suffixes streamed on the WS order channel.
CANCEL_REASONS = {
    "canceled-post-only": "price crossed the book — reprice further from mid",
    "canceled-margin-not-allowed": "reduce size or add margin",
    "canceled-fat-finger": "price outside mark bounds — clamp harder",
    "canceled-not-enough-liquidity": "market order could not fill",
    "canceled-expired": "order_expiry elapsed — refresh expiry",
    "canceled-self-trade": "would self-trade",
}

_counter = itertools.count()


def unique_client_order_index() -> int:
    """uint48, unique across ALL markets on the account."""
    return (time.time_ns() // 1_000 + next(_counter)) % (2**48)


class ExecutionError(Exception):
    def __init__(self, code: int | None, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.name = ERROR_CODES.get(code or 0, "Unknown")


def _extract_code(err: str) -> int | None:
    for code in ERROR_CODES:
        if str(code) in err:
            return code
    return None


class ExecutionEngine:
    """All order flow for the fleet. One instance shared by every worker."""

    def __init__(self, pool: SignerPool) -> None:
        self.pool = pool
        self.client = pool.client
        self.sent_tx_count = 0
        self.last_send_ts: float = time.time()
        self.rate_limited_until: float = 0.0
        self._veto_log: list[str] = []

    # ── bookkeeping used by the watchdog ─────────────────────────────────────
    def record_veto(self, reason: str) -> None:
        self._veto_log.append(f"{time.strftime('%H:%M:%S')} {reason}")
        if len(self._veto_log) > 40:
            self._veto_log = self._veto_log[-40:]

    def recent_vetoes(self, n: int = 20) -> list[str]:
        return self._veto_log[-n:]

    def _sent(self) -> None:
        self.sent_tx_count += 1
        self.last_send_ts = time.time()

    async def _pre_send(self) -> None:
        wait = self.rate_limited_until - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

    def _handle_resp(self, market: Market, action: str, resp, err) -> None:
        if err is not None:
            code = _extract_code(str(err))
            if code == 23000:
                self.rate_limited_until = time.time() + 2.0
            raise ExecutionError(code, str(err))
        code = getattr(resp, "code", None)
        tx_hash = getattr(resp, "tx_hash", "")
        quota = getattr(resp, "volume_quota_remaining", None)
        extra = f" quota={quota}" if quota is not None else ""
        activity.ok("RESP", market.symbol, f"{action} code={code} tx={_short(tx_hash)}{extra}")
        if code is not None and code != 200:
            raise ExecutionError(code, f"{action} rejected code={code}")

    # ── order primitives ─────────────────────────────────────────────────────
    async def place_post_only(
        self,
        market: Market,
        is_ask: bool,
        price: float,
        size_base: float,
        reduce_only: bool = False,
    ) -> int:
        """Post-only limit (the ONLY order type MM quotes use).
        Returns the client_order_index used."""
        await self._pre_send()
        coi = unique_client_order_index()
        price_int = market.to_price_int(price)
        size_int = market.to_size_int(size_base)
        side = "ask" if is_ask else "bid"
        activity.ok(
            "SEND",
            market.symbol,
            f"create POST_ONLY {side} px={price} sz={size_base:.6g} coi={coi} tx_type={TX_TYPE_CREATE_ORDER}",
        )
        key = self.pool.key_for_market(market.market_id)
        async with self.pool.lock_for_key(key):
            tx, resp, err = await self.client.create_order(
                market_index=market.market_id,
                client_order_index=coi,
                base_amount=size_int,
                price=price_int,
                is_ask=is_ask,
                order_type=self.client.ORDER_TYPE_LIMIT,
                time_in_force=self.client.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=reduce_only,
                order_expiry=self.client.DEFAULT_28_DAY_ORDER_EXPIRY,
                api_key_index=key,
            )
        self._sent()
        try:
            self._handle_resp(market, f"create {side}", resp, err)
        except ExecutionError as exc:
            if exc.code == 21104:
                await self.pool.resync_nonce(key)
            raise
        return coi

    async def place_market_ioc(
        self,
        market: Market,
        is_ask: bool,
        size_base: float,
        worst_price: float,
        reduce_only: bool = True,
    ) -> int:
        """IOC market order (flatten / instant close). worst_price caps slippage."""
        await self._pre_send()
        coi = unique_client_order_index()
        side = "SELL" if is_ask else "BUY"
        activity.ok(
            "SEND",
            market.symbol,
            f"market IOC {side} sz={size_base:.6g} worst_px={worst_price} coi={coi}",
        )
        key = self.pool.key_for_market(market.market_id)
        async with self.pool.lock_for_key(key):
            tx, resp, err = await self.client.create_order(
                market_index=market.market_id,
                client_order_index=coi,
                base_amount=market.to_size_int(size_base),
                price=market.to_price_int(worst_price),
                is_ask=is_ask,
                order_type=self.client.ORDER_TYPE_MARKET,
                time_in_force=self.client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=self.client.DEFAULT_IOC_EXPIRY,
                api_key_index=key,
            )
        self._sent()
        try:
            self._handle_resp(market, f"market {side}", resp, err)
        except ExecutionError as exc:
            if exc.code == 21104:
                await self.pool.resync_nonce(key)
            raise
        return coi

    async def cancel(self, market: Market, order_index: int) -> None:
        await self._pre_send()
        activity.ok("SEND", market.symbol, f"cancel order_index={order_index} tx_type={TX_TYPE_CANCEL_ORDER}")
        key = self.pool.key_for_market(market.market_id)
        async with self.pool.lock_for_key(key):
            tx, resp, err = await self.client.cancel_order(
                market_index=market.market_id,
                order_index=order_index,
                api_key_index=key,
            )
        self._sent()
        try:
            self._handle_resp(market, "cancel", resp, err)
        except ExecutionError as exc:
            if exc.code == 21104:
                await self.pool.resync_nonce(key)
            raise

    async def modify(self, market: Market, order_index: int, price: float, size_base: float) -> None:
        """Prefer modify over cancel+create — saves volume quota on Premium."""
        await self._pre_send()
        activity.ok("SEND", market.symbol, f"modify order_index={order_index} px={price} sz={size_base:.6g}")
        key = self.pool.key_for_market(market.market_id)
        async with self.pool.lock_for_key(key):
            tx, resp, err = await self.client.modify_order(
                market_index=market.market_id,
                order_index=order_index,
                base_amount=market.to_size_int(size_base),
                price=market.to_price_int(price),
                api_key_index=key,
            )
        self._sent()
        self._handle_resp(market, "modify", resp, err)

    async def set_leverage(self, market: Market, leverage: int) -> None:
        """Set cross-margin leverage for a perp market (no-op for spot)."""
        if market.market_type != "perp":
            return
        await self._pre_send()
        key = self.pool.key_for_market(market.market_id)
        async with self.pool.lock_for_key(key):
            tx, resp, err = await self.client.update_leverage(
                market_index=market.market_id,
                margin_mode=self.client.CROSS_MARGIN_MODE,
                leverage=int(leverage),
                api_key_index=key,
            )
        self._sent()
        if err is not None:
            raise ExecutionError(_extract_code(str(err)), f"update_leverage: {err}")
        activity.ok("SEND", market.symbol, f"leverage set to {leverage}x (cross)")

    async def cancel_all(self, market_id: int | None = None) -> None:
        """Cancel everything (optionally scoped to one market) — used by
        flatten and the order-cap sweep."""
        await self._pre_send()
        key = self.pool.key_indices[0]
        kwargs = {}
        if market_id is not None:
            kwargs["cancel_all_market_index"] = market_id
        async with self.pool.lock_for_key(key):
            # Immediate cancel-all requires a nil (0) time on the venue;
            # a real timestamp is only valid for the SCHEDULED variant.
            tx, resp, err = await self.client.cancel_all_orders(
                time_in_force=self.client.CANCEL_ALL_TIF_IMMEDIATE,
                timestamp_ms=0,
                api_key_index=key,
                **kwargs,
            )
        self._sent()
        scope = f"market {market_id}" if market_id is not None else "ALL markets"
        if err is not None:
            activity.err("SEND", "", f"cancel_all {scope} failed: {err}")
            raise ExecutionError(_extract_code(str(err)), str(err))
        activity.ok("SEND", "", f"cancel_all {scope} code={getattr(resp, 'code', '?')}")

    # ── batching (sendTxBatch — one round trip per refresh cycle) ────────────
    async def replace_quotes_batch(
        self,
        market: Market,
        cancels: list[int],
        creates: list[tuple[bool, float, float]],  # (is_ask, price, size_base)
    ) -> list[int]:
        """[cancel, cancel, create, create] in ONE sendTxBatch round trip.
        Returns the client_order_index of each created order."""
        await self._pre_send()
        key = self.pool.key_for_market(market.market_id)
        tx_types: list[int] = []
        tx_infos: list[str] = []
        cois: list[int] = []
        async with self.pool.lock_for_key(key):
            for order_index in cancels:
                tx_type, tx_info, _tx_hash, err = self.client.sign_cancel_order(
                    market_index=market.market_id,
                    order_index=order_index,
                    api_key_index=key,
                )
                if err is not None:
                    raise ExecutionError(_extract_code(str(err)), f"sign cancel: {err}")
                tx_types.append(int(tx_type))
                tx_infos.append(tx_info)
            for is_ask, price, size_base in creates:
                coi = unique_client_order_index()
                tx_type, tx_info, _tx_hash, err = self.client.sign_create_order(
                    market_index=market.market_id,
                    client_order_index=coi,
                    base_amount=market.to_size_int(size_base),
                    price=market.to_price_int(price),
                    is_ask=is_ask,
                    order_type=self.client.ORDER_TYPE_LIMIT,
                    time_in_force=self.client.ORDER_TIME_IN_FORCE_POST_ONLY,
                    order_expiry=self.client.DEFAULT_28_DAY_ORDER_EXPIRY,
                    api_key_index=key,
                )
                if err is not None:
                    raise ExecutionError(_extract_code(str(err)), f"sign create: {err}")
                tx_types.append(int(tx_type))
                tx_infos.append(tx_info)
                cois.append(coi)

            if not tx_types:
                return []
            if len(tx_types) > MAX_BATCH_TX_REST:
                raise ExecutionError(None, f"batch too large: {len(tx_types)} > {MAX_BATCH_TX_REST}")

            activity.ok(
                "SEND",
                market.symbol,
                f"sendTxBatch n={len(tx_types)} types={tx_types}",
            )
            try:
                resp = await self.client.send_tx_batch(tx_types=tx_types, tx_infos=tx_infos)
            except Exception as exc:
                code = _extract_code(str(exc))
                if code == 23000:
                    self.rate_limited_until = time.time() + 2.0
                if code == 21104:
                    await self.pool.resync_nonce(key)
                raise ExecutionError(code, f"sendTxBatch: {exc}") from exc
        self._sent()
        activity.ok("RESP", market.symbol, f"sendTxBatch code={getattr(resp, 'code', '?')}")
        return cois


def _short(tx_hash: str | None) -> str:
    if not tx_hash:
        return "-"
    return tx_hash[:10] + "…" if len(tx_hash) > 12 else tx_hash
