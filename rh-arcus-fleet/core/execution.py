"""Order execution against production Arcus.

Every mutating call goes through signed WS RPCs (REST cancel-all / leverage
are the kill-switch path). A 202 ACK or even a 200 body is NEVER treated as
an open order — the `orders` / `userFills` streams confirm state.
"""

from __future__ import annotations

import itertools
import time
from typing import Any

from .config import GOOD_TIL_DAYS, MAX_BATCH_ORDERS, MAX_MARKET_SLIPPAGE
from .decimalx import D, fmt_decimal, snap
from .logging_utils import activity
from .market_registry import Market
from .rest import ArcusRest, RestError
from .signing import (
    Ed25519PrivateKey,
    good_til_us,
    now_ns,
    public_key_hex,
    sign_scheme1,
    typed_cancel_payload,
    typed_place_payload,
)
from .ws_hub import WsHub

_counter = itertools.count(1)

REJECT_HINTS = {
    "POST_ONLY_WOULD_CROSS": "price crossed the book — reprice further from mid",
    "SELF_TRADE": "would self-trade",
    "FAT_FINGER": "price outside venue bounds — clamp harder",
    "INSUFFICIENT_MARGIN": "reduce size or add margin",
    "MIN_SIZE": "below venue min size / notional (MARKET too) — top-up dust",
    "INVALID_QUANTITY": "size not on step / below min",
}


class ExecutionError(Exception):
    def __init__(self, name: str, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.name = name
        self.status = status
        self.code = status


def unique_client_id(market_id: int, side: str) -> str:
    """Lowercase, unique among live orders, [A-Za-z0-9_-], reused after terminal."""
    tag = "b" if side.upper() in ("BUY", "BID") else "a"
    return f"mm{market_id}{tag}{next(_counter)}".lower()


class ExecutionEngine:
    def __init__(
        self,
        hub: WsHub,
        rest: ArcusRest,
        priv: Ed25519PrivateKey,
        address: str,
        account_index: int,
    ) -> None:
        self.hub = hub
        self.rest = rest
        self.priv = priv
        self.address = address
        self.account_index = account_index
        self.api_key = public_key_hex(priv)
        self.sent_tx_count = 0
        self.last_send_ts: float = time.time()
        self.last_veto_ts: float = 0.0
        self.rate_limited_until: float = 0.0
        self._veto_log: list[str] = []

    def record_veto(self, reason: str) -> None:
        self.last_veto_ts = time.time()
        self._veto_log.append(f"{time.strftime('%H:%M:%S')} {reason}")
        if len(self._veto_log) > 40:
            self._veto_log = self._veto_log[-40:]

    def recent_vetoes(self, n: int = 20) -> list[str]:
        return self._veto_log[-n:]

    def _sent(self) -> None:
        self.sent_tx_count += 1
        self.last_send_ts = time.time()

    def _tick(self, market: Market, price: float) -> str:
        return str(market.tick_at(price))

    def _handle_rpc(self, market: Market, action: str, resp: dict) -> dict:
        status = int(resp.get("status") or 0)
        err = resp.get("error")
        result = resp.get("result") or {}
        if status == 429:
            self.rate_limited_until = time.time() + 2.0
        if status >= 400:
            detail = err if err else result
            name = ""
            if isinstance(detail, dict):
                name = str(detail.get("type") or detail.get("rejectionReason") or "")
            activity.err("RESP", market.symbol, f"{action} status={status} {detail}")
            raise ExecutionError(name or "Rejected", f"{action} status={status}: {detail}", status=status)
        # 202 ACK and 200-with-status are acknowledgements only.
        extra = ""
        if isinstance(result, dict):
            extra = f" ack={result.get('status', 'ACK')} oid={result.get('orderId', '-')}"
        activity.ok("RESP", market.symbol, f"{action} status={status}{extra} (await WS confirm)")
        return result if isinstance(result, dict) else {}

    async def _post(self, method: str, body: dict, signature: str, ts: int, market: Market, action: str) -> dict:
        wait = self.rate_limited_until - time.time()
        if wait > 0:
            import asyncio

            await asyncio.sleep(wait)
        try:
            resp = await self.hub.post(
                method, body, api_key=self.api_key, timestamp_ns=ts, signature=signature
            )
        except Exception as exc:
            raise ExecutionError("Transport", f"{action}: {exc}") from exc
        self._sent()
        return self._handle_rpc(market, action, resp)

    def _place_body(
        self,
        market: Market,
        side: str,
        price: float,
        size: float,
        tif: str,
        ts: int,
        gtt: int,
        *,
        client_id: str | None,
        reduce_only: bool,
        order_type: str,
        signature: str,
    ) -> dict[str, Any]:
        tick = market.tick_at(price)
        px = snap(D(price), tick, "nearest")
        qty = snap(D(size), market.step_size, "down")
        body: dict[str, Any] = {
            "address": self.address,
            "accountIndex": self.account_index,
            "marketId": market.market_id,
            "orderSide": side.upper(),
            "orderType": order_type,
            "timeInForce": tif.upper(),
            "goodTilTime": str(gtt),
            "quantity": fmt_decimal(qty),
            "price": fmt_decimal(px),
            "reduceOnly": bool(reduce_only),
            "timestamp": ts,
            "signature": signature,
        }
        if client_id:
            body["clientId"] = client_id.lower()
        return body

    async def place_alo(
        self,
        market: Market,
        is_ask: bool,
        price: float,
        size_base: float,
        reduce_only: bool = False,
    ) -> str:
        """Post-only LIMIT+ALO (the ONLY quote type). Returns client_id."""
        side = "SELL" if is_ask else "BUY"
        cid = unique_client_id(market.market_id, side)
        ts = now_ns()
        gtt = good_til_us(GOOD_TIL_DAYS)
        tick = market.tick_at(price)
        px = snap(D(price), tick, "up" if is_ask else "down")
        qty = snap(D(size_base), market.step_size, "down")
        payload = typed_place_payload(
            address=self.address,
            account_index=self.account_index,
            market_id=market.market_id,
            side=side,
            price=px,
            quantity=qty,
            tick=tick,
            step=market.step_size,
            tif="ALO",
            timestamp_ns=ts,
            good_til_us_value=gtt,
            reduce_only=reduce_only,
            client_id=cid,
        )
        sig = sign_scheme1(self.priv, payload)
        body = self._place_body(
            market, side, float(px), float(qty), "ALO", ts, gtt,
            client_id=cid, reduce_only=reduce_only, order_type="LIMIT", signature=sig,
        )
        activity.ok(
            "SEND", market.symbol,
            f"create ALO {side} px={px} sz={qty} cid={cid}",
        )
        await self._post("placeOrder", body, sig, ts, market, f"create {side}")
        return cid

    async def place_market_ioc(
        self,
        market: Market,
        is_ask: bool,
        size_base: float,
        worst_price: float,
        reduce_only: bool = True,
    ) -> str:
        """IOC MARKET flatten. worst_price is the protective 10%-of-mark bound."""
        side = "SELL" if is_ask else "BUY"
        cid = unique_client_id(market.market_id, side)
        mark = market.mark_price
        if mark:
            lo, hi = mark * (1 - MAX_MARKET_SLIPPAGE), mark * (1 + MAX_MARKET_SLIPPAGE)
            worst_price = min(max(worst_price, lo), hi)
        ts = now_ns()
        gtt = good_til_us(GOOD_TIL_DAYS)
        tick = market.tick_at(worst_price)
        px = snap(D(worst_price), tick, "nearest")
        qty = snap(D(size_base), market.step_size, "down")
        payload = typed_place_payload(
            address=self.address,
            account_index=self.account_index,
            market_id=market.market_id,
            side=side,
            price=px,
            quantity=qty,
            tick=tick,
            step=market.step_size,
            tif="IOC",
            timestamp_ns=ts,
            good_til_us_value=gtt,
            reduce_only=reduce_only,
            client_id=cid,
        )
        sig = sign_scheme1(self.priv, payload)
        body = self._place_body(
            market, side, float(px), float(qty), "IOC", ts, gtt,
            client_id=cid, reduce_only=reduce_only, order_type="MARKET", signature=sig,
        )
        activity.ok(
            "SEND", market.symbol,
            f"market IOC {side} sz={qty} worst_px={px} cid={cid}",
        )
        await self._post("placeOrder", body, sig, ts, market, f"market {side}")
        return cid

    async def cancel(self, market: Market, *, client_id: str | None = None, order_id: str | None = None) -> None:
        ts = now_ns()
        payload = typed_cancel_payload(
            address=self.address,
            account_index=self.account_index,
            market_id=market.market_id,
            timestamp_ns=ts,
            order_id=order_id,
            client_id=client_id,
        )
        sig = sign_scheme1(self.priv, payload)
        body: dict[str, Any] = {
            "address": self.address,
            "accountIndex": self.account_index,
            "marketId": market.market_id,
            "timestamp": ts,
        }
        if client_id:
            body["kind"] = "clientId"
            body["clientId"] = client_id.lower()
        else:
            body["kind"] = "orderId"
            body["orderId"] = order_id
        activity.ok("SEND", market.symbol, f"cancel cid={client_id or '-'} oid={order_id or '-'}")
        await self._post("cancelOrder", body, sig, ts, market, "cancel")

    async def cancel_all(self, market_id: int | None = None) -> None:
        """REST kill-switch. Arcus has no cancel-on-disconnect."""
        self._sent()
        try:
            await self.rest.cancel_all(market_id)
        except RestError as exc:
            activity.err("SEND", "", f"cancel_all failed: {exc}")
            raise ExecutionError("CancelAll", str(exc), status=exc.status) from exc
        scope = f"market {market_id}" if market_id is not None else "ALL markets"
        activity.ok("SEND", "", f"cancel_all {scope}")

    async def set_leverage(self, market: Market, leverage: int) -> None:
        if market.market_type != "perp":
            return
        self._sent()
        try:
            await self.rest.set_leverage(market.market_id, leverage)
        except RestError as exc:
            raise ExecutionError("SetLeverage", str(exc), status=exc.status) from exc
        activity.ok("SEND", market.symbol, f"leverage set to {leverage}x (cross)")

    async def replace_quotes_batch(
        self,
        market: Market,
        cancels: list[str],
        creates: list[tuple[bool, float, float, bool]],
    ) -> list[str]:
        """Cancel by clientId + place ALO quotes. Batches of ≤39 are IP-free."""
        if len(cancels) + len(creates) > MAX_BATCH_ORDERS:
            raise ExecutionError("BatchTooLarge", f"batch {len(cancels)+len(creates)} > {MAX_BATCH_ORDERS}")
        ts = now_ns()
        gtt = good_til_us(GOOD_TIL_DAYS)
        cids: list[str] = []

        if cancels:
            elements = []
            header_sig = ""
            for cid in cancels:
                payload = typed_cancel_payload(
                    address=self.address,
                    account_index=self.account_index,
                    market_id=market.market_id,
                    timestamp_ns=ts,
                    client_id=cid,
                )
                sig = sign_scheme1(self.priv, payload)
                header_sig = sig
                elements.append(
                    {
                        "address": self.address,
                        "accountIndex": self.account_index,
                        "marketId": market.market_id,
                        "clientId": cid.lower(),
                        "signature": sig,
                    }
                )
            activity.ok("SEND", market.symbol, f"batchCancel n={len(elements)}")
            await self._post("batchCancelOrders", {"cancels": elements}, header_sig, ts, market, "batchCancel")

        if creates:
            ts = now_ns()
            elements = []
            header_sig = ""
            for is_ask, price, size_base, reduce_only in creates:
                side = "SELL" if is_ask else "BUY"
                cid = unique_client_id(market.market_id, side)
                tick = market.tick_at(price)
                px = snap(D(price), tick, "up" if is_ask else "down")
                qty = snap(D(size_base), market.step_size, "down")
                payload = typed_place_payload(
                    address=self.address,
                    account_index=self.account_index,
                    market_id=market.market_id,
                    side=side,
                    price=px,
                    quantity=qty,
                    tick=tick,
                    step=market.step_size,
                    tif="ALO",
                    timestamp_ns=ts,
                    good_til_us_value=gtt,
                    reduce_only=reduce_only,
                    client_id=cid,
                )
                sig = sign_scheme1(self.priv, payload)
                header_sig = sig
                elements.append(
                    self._place_body(
                        market, side, float(px), float(qty), "ALO", ts, gtt,
                        client_id=cid, reduce_only=reduce_only, order_type="LIMIT", signature=sig,
                    )
                )
                cids.append(cid)
            activity.ok("SEND", market.symbol, f"batchPlace ALO n={len(elements)}")
            await self._post("batchPlaceOrders", {"orders": elements}, header_sig, ts, market, "batchPlace")
        return cids
