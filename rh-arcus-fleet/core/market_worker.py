"""Per-market worker: Avellaneda-Stoikov inventory skew + maker-maker loop.

Strategy that worked on Lighter, applied here from day one:
  * A-S reservation skew — never taker-flatten as the normal exit
  * join-don't-improve at the touch
  * contrarian gate (skip the side pressed by momentum + imbalance)
  * vol circuit breaker, min-edge rule
  * maker-maker round trips
  * dust positions get a top-up before close (MARKET min sizes)
  * trade side from ask/bid account ids / maker-taker addresses, never a bare side
  * send ACK ≠ executed — order state comes from the account WS stream
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .config import MIN_QUOTE_USDG
from .execution import REJECT_HINTS, ExecutionEngine, ExecutionError
from .fills import derive_fill_side
from .logging_utils import activity
from .market_registry import Market
from .ws_hub import WsHub


@dataclass
class OrderState:
    client_id: str
    order_id: str | None = None
    price: float = 0.0
    size: float = 0.0
    status: str = "pending"  # pending → open → filled/canceled/rejected
    is_ask: bool = False
    placed_ts: float = field(default_factory=time.time)


@dataclass
class Position:
    size: float = 0.0
    avg_entry: float = 0.0
    unrealized_pnl: float = 0.0


@dataclass
class Fill:
    ts: float
    side: str
    size: float
    price: float
    notional: float
    role: str


class MarketWorker:
    def __init__(
        self,
        market: Market,
        hub: WsHub,
        execution: ExecutionEngine | None,
        *,
        spread_bps: float,
        requote_bps: float,
        order_size_usd: float,
        leverage: int,
        refresh_ms: int,
        max_hold_s: float = 300.0,
        adverse_stop_bps: float = 10.0,
        market_loss_usd: float = 10.0,
        vol_mult: float = 0.5,
        min_edge_bps: float = 0.5,
        vol_breaker_bps: float = 2.5,
        inventory_mult: float = 3.0,
        quote_rwa_off_hours: bool = False,
    ) -> None:
        self.market = market
        self.hub = hub
        self.exec = execution
        self.spread_bps = spread_bps
        self.requote_bps = requote_bps
        self.order_size_usd = order_size_usd
        self.leverage = leverage
        self.refresh_ms = refresh_ms
        self.max_hold_s = max_hold_s
        self.adverse_stop_bps = adverse_stop_bps
        self.market_loss_usd = market_loss_usd
        self.vol_mult = vol_mult
        self.min_edge_bps = min_edge_bps
        self.vol_breaker_bps = vol_breaker_bps
        self.inventory_mult = inventory_mult
        self.quote_rwa_off_hours = quote_rwa_off_hours

        self.enabled = True
        self.orders: dict[str, OrderState | None] = {"bid": None, "ask": None}
        self.position = Position()
        self.fills: list[Fill] = []
        self.fill_count = 0
        self.volume_usd = 0.0
        self.realized_pnl = 0.0
        self.last_fill_ts: float = time.time()
        self.last_tick_ts: float = 0.0
        self.consecutive_errors = 0
        self._extra_spread_ticks = 0
        self._unmatched_orders: dict[str, dict] = {}
        self._pos_opened_ts: float | None = None
        self._last_mid: float | None = None
        self._vol_bps: float = 0.0
        self._mom_bps: float = 0.0
        self._last_topup_ts: float = 0.0
        self._self_cancelled: set[str] = set()
        self.our_address: str | None = None
        self.account_index: int | None = None
        self._seen_trade_ids: set[str] = set()

    @property
    def book(self):
        return self.hub.book_for(self.market.symbol, self.market.market_id)

    @property
    def mid(self) -> float | None:
        book = self.book
        if book and book.mid is not None:
            return book.mid
        stats = self.hub.market_stats.get(self.market.market_id, {})
        v = stats.get("markPrice") or stats.get("oraclePrice")
        return float(v) if v else None

    @property
    def mark_price(self) -> float | None:
        return self.hub.mark_price(self.market.market_id) or self.market.mark_price

    def status_row(self) -> dict:
        bid = self.orders["bid"]
        ask = self.orders["ask"]
        return {
            "symbol": self.market.symbol,
            "market_id": self.market.market_id,
            "type": self.market.market_type,
            "group": self.market.group,
            "enabled": self.enabled,
            "mid": self.mid,
            "our_bid": bid.price if bid and bid.status in ("pending", "open") else None,
            "our_ask": ask.price if ask and ask.status in ("pending", "open") else None,
            "position": self.position.size,
            "fills": self.fill_count,
            "volume_usd": round(self.volume_usd, 2),
            "pnl": round(self.realized_pnl + self.position.unrealized_pnl, 4),
            "cost_per_million": (
                round(-(self.realized_pnl + self.position.unrealized_pnl) / self.volume_usd * 1_000_000, 2)
                if self.volume_usd >= 50
                else None
            ),
            "book_age_ms": None if not self.book else min(self.book.age_ms, 10**9),
        }

    def _record_veto(self, reason: str) -> None:
        if self.exec is not None:
            self.exec.record_veto(reason)

    async def tick(self, fleet_paused: bool) -> None:
        m = self.market
        self.last_tick_ts = time.time()
        mid = self.mid
        book = self.book

        if not self.enabled or self.exec is None:
            return
        if mid is None or book is None or not book.ready or book.age_ms > 2000:
            self._record_veto(
                f"{m.symbol}: stale book (age={None if not book else int(min(book.age_ms, 10**9))}ms) — not quoting blind"
            )
            return
        if m.status.upper() not in ("ONLINE", "ACTIVE"):
            self._record_veto(f"{m.symbol}: market {m.status} — not quoting")
            return
        if m.is_rwa and m.is_outside_rth and not self.quote_rwa_off_hours:
            self._record_veto(f"{m.symbol}: RWA off-hours — not quoting")
            await self.cancel_both_sides()
            return

        if self.realized_pnl < -abs(self.market_loss_usd):
            self.enabled = False
            activity.err(
                "RISK", m.symbol,
                f"realized loss ${-self.realized_pnl:.2f} > ${self.market_loss_usd:.2f} limit — market disabled",
            )
            await self.cancel_both_sides()
            await self.instant_close_ioc()
            return

        if self._last_mid:
            ret_bps = (mid - self._last_mid) / self._last_mid * 10_000
            self._vol_bps = 0.8 * self._vol_bps + 0.2 * abs(ret_bps)
            self._mom_bps = 0.5 * self._mom_bps + 0.5 * ret_bps
        self._last_mid = mid

        pos = self.position.size
        if abs(pos) >= m.min_size_step:
            now = time.time()
            if self._pos_opened_ts is None:
                self._pos_opened_ts = now
            entry = self.position.avg_entry or mid
            adverse_bps = ((entry - mid) if pos > 0 else (mid - entry)) / entry * 10_000
            held_s = now - self._pos_opened_ts
            if held_s > self.max_hold_s or adverse_bps > self.adverse_stop_bps:
                reason = "max hold" if held_s > self.max_hold_s else f"adverse {adverse_bps:.1f}bps"
                activity.warn("CLOSE", m.symbol, f"inventory backstop ({reason}) — taker flatten")
                await self.cancel_both_sides()
                await self._taker_flatten()
                return
        else:
            self._pos_opened_ts = None

        if fleet_paused:
            return

        min_tick = float(m.tick_at(mid))
        bb, ba = book.best_bid, book.best_ask

        if self._vol_bps > max(self.vol_breaker_bps, 3 * self.spread_bps):
            self._record_veto(f"{m.symbol}: vol {self._vol_bps:.2f}bps/tick > breaker — quotes pulled")
            await self.cancel_both_sides()
            return
        if bb and ba:
            book_spread_bps = (ba - bb) / mid * 10_000
            if book_spread_bps < self.min_edge_bps:
                self._record_veto(
                    f"{m.symbol}: book spread {book_spread_bps:.2f}bps < min edge {self.min_edge_bps}bps — not quoting"
                )
                await self.cancel_both_sides()
                return

        if self._extra_spread_ticks:
            self._extra_spread_ticks -= 1
        extra = self._extra_spread_ticks * min_tick
        half_bps = max(self.spread_bps, min(self._vol_bps * self.vol_mult, self.spread_bps * 25))
        half_spread = max(mid * (half_bps / 10_000), min_tick) + extra
        base_size = self.order_size_usd / mid

        anchor = mid
        imbalance = 0.0
        if bb and ba:
            bid_sz = book.bids.size_at(bb)
            ask_sz = book.asks.size_at(ba)
            if bid_sz > 0 and ask_sz > 0:
                anchor = (bb * ask_sz + ba * bid_sz) / (bid_sz + ask_sz)
                imbalance = (bid_sz - ask_sz) / (bid_sz + ask_sz)

        max_inv_base = max(base_size * self.inventory_mult, m.min_size_step)
        q_ratio = max(-1.0, min(1.0, pos / max_inv_base))
        reservation = anchor - q_ratio * half_spread
        bid_px = reservation - half_spread
        ask_px = reservation + half_spread

        bid_size = base_size * (1 - max(q_ratio, 0.0)) + max(-pos, 0.0)
        ask_size = base_size * (1 - max(-q_ratio, 0.0)) + max(pos, 0.0)

        # JOIN, never improve.
        if bb:
            bid_px = min(bid_px, bb)
        if ba:
            ask_px = max(ask_px, ba)

        mark = self.mark_price or mid
        best_ask = book.best_ask or mark
        best_bid = book.best_bid or mark
        if m.upper_bound:
            ask_px = min(ask_px, m.upper_bound)
            bid_px = min(bid_px, m.upper_bound)
        if m.lower_bound:
            bid_px = max(bid_px, m.lower_bound)
            ask_px = max(ask_px, m.lower_bound)
        bid_px = min(bid_px, mark * 1.05, best_ask * 1.05 - min_tick)
        ask_px = max(ask_px, mark * 0.95, best_bid * 0.95 + min_tick)

        # ALO must not cross the live book.
        bid_px = min(bid_px, best_ask - min_tick)
        ask_px = max(ask_px, best_bid + min_tick)

        ask_order = self.orders["ask"]
        bid_order = self.orders["bid"]
        if ask_order and ask_order.price:
            bid_px = min(bid_px, ask_order.price - min_tick)
        if bid_order and bid_order.price:
            ask_px = max(ask_px, bid_order.price + min_tick)

        bid_px = m.round_price(bid_px, "bid")
        ask_px = m.round_price(ask_px, "ask")

        gate = 0.5
        strong_mom = max(0.8, 2 * self.spread_bps)
        skip_bid = self._mom_bps < -strong_mom or (self._mom_bps < -0.3 and imbalance < -gate)
        skip_ask = self._mom_bps > strong_mom or (self._mom_bps > 0.3 and imbalance > gate)

        min_quote = max(m.min_quote_amount, MIN_QUOTE_USDG)

        def _clamp_size(px: float, size: float) -> float:
            if px <= 0:
                return 0.0
            if px * size < min_quote:
                size = min_quote / px * 1.01
            return m.round_size(max(size, m.min_base_amount))

        at_limit_long = q_ratio >= 1.0
        at_limit_short = q_ratio <= -1.0
        if at_limit_long:
            ask_size = abs(pos)
        if at_limit_short:
            bid_size = abs(pos)

        activity.ok(
            "BOOK", m.symbol,
            f"bid={best_bid} ask={best_ask} mid={mid:.6g} "
            f"spread={(best_ask - best_bid) / mid * 10_000:.2f}bps "
            f"q={q_ratio:+.2f} imb={imbalance:+.2f} mom={self._mom_bps:+.2f}bps",
        )

        if skip_bid or at_limit_long:
            if skip_bid:
                self._record_veto(f"{m.symbol}: bid gated (mom {self._mom_bps:+.2f}, imb {imbalance:+.2f})")
            await self._cancel_side("bid")
        else:
            await self.evaluate_side("bid", bid_px, _clamp_size(bid_px, bid_size), reduce_only=at_limit_short)
        if skip_ask or at_limit_short:
            if skip_ask:
                self._record_veto(f"{m.symbol}: ask gated (mom {self._mom_bps:+.2f}, imb {imbalance:+.2f})")
            await self._cancel_side("ask")
        else:
            await self.evaluate_side("ask", ask_px, _clamp_size(ask_px, ask_size), reduce_only=at_limit_long)

    def _mark_self_cancelled(self, order: OrderState | None) -> None:
        if order is not None:
            self._self_cancelled.add(order.client_id)
            while len(self._self_cancelled) > 128:
                self._self_cancelled.pop()

    async def _cancel_side(self, side: str) -> None:
        order = self.orders[side]
        if order is not None and order.status in ("pending", "open") and self.exec:
            try:
                await self.exec.cancel(self.market, client_id=order.client_id, order_id=order.order_id)
                self._mark_self_cancelled(order)
            except ExecutionError:
                pass
        self.orders[side] = None

    async def evaluate_side(self, side: str, desired_px: float, size_base: float, reduce_only: bool = False) -> None:
        if self.exec is None or size_base <= 0:
            return
        m = self.market
        existing = self.orders[side]
        if existing and existing.status in ("pending", "open"):
            drift_bps = abs(existing.price - desired_px) / desired_px * 10_000 if desired_px else 0
            if drift_bps < self.requote_bps:
                return
            try:
                await self.exec.cancel(m, client_id=existing.client_id, order_id=existing.order_id)
                self._mark_self_cancelled(existing)
                activity.ok(
                    "ORDER", m.symbol,
                    f"{side} requote {existing.price}→{desired_px} (drift {drift_bps:.2f}bps)",
                )
            except ExecutionError as exc:
                self._on_exec_error(side, exc)
            self.orders[side] = None
            return

        try:
            cid = await self.exec.place_alo(
                m, is_ask=(side == "ask"), price=desired_px, size_base=size_base, reduce_only=reduce_only
            )
        except ExecutionError as exc:
            self._on_exec_error(side, exc)
            return
        self.register_order(
            side,
            OrderState(client_id=cid, price=desired_px, size=size_base, is_ask=(side == "ask")),
        )
        self.consecutive_errors = 0

    def _on_exec_error(self, side: str, exc: ExecutionError) -> None:
        self.consecutive_errors += 1
        activity.err("ORDER", self.market.symbol, f"{side} {exc.name}: {exc}")
        self._record_veto(f"{self.market.symbol} {side}: {exc.name}")
        name = (exc.name or "").upper()
        if name in ("POST_ONLY_WOULD_CROSS", "FAT_FINGER", "PRICE_TOO_FAR"):
            self._extra_spread_ticks = min(self._extra_spread_ticks + 2, 50)
        elif "MARGIN" in name:
            self.order_size_usd = max(MIN_QUOTE_USDG, self.order_size_usd * 0.5)
            if self.order_size_usd <= MIN_QUOTE_USDG and self.consecutive_errors > 5:
                self.enabled = False
                activity.err("RISK", self.market.symbol, "margin errors persist — market disabled")

    async def _taker_flatten(self) -> None:
        """Close with a taker IOC. Dust is topped UP first — MARKET orders
        enforce min size too and otherwise silent-cancel."""
        m = self.market
        pos = self.position.size
        mid = self.mid
        if self.exec is None or mid is None or abs(pos) < m.min_size_step:
            return
        is_long = pos > 0
        min_limit_size = max(m.min_base_amount, max(m.min_quote_amount, MIN_QUOTE_USDG) / mid)
        if abs(pos) < min_limit_size:
            if time.time() - self._last_topup_ts < 60:
                return
            self._last_topup_ts = time.time()
            topup = max(m.min_base_amount, max(m.min_quote_amount, MIN_QUOTE_USDG) / mid * 1.02)
            topup = m.round_size(topup)
            activity.warn(
                "CLOSE", m.symbol,
                f"dust {pos:+.6g} below venue minimum {min_limit_size:.6g} — "
                f"topping up {topup:.6g} to make it closeable",
            )
            worst = mid * (1.02 if is_long else 0.98)
            try:
                await self.exec.place_market_ioc(
                    m,
                    is_ask=not is_long,
                    size_base=topup,
                    worst_price=m.round_price(worst),
                    reduce_only=False,
                )
            except ExecutionError as exc:
                activity.err("CLOSE", m.symbol, f"dust top-up failed: {exc.name}: {exc}")
            return
        await self.instant_close_ioc()

    async def cancel_both_sides(self) -> None:
        for side in ("bid", "ask"):
            order = self.orders[side]
            if order and order.status in ("pending", "open") and self.exec:
                try:
                    await self.exec.cancel(self.market, client_id=order.client_id, order_id=order.order_id)
                    self._mark_self_cancelled(order)
                except ExecutionError as exc:
                    activity.warn("ORDER", self.market.symbol, f"cancel {side} failed: {exc}")
            self.orders[side] = None

    async def instant_close_ioc(self) -> None:
        pos = self.position.size
        if self.exec is None or abs(pos) < self.market.min_size_step:
            return
        mid = self.mid or self.mark_price
        if mid is None:
            activity.warn("CLOSE", self.market.symbol, "no price available — cannot flatten yet")
            return
        is_ask = pos > 0
        worst = mid * (0.98 if is_ask else 1.02)
        activity.ok("CLOSE", self.market.symbol, f"position={pos:+.6g} → IOC flatten initiated")
        try:
            await self.exec.place_market_ioc(
                self.market,
                is_ask=is_ask,
                size_base=abs(pos),
                worst_price=self.market.round_price(worst),
                reduce_only=True,
            )
        except ExecutionError as exc:
            activity.err("CLOSE", self.market.symbol, f"flatten failed: {exc.name}: {exc}")

    def register_order(self, side: str, state: OrderState) -> None:
        self.orders[side] = state
        buffered = self._unmatched_orders.pop(state.client_id, None)
        if buffered is not None:
            self.on_order_event(buffered)

    def on_order_event(self, order: dict) -> None:
        """Order update from the `orders` channel — the source of truth."""
        cid = str(order.get("clientId") or order.get("client_id") or "").lower()
        oid = order.get("orderId") or order.get("order_id")
        status = str(order.get("status") or order.get("state") or "").lower()
        if status in ("cancelled", "margin_canceled"):
            status = "canceled" if status == "cancelled" else "canceled-margin"
        matched = False
        for side in ("bid", "ask"):
            state = self.orders[side]
            if state is None:
                continue
            if cid and state.client_id == cid:
                matched = True
                if oid:
                    state.order_id = str(oid)
                prev = state.status
                state.status = status or prev
                if status == "open" and prev != "open":
                    remaining = order.get("remainingSize", state.size)
                    activity.ok(
                        "ORDER", self.market.symbol,
                        f"{side} cid={cid} status=open remaining={remaining}",
                    )
                elif status.startswith("canceled") or status == "rejected":
                    reason = str(order.get("rejectionReason") or "")
                    hint = REJECT_HINTS.get(reason.upper(), "")
                    activity.err(
                        "ORDER", self.market.symbol,
                        f"{side} {status}" + (f" {reason}" if reason else "") + (f" — {hint}" if hint else ""),
                    )
                    if reason.upper() == "POST_ONLY_WOULD_CROSS":
                        self._extra_spread_ticks = min(self._extra_spread_ticks + 2, 50)
                    self.orders[side] = None
                elif status == "filled":
                    self.orders[side] = None
        if not matched and cid:
            if cid in self._self_cancelled:
                self._self_cancelled.discard(cid)
                return
            if status.startswith("canceled") or status == "rejected":
                reason = str(order.get("rejectionReason") or "")
                hint = REJECT_HINTS.get(reason.upper(), "")
                activity.err(
                    "ORDER", self.market.symbol,
                    f"untracked order cid={cid} {status}" + (f" — {hint or reason}" if (hint or reason) else ""),
                )
            self._unmatched_orders[cid] = order
            while len(self._unmatched_orders) > 64:
                self._unmatched_orders.pop(next(iter(self._unmatched_orders)))

    def on_trade_event(self, trade: dict) -> None:
        """Fill from userFills / public trade. Side is DERIVED, never assumed."""
        tid = str(trade.get("tradeId") or trade.get("trade_id") or "")
        if tid:
            if tid in self._seen_trade_ids:
                return
            self._seen_trade_ids.add(tid)
            if len(self._seen_trade_ids) > 400:
                self._seen_trade_ids = set(list(self._seen_trade_ids)[-200:])

        price = _f(trade.get("fillPrice") or trade.get("price"))
        size = _f(trade.get("fillSize") or trade.get("size") or trade.get("quantity"))
        if price is None or size is None or size <= 0:
            return

        resting_side = None
        oid = str(trade.get("orderId") or "")
        cid = str(trade.get("clientId") or "").lower()
        for slot in self.orders.values():
            if slot and ((cid and slot.client_id == cid) or (oid and slot.order_id == oid)):
                resting_side = "SELL" if slot.is_ask else "BUY"
                break

        derived = derive_fill_side(
            trade,
            our_address=self.our_address,
            our_account_index=self.account_index,
            our_order_id=oid or None,
            our_client_id=cid or None,
            our_resting_side=resting_side,
        )
        if derived is None:
            # Account-scoped userFills with no addresses: last resort is the
            # resting order we already matched. Still refuse a bare side.
            if resting_side is None:
                return
            is_ask = resting_side == "SELL"
            role = "maker"
        else:
            is_ask = derived.side == "SELL"
            role = derived.role

        notional = price * size
        signed = -size if is_ask else size
        pos = self.position
        if pos.size != 0 and (pos.size > 0) != (signed > 0):
            closed = min(abs(signed), abs(pos.size))
            direction = 1 if pos.size > 0 else -1
            self.realized_pnl += (price - pos.avg_entry) * closed * direction
        new_size = pos.size + signed
        if pos.size == 0 or (pos.size > 0) == (signed > 0):
            total = abs(pos.size) + abs(signed)
            if total > 0:
                pos.avg_entry = (pos.avg_entry * abs(pos.size) + price * abs(signed)) / total
        elif abs(signed) > abs(pos.size):
            pos.avg_entry = price
        pos.size = new_size
        if abs(new_size) < self.market.min_size_step:
            self._pos_opened_ts = None
        elif self._pos_opened_ts is None:
            self._pos_opened_ts = time.time()

        self.fill_count += 1
        self.volume_usd += notional
        self.last_fill_ts = time.time()
        fill = Fill(
            ts=time.time(),
            side="SELL" if is_ask else "BUY",
            size=size,
            price=price,
            notional=notional,
            role=role,
        )
        self.fills.append(fill)
        if len(self.fills) > 100:
            self.fills = self.fills[-100:]
        activity.ok(
            "FILL", self.market.symbol,
            f"{fill.side} {size:.6g} @{price} notional=${notional:.2f} {fill.role}",
        )

    def on_position_event(self, position: dict) -> None:
        size = _f(position.get("size") or position.get("position") or position.get("positionSize"))
        side = str(position.get("side") or position.get("positionSide") or "").upper()
        if size is not None:
            if side in ("SHORT", "SELL") and size > 0:
                size = -size
            sign = position.get("sign")
            if sign is not None and int(sign) < 0:
                size = -abs(size)
            self.position.size = size
            if abs(size) < self.market.min_size_step:
                self._pos_opened_ts = None
            elif self._pos_opened_ts is None:
                self._pos_opened_ts = time.time()
        entry = _f(position.get("entryPrice") or position.get("avgEntryPrice") or position.get("avg_entry_price"))
        if entry is not None:
            self.position.avg_entry = entry
        upnl = _f(position.get("unrealizedPnl") or position.get("unrealized_pnl"))
        if upnl is not None:
            self.position.unrealized_pnl = upnl


def _f(v) -> float | None:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None
