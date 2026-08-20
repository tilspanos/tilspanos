"""Per-market worker: state + the Breads adaptive post-only MM tick loop.

One strategy, no ensembles: quote mid ± spread with POST_ONLY on both sides,
requote on drift, never MM while holding a position (flatten first with a
reduce-only IOC), clamp prices inside the official fat-finger bounds.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .config import MIN_QUOTE_USDG
from .execution import CANCEL_REASONS, ExecutionEngine, ExecutionError
from .logging_utils import activity
from .market_registry import Market
from .ws_hub import WsHub


@dataclass
class OrderState:
    client_order_index: int
    order_index: int | None = None  # assigned by the venue, learned via WS
    price: float = 0.0
    size: float = 0.0
    status: str = "pending"  # pending → open → filled/canceled-*
    is_ask: bool = False
    placed_ts: float = field(default_factory=time.time)


@dataclass
class Position:
    size: float = 0.0  # signed base amount
    avg_entry: float = 0.0
    unrealized_pnl: float = 0.0


@dataclass
class Fill:
    ts: float
    side: str
    size: float
    price: float
    notional: float
    role: str  # maker | taker


class MarketWorker:
    def __init__(
        self,
        market: Market,
        hub: WsHub,
        execution: ExecutionEngine,
        *,
        spread_bps: float,
        requote_bps: float,
        order_size_usd: float,
        leverage: int,
        refresh_ms: int,
    ) -> None:
        self.market = market
        self.hub = hub
        self.exec = execution
        self.spread_bps = spread_bps
        self.requote_bps = requote_bps
        self.order_size_usd = order_size_usd
        self.leverage = leverage
        self.refresh_ms = refresh_ms

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
        # extra safety margin applied after fat-finger / post-only rejections
        self._extra_spread_ticks = 0
        # WS order events that arrived before the REST response registered the
        # order (the stream can outrun sendTx confirmation) — replayed on register
        self._unmatched_orders: dict[int, dict] = {}

    # ── derived market state ─────────────────────────────────────────────────
    @property
    def book(self):
        return self.hub.books.get(self.market.market_id)

    @property
    def mid(self) -> float | None:
        book = self.book
        if book and book.mid is not None:
            return book.mid
        stats = self.hub.stats_for(self.market.market_id)
        v = stats.get("mid_price")
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
            "book_age_ms": None if not self.book else min(self.book.age_ms, 10**9),
        }

    # ── main tick (called every refresh_ms by the orchestrator) ─────────────
    async def tick(self, fleet_paused: bool) -> None:
        m = self.market
        self.last_tick_ts = time.time()
        mid = self.mid
        book = self.book

        if not self.enabled:
            return
        if mid is None or book is None or book.age_ms > 2000:
            self.exec.record_veto(f"{m.symbol}: stale book (age={None if not book else int(book.age_ms)}ms) — not quoting blind")
            return

        # RULE: never MM while holding a position — flatten first.
        if abs(self.position.size) >= m.min_size_step:
            await self.cancel_both_sides()
            await self.instant_close_ioc()
            return

        if fleet_paused:
            return

        min_tick = m.min_tick
        extra = self._extra_spread_ticks * min_tick
        half_spread = max(mid * (self.spread_bps / 10_000), min_tick) + extra
        size_base = self.order_size_usd / mid

        bid_px = mid - half_spread
        ask_px = mid + half_spread

        # Fat-finger safety (official bounds):
        #   bid ≤ min(mark, best_ask) × 1.05,  ask ≥ max(mark, best_bid) × 0.95
        mark = self.mark_price or mid
        best_ask = book.best_ask or mark
        best_bid = book.best_bid or mark
        bid_px = min(bid_px, mark * 1.05, best_ask * 1.05 - min_tick)
        ask_px = max(ask_px, mark * 0.95, best_bid * 0.95 + min_tick)

        # POST_ONLY must not cross the live book.
        bid_px = min(bid_px, best_ask - min_tick)
        ask_px = max(ask_px, best_bid + min_tick)

        # Don't cross our own resting orders.
        ask_order = self.orders["ask"]
        bid_order = self.orders["bid"]
        if ask_order and ask_order.price:
            bid_px = min(bid_px, ask_order.price - min_tick)
        if bid_order and bid_order.price:
            ask_px = max(ask_px, bid_order.price + min_tick)

        bid_px = m.round_price(bid_px)
        ask_px = m.round_price(ask_px)

        # Venue minimums: max(min_base_amount, $10 USDG min_quote_amount).
        min_quote = max(m.min_quote_amount, MIN_QUOTE_USDG)
        if bid_px * size_base < min_quote:
            size_base = min_quote / bid_px * 1.01
        size_base = max(size_base, m.min_base_amount)
        size_base = round(size_base, m.size_decimals)

        book_line = (
            f"bid={best_bid} ask={best_ask} mid={mid:.6g} "
            f"spread={(best_ask - best_bid) / mid * 10_000:.2f}bps "
            f"depth=${book.bids.depth_usd():.0f}/${book.asks.depth_usd():.0f}"
        )
        activity.ok("BOOK", m.symbol, book_line)

        await self.evaluate_side("bid", bid_px, size_base)
        await self.evaluate_side("ask", ask_px, size_base)

    async def evaluate_side(self, side: str, desired_px: float, size_base: float) -> None:
        m = self.market
        existing = self.orders[side]
        if existing and existing.status in ("pending", "open"):
            drift_bps = abs(existing.price - desired_px) / desired_px * 10_000
            if drift_bps < self.requote_bps:
                return  # resting order is fine — don't churn
            if existing.order_index is not None:
                try:
                    await self.exec.cancel(m, existing.order_index)
                    activity.ok(
                        "ORDER", m.symbol,
                        f"{side} requote {existing.price}→{desired_px} (drift {drift_bps:.2f}bps)",
                    )
                except ExecutionError as exc:
                    self._on_exec_error(side, exc)
                self.orders[side] = None
            # place fresh next tick — mirrors the Breads cancel-then-quote cadence
            return

        try:
            coi = await self.exec.place_post_only(
                m, is_ask=(side == "ask"), price=desired_px, size_base=size_base
            )
        except ExecutionError as exc:
            self._on_exec_error(side, exc)
            return
        self.register_order(
            side,
            OrderState(
                client_order_index=coi,
                price=desired_px,
                size=size_base,
                is_ask=(side == "ask"),
            ),
        )
        self.consecutive_errors = 0

    def _on_exec_error(self, side: str, exc: ExecutionError) -> None:
        m = self.market
        self.consecutive_errors += 1
        activity.err("ORDER", m.symbol, f"{side} {exc.name}({exc.code}): {exc}")
        self.exec.record_veto(f"{m.symbol} {side}: {exc.name}({exc.code})")
        if exc.code in (21733, 21734, 21705):
            # FatFinger / TooFarFromMark / PostOnlyWouldCross — widen and retry next tick
            self._extra_spread_ticks = min(self._extra_spread_ticks + 2, 50)
        elif exc.code == 21739:
            self.order_size_usd = max(MIN_QUOTE_USDG, self.order_size_usd * 0.5)
            if self.order_size_usd <= MIN_QUOTE_USDG and self.consecutive_errors > 5:
                self.enabled = False
                activity.err("RISK", m.symbol, "NotEnoughOrderMargin persists — market disabled")
        elif exc.code == 21718:
            activity.warn("ORDER", m.symbol, "MaxOrdersPerMarket — sweeping stale orders")

    # ── flatten ──────────────────────────────────────────────────────────────
    async def cancel_both_sides(self) -> None:
        for side in ("bid", "ask"):
            order = self.orders[side]
            if order and order.order_index is not None and order.status in ("pending", "open"):
                try:
                    await self.exec.cancel(self.market, order.order_index)
                except ExecutionError as exc:
                    activity.warn("ORDER", self.market.symbol, f"cancel {side} failed: {exc}")
            self.orders[side] = None

    async def instant_close_ioc(self) -> None:
        """Reduce-only IOC at a slippage-capped worst price (Breads flatten)."""
        pos = self.position.size
        if abs(pos) < self.market.min_size_step:
            return
        mid = self.mid or self.mark_price
        if mid is None:
            activity.warn("CLOSE", self.market.symbol, "no price available — cannot flatten yet")
            return
        is_ask = pos > 0  # long → sell
        worst = mid * (0.98 if is_ask else 1.02)  # 2% slippage cap
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

    # ── WS event ingestion (order/trade/position confirmations) ─────────────
    def register_order(self, side: str, state: OrderState) -> None:
        """Attach a just-placed order and replay any WS confirmation that
        raced ahead of the REST response."""
        self.orders[side] = state
        buffered = self._unmatched_orders.pop(state.client_order_index, None)
        if buffered is not None:
            self.on_order_event(buffered)

    def on_order_event(self, order: dict) -> None:
        """Order update from account_all_orders — the source of truth.
        sendTx code=200 is never trusted; THIS is what confirms an order."""
        coi = _i(order.get("client_order_index") or order.get("client_order_id"))
        oi = _i(order.get("order_index") or order.get("order_id"))
        status = str(order.get("status", "")).lower()
        matched = False
        for side in ("bid", "ask"):
            state = self.orders[side]
            if state is None:
                continue
            if coi is not None and state.client_order_index == coi:
                matched = True
                if oi is not None:
                    state.order_index = oi
                prev = state.status
                state.status = status or prev
                if status == "open" and prev != "open":
                    remaining = order.get("remaining_base_amount", state.size)
                    activity.ok(
                        "ORDER", self.market.symbol,
                        f"{side} client_id={coi} status=open remaining={remaining}",
                    )
                elif status.startswith("canceled"):
                    hint = CANCEL_REASONS.get(status, "")
                    activity.err(
                        "ORDER", self.market.symbol,
                        f"{side} {status}" + (f" — {hint}" if hint else ""),
                    )
                    if status in ("canceled-post-only", "canceled-fat-finger"):
                        self._extra_spread_ticks = min(self._extra_spread_ticks + 2, 50)
                    self.orders[side] = None
                elif status == "filled":
                    self.orders[side] = None
        if not matched and coi is not None:
            # Keep the event so register_order can replay it (bounded buffer).
            self._unmatched_orders[coi] = order
            while len(self._unmatched_orders) > 64:
                self._unmatched_orders.pop(next(iter(self._unmatched_orders)))

    def on_trade_event(self, trade: dict) -> None:
        """Fill from account_all_trades: update position, volume, spread PnL."""
        price = _f(trade.get("price"))
        size = _f(trade.get("size") or trade.get("filled_base_amount"))
        if price is None or size is None:
            return
        is_ask = bool(trade.get("is_ask")) if "is_ask" in trade else (
            str(trade.get("side", "")).lower() in ("sell", "ask")
        )
        is_maker = bool(trade.get("is_maker", trade.get("maker", True)))
        notional = price * size
        signed = -size if is_ask else size

        # Realized spread capture vs average entry when reducing/closing.
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
        elif abs(signed) > abs(pos.size):  # flipped through zero
            pos.avg_entry = price
        pos.size = new_size

        self.fill_count += 1
        self.volume_usd += notional
        self.last_fill_ts = time.time()
        fill = Fill(
            ts=time.time(),
            side="SELL" if is_ask else "BUY",
            size=size,
            price=price,
            notional=notional,
            role="maker" if is_maker else "taker",
        )
        self.fills.append(fill)
        if len(self.fills) > 100:
            self.fills = self.fills[-100:]
        activity.ok(
            "FILL", self.market.symbol,
            f"{fill.side} {size:.6g} @{price} notional=${notional:.2f} {fill.role}",
        )

    def on_position_event(self, position: dict) -> None:
        """Position snapshot/update from account_all (authoritative)."""
        size = _f(position.get("position") or position.get("size"))
        sign = position.get("sign")
        if size is not None:
            if sign is not None and int(sign) < 0:
                size = -abs(size)
            self.position.size = size
        entry = _f(position.get("avg_entry_price") or position.get("avg_entry"))
        if entry is not None:
            self.position.avg_entry = entry
        upnl = _f(position.get("unrealized_pnl"))
        if upnl is not None:
            self.position.unrealized_pnl = upnl


def _f(v) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _i(v) -> int | None:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None
