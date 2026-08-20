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
        max_hold_s: float = 120.0,
        adverse_stop_bps: float = 10.0,
        market_loss_usd: float = 10.0,
        vol_mult: float = 0.5,
        min_edge_bps: float = 0.5,
        vol_breaker_bps: float = 2.5,
        inventory_mult: float = 3.0,
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
        self.inventory_mult = inventory_mult  # max inventory as a multiple of order size

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
        # inventory + adverse-selection state
        self._pos_opened_ts: float | None = None
        self._last_mid: float | None = None
        self._vol_bps: float = 0.0  # EWMA of per-tick |mid move| in bps
        self._mom_bps: float = 0.0  # signed EWMA of per-tick mid moves (momentum)
        self._last_topup_ts: float = 0.0
        # cois of orders WE cancelled — their WS cancel confirmations are
        # expected and must not be reported as silent rejections
        self._self_cancelled: set[int] = set()
        # our venue account, used to derive trade side from ask/bid account ids
        self.account_index: int | None = None

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
            "cost_per_million": (
                round(-(self.realized_pnl + self.position.unrealized_pnl) / self.volume_usd * 1_000_000, 2)
                if self.volume_usd >= 50
                else None
            ),
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

        # Auto-disable a market that keeps losing: protect the fleet's PnL.
        if self.realized_pnl < -abs(self.market_loss_usd):
            self.enabled = False
            activity.err(
                "RISK", m.symbol,
                f"realized loss ${-self.realized_pnl:.2f} > ${self.market_loss_usd:.2f} limit — market disabled",
            )
            await self.cancel_both_sides()
            await self.instant_close_ioc()
            return

        # volatility + momentum estimates (EWMA of per-tick mid moves, in bps)
        if self._last_mid:
            ret_bps = (mid - self._last_mid) / self._last_mid * 10_000
            self._vol_bps = 0.8 * self._vol_bps + 0.2 * abs(ret_bps)
            self._mom_bps = 0.5 * self._mom_bps + 0.5 * ret_bps
        self._last_mid = mid

        # ── Inventory backstops (Avellaneda-Stoikov: skewed quotes unload
        # inventory passively; the taker path exists ONLY for emergencies) ──
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

        min_tick = m.min_tick
        bb, ba = book.best_bid, book.best_ask

        # ── Circuit breakers: the cheapest volume is the toxic fill you skip ──
        # 1. Vol breaker: when the market is running, pull quotes entirely
        #    instead of standing in front of it.
        if self._vol_bps > max(self.vol_breaker_bps, 3 * self.spread_bps):
            self.exec.record_veto(f"{m.symbol}: vol {self._vol_bps:.2f}bps/tick > breaker — quotes pulled")
            await self.cancel_both_sides()
            return
        # 2. Minimum edge: if the book spread is too tight to pay for the
        #    risk, there is no trade. Joining a locked book only buys losses.
        if bb and ba:
            book_spread_bps = (ba - bb) / mid * 10_000
            if book_spread_bps < self.min_edge_bps:
                self.exec.record_veto(
                    f"{m.symbol}: book spread {book_spread_bps:.2f}bps < min edge {self.min_edge_bps}bps — not quoting"
                )
                await self.cancel_both_sides()
                return

        # decay the post-only/fat-finger penalty once things are calm again
        if self._extra_spread_ticks:
            self._extra_spread_ticks -= 1
        extra = self._extra_spread_ticks * min_tick
        # widen with volatility: quoting tighter than the market moves per
        # tick is how MMs get run over (capped at 25x the base spread)
        half_bps = max(self.spread_bps, min(self._vol_bps * self.vol_mult, self.spread_bps * 25))
        half_spread = max(mid * (half_bps / 10_000), min_tick) + extra
        base_size = self.order_size_usd / mid

        # anchor on the microprice: top-of-book size imbalance predicts the
        # next move, so lean quotes toward the pressured side
        anchor = mid
        imbalance = 0.0  # +1 = all bid-side size (up pressure), -1 = all ask-side
        if bb and ba:
            bid_sz = book.bids.levels.get(bb, 0.0)
            ask_sz = book.asks.levels.get(ba, 0.0)
            if bid_sz > 0 and ask_sz > 0:
                anchor = (bb * ask_sz + ba * bid_sz) / (bid_sz + ask_sz)
                imbalance = (bid_sz - ask_sz) / (bid_sz + ask_sz)

        # ── Avellaneda-Stoikov reservation price: shift BOTH quotes by
        # inventory so incoming flow mean-reverts the position for free —
        # never pay taker to unload (r = S − q·γ·σ²·(T−t), implemented as a
        # half-spread-proportional skew at full inventory).
        max_inv_base = max(base_size * self.inventory_mult, m.min_size_step)
        q_ratio = max(-1.0, min(1.0, pos / max_inv_base))
        reservation = anchor - q_ratio * half_spread
        bid_px = reservation - half_spread
        ask_px = reservation + half_spread

        # sizes: shrink the side that grows inventory, enlarge the side that
        # unloads it (exit side carries the position on top of base size)
        bid_size = base_size * (1 - max(q_ratio, 0.0)) + max(-pos, 0.0)
        ask_size = base_size * (1 - max(-q_ratio, 0.0)) + max(pos, 0.0)

        # ── JOIN, never improve: quote AT the best level, not inside the
        # spread. Improving the book means being first in line for informed
        # flow (pure adverse selection); joining earns the FULL book spread
        # with far less toxicity.
        if bb:
            bid_px = min(bid_px, bb)
        if ba:
            ask_px = max(ask_px, ba)

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

        # ── Contrarian gate (arXiv 2502.18625, live BTC-perp experiment):
        # maker fills are most toxic when flow presses into the quote.
        # A STRONG trend alone gates the side being run over (selling into a
        # rally is how the 40bps-ramp losses happened); weaker momentum needs
        # book-imbalance confirmation.
        gate = 0.5
        strong_mom = max(0.8, 2 * self.spread_bps)
        skip_bid = self._mom_bps < -strong_mom or (self._mom_bps < -0.3 and imbalance < -gate)
        skip_ask = self._mom_bps > strong_mom or (self._mom_bps > 0.3 and imbalance > gate)

        # Venue minimums per side: max(min_base_amount, $10 USDG notional).
        min_quote = max(m.min_quote_amount, MIN_QUOTE_USDG)

        def _clamp_size(px: float, size: float) -> float:
            if px * size < min_quote:
                size = min_quote / px * 1.01
            return round(max(size, m.min_base_amount), m.size_decimals)

        # At the inventory limit, only the reducing side stays, sized exactly
        # to the position and flagged reduce-only — no risk may be added.
        at_limit_long = q_ratio >= 1.0
        at_limit_short = q_ratio <= -1.0
        if at_limit_long:
            ask_size = abs(pos)
        if at_limit_short:
            bid_size = abs(pos)

        book_line = (
            f"bid={best_bid} ask={best_ask} mid={mid:.6g} "
            f"spread={(best_ask - best_bid) / mid * 10_000:.2f}bps "
            f"q={q_ratio:+.2f} imb={imbalance:+.2f} mom={self._mom_bps:+.2f}bps"
        )
        activity.ok("BOOK", m.symbol, book_line)

        if skip_bid or at_limit_long:
            if skip_bid:
                self.exec.record_veto(f"{m.symbol}: bid gated (mom {self._mom_bps:+.2f}, imb {imbalance:+.2f})")
            await self._cancel_side("bid")
        else:
            await self.evaluate_side(
                "bid", bid_px, _clamp_size(bid_px, bid_size), reduce_only=at_limit_short
            )
        if skip_ask or at_limit_short:
            if skip_ask:
                self.exec.record_veto(f"{m.symbol}: ask gated (mom {self._mom_bps:+.2f}, imb {imbalance:+.2f})")
            await self._cancel_side("ask")
        else:
            await self.evaluate_side(
                "ask", ask_px, _clamp_size(ask_px, ask_size), reduce_only=at_limit_long
            )

    def _mark_self_cancelled(self, order: "OrderState | None") -> None:
        if order is not None:
            self._self_cancelled.add(order.client_order_index)
            while len(self._self_cancelled) > 128:
                self._self_cancelled.pop()

    async def _cancel_side(self, side: str) -> None:
        order = self.orders[side]
        if order is not None and order.order_index is not None and order.status in ("pending", "open"):
            try:
                await self.exec.cancel(self.market, order.order_index)
                self._mark_self_cancelled(order)
            except ExecutionError:
                pass
        self.orders[side] = None

    async def evaluate_side(self, side: str, desired_px: float, size_base: float, reduce_only: bool = False) -> None:
        m = self.market
        existing = self.orders[side]
        if existing and existing.status in ("pending", "open"):
            drift_bps = abs(existing.price - desired_px) / desired_px * 10_000
            if drift_bps < self.requote_bps:
                return  # resting order is fine — don't churn
            if existing.order_index is not None:
                try:
                    await self.exec.cancel(m, existing.order_index)
                    self._mark_self_cancelled(existing)
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
                m, is_ask=(side == "ask"), price=desired_px, size_base=size_base,
                reduce_only=reduce_only,
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

    # ── emergency taker flatten (backstop only — A-S skew unloads passively) ─
    async def _taker_flatten(self) -> None:
        """Close the position with a taker IOC, handling sub-minimum dust:
        the venue enforces min sizes on market orders too (code=200 then a
        silent sequencer cancel), so dust is first topped UP with one
        valid-size IOC to make the position closeable."""
        m = self.market
        pos = self.position.size
        mid = self.mid
        if mid is None or abs(pos) < m.min_size_step:
            return
        is_long = pos > 0
        min_limit_size = max(
            m.min_base_amount, max(m.min_quote_amount, MIN_QUOTE_USDG) / mid
        )
        if abs(pos) < min_limit_size:
            # Cooldown: never top-up more than once a minute — if the first
            # one didn't resolve the dust, wait for position reconciliation.
            if time.time() - self._last_topup_ts < 60:
                return
            self._last_topup_ts = time.time()
            topup = max(m.min_base_amount, max(m.min_quote_amount, MIN_QUOTE_USDG) / mid * 1.02)
            topup = round(topup, m.size_decimals)
            activity.warn(
                "CLOSE", m.symbol,
                f"dust {pos:+.6g} below venue minimum {min_limit_size:.6g} — "
                f"topping up {topup:.6g} to make it closeable",
            )
            worst = mid * (1.02 if is_long else 0.98)
            try:
                await self.exec.place_market_ioc(
                    m,
                    is_ask=not is_long,  # long dust -> buy more; short dust -> sell more
                    size_base=topup,
                    worst_price=m.round_price(worst),
                    reduce_only=False,
                )
            except ExecutionError as exc:
                activity.err("CLOSE", m.symbol, f"dust top-up failed: {exc.name}: {exc}")
            return  # next tick sees a closeable position
        await self.instant_close_ioc()

    # ── flatten ──────────────────────────────────────────────────────────────
    async def cancel_both_sides(self) -> None:
        for side in ("bid", "ask"):
            order = self.orders[side]
            if order and order.order_index is not None and order.status in ("pending", "open"):
                try:
                    await self.exec.cancel(self.market, order.order_index)
                    self._mark_self_cancelled(order)
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
            if coi in self._self_cancelled:
                self._self_cancelled.discard(coi)
                return  # expected confirmation of our own cancel — not an error
            # Surface silent rejections (e.g. market orders the sequencer
            # cancels) — without this, failed IOCs are invisible.
            if status.startswith("canceled"):
                hint = CANCEL_REASONS.get(status, "")
                activity.err(
                    "ORDER", self.market.symbol,
                    f"untracked order coi={coi} {status}" + (f" — {hint}" if hint else ""),
                )
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
        # Venue trade messages carry no "side" field — OUR side is derived
        # from which account id (ask or bid) matches ours.
        ask_acct = _i(trade.get("ask_account_id"))
        bid_acct = _i(trade.get("bid_account_id"))
        if self.account_index is not None and (ask_acct is not None or bid_acct is not None):
            is_ask = ask_acct == self.account_index
            if not is_ask and bid_acct != self.account_index:
                return  # not our trade at all
            maker_is_ask = trade.get("is_maker_ask")
            is_maker = bool(maker_is_ask) == is_ask if maker_is_ask is not None else True
        elif "is_ask" in trade:
            is_ask = bool(trade.get("is_ask"))
            is_maker = bool(trade.get("is_maker", trade.get("maker", True)))
        else:
            is_ask = str(trade.get("side", "")).lower() in ("sell", "ask")
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
            if abs(size) < self.market.min_size_step:
                self._pos_opened_ts = None
            elif self._pos_opened_ts is None:
                self._pos_opened_ts = time.time()
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
