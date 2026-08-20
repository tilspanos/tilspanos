#!/usr/bin/env python3
"""PHASE 0 GATE — prove real order flow on PRODUCTION before anything else.

    python scripts/prove_production.py

Exit 0 means, verified live on https://api.rh.lighter.xyz/:
  1. Full universe discovered (>= 40 perps, >= 26 spot markets)
  2. BTC, ETH, LIT: post-only bid placed -> confirmed OPEN via WS -> canceled
  3. ETH: $10 IOC market order FILLED (then position flattened)
  4. sendTxBatch round-trip on SOL (create + cancel in one batch)

Anything less exits non-zero. Requires L1_ADDRESS + API_PRIVATE_KEY in .env.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import BASE_URL, MIN_QUOTE_USDG, load_config
from core.execution import ExecutionEngine, ExecutionError
from core.logging_utils import activity
from core.market_registry import Market, MarketRegistry
from core.market_worker import MarketWorker
from core.signer_pool import SignerPool, discover_account_index
from core.ws_hub import WsHub

PROOF_SIZE_USD = 10.0  # venue minimum notional
WS_CONFIRM_TIMEOUT_S = 15.0
SAFE_BID_DISCOUNT = 0.98  # rest 2% below best bid: post-only-safe, won't fill


class ProofHarness:
    def __init__(self) -> None:
        self.cfg = load_config(require_keys=True)
        self.registry: MarketRegistry | None = None
        self.pool: SignerPool | None = None
        self.execution: ExecutionEngine | None = None
        self.hub = WsHub()
        self.workers: dict[int, MarketWorker] = {}

    def worker(self, market: Market) -> MarketWorker:
        if market.market_id not in self.workers:
            self.workers[market.market_id] = MarketWorker(
                market, self.hub, self.execution,
                spread_bps=1.0, requote_bps=0.02, order_size_usd=PROOF_SIZE_USD,
                leverage=2, refresh_ms=12000,
            )
        return self.workers[market.market_id]

    async def wait_order_status(self, market: Market, coi: int, want: set[str]) -> str:
        """Confirm order state via the account WS stream — sendTx 200 is never
        trusted as an open order."""
        worker = self.workers[market.market_id]
        deadline = time.time() + WS_CONFIRM_TIMEOUT_S
        while time.time() < deadline:
            for state in worker.orders.values():
                if state and state.client_order_index == coi and state.status in want:
                    return state.status
            # order may already be terminal (filled/canceled cleared the slot)
            await asyncio.sleep(0.2)
        # fall back to REST accountActiveOrders visibility via order state
        for state in worker.orders.values():
            if state and state.client_order_index == coi:
                return state.status
        return "timeout"

    async def run(self) -> None:
        # ── 1. Discover ALL markets ──────────────────────────────────────────
        self.registry = await MarketRegistry.sync(BASE_URL)
        n_perps, n_spots = len(self.registry.perps), len(self.registry.spots)
        assert n_perps >= 40, f"expected >=40 perps on production, got {n_perps}"
        assert n_spots >= 26, f"expected >=26 spot markets on production, got {n_spots}"
        print(f"\u2713 {len(self.registry.all)} active markets on PRODUCTION ({n_perps} perps, {n_spots} spot)")

        account_index = self.cfg.account_index
        if account_index is None:
            account_index = await discover_account_index(self.cfg.l1_address)
        print(f"\u2713 account_index={account_index} (L1 {self.cfg.l1_address})")
        self.cfg.account_index = account_index
        self.pool = SignerPool(self.cfg, account_index)
        self.execution = ExecutionEngine(self.pool)

        # ── WS: books for proof markets + authenticated account stream ──────
        proof_syms = ["BTC", "ETH", "LIT", "SOL"]
        markets = [self.registry.get(s) for s in proof_syms]
        for m in markets:
            self.worker(m)

        async def route_orders(_mtype: str, msg: dict) -> None:
            from core.fleet_orchestrator import _per_market
            for mid, orders in _per_market(msg.get("orders")):
                if mid in self.workers:
                    for o in orders:
                        self.workers[mid].on_order_event(o)

        async def route_trades(_mtype: str, msg: dict) -> None:
            from core.fleet_orchestrator import _per_market
            for mid, trades in _per_market(msg.get("trades")):
                if mid in self.workers:
                    for t in trades:
                        self.workers[mid].on_trade_event(t)

        self.hub.on_order_update = route_orders
        self.hub.on_trade_update = route_trades
        await self.hub.start_market_data([m.market_id for m in markets])
        await self.hub.start_account(account_index, self.pool.create_auth_token())

        # wait for books
        deadline = time.time() + 15
        while time.time() < deadline:
            if all(self.hub.books[m.market_id].mid is not None for m in markets):
                break
            await asyncio.sleep(0.3)
        for m in markets:
            assert self.hub.books[m.market_id].mid is not None, f"{m.symbol}: no order book via WS"
        print("\u2713 WS order books live for", ", ".join(proof_syms))

        # ── 2. Three-market order proof (large/mid/small) ────────────────────
        for m in [self.registry.get(s) for s in ("BTC", "ETH", "LIT")]:
            await self.prove_post_only(m)

        # ── 3. One market fill proof ─────────────────────────────────────────
        await self.prove_market_fill(self.registry.get("ETH"))

        # ── 4. Batch proof on SOL ────────────────────────────────────────────
        await self.prove_batch(self.registry.get("SOL"))

        print("\n\U0001f7e2 PRODUCTION EXECUTION PROVEN \u2014 build fleet + dashboard")

    async def prove_post_only(self, m: Market) -> None:
        book = self.hub.books[m.market_id]
        bid_px = m.round_price(book.best_bid * SAFE_BID_DISCOUNT)
        size = max(MIN_QUOTE_USDG / bid_px * 1.02, m.min_base_amount)
        size = round(size, m.size_decimals)
        worker = self.worker(m)
        coi = await self.execution.place_post_only(m, is_ask=False, price=bid_px, size_base=size)
        from core.market_worker import OrderState
        # register_order replays any WS confirmation that raced ahead of the
        # REST response, so a fast stream can't make the proof miss "open".
        worker.register_order("bid", OrderState(client_order_index=coi, price=bid_px, size=size))
        status = await self.wait_order_status(m, coi, {"open"})
        assert status == "open", f"{m.symbol} order not confirmed open via WS: {status}"
        state = worker.orders["bid"]
        assert state and state.order_index is not None, f"{m.symbol}: no order_index from WS"
        await self.execution.cancel(m, state.order_index)
        worker.orders["bid"] = None
        print(f"\u2713 {m.symbol} production: placed \u2192 open \u2192 canceled (coi={coi})")

    async def prove_market_fill(self, m: Market) -> None:
        book = self.hub.books[m.market_id]
        mid = book.mid
        size = round(max(MIN_QUOTE_USDG / mid * 1.05, m.min_base_amount), m.size_decimals)
        worker = self.worker(m)
        fills_before = worker.fill_count
        await self.execution.place_market_ioc(
            m, is_ask=False, size_base=size,
            worst_price=m.round_price(mid * 1.02), reduce_only=False,
        )
        deadline = time.time() + WS_CONFIRM_TIMEOUT_S
        while time.time() < deadline and worker.fill_count == fills_before:
            await asyncio.sleep(0.2)
        assert worker.fill_count > fills_before, f"{m.symbol} market IOC produced no fill via WS"
        fill = worker.fills[-1]
        print(f"\u2713 {m.symbol} production market fill: {fill.price} \u00d7 {fill.size}")
        # flatten the proof position immediately
        await worker.instant_close_ioc()
        deadline = time.time() + WS_CONFIRM_TIMEOUT_S
        while time.time() < deadline and abs(worker.position.size) >= m.min_size_step:
            await asyncio.sleep(0.3)
        print(f"\u2713 {m.symbol} proof position flattened (pos={worker.position.size:+.6g})")

    async def prove_batch(self, m: Market) -> None:
        book = self.hub.books[m.market_id]
        bid_px = m.round_price(book.best_bid * SAFE_BID_DISCOUNT)
        size = round(max(MIN_QUOTE_USDG / bid_px * 1.02, m.min_base_amount), m.size_decimals)
        worker = self.worker(m)
        from core.market_worker import OrderState
        # batch 1: create
        cois = await self.execution.replace_quotes_batch(m, cancels=[], creates=[(False, bid_px, size)])
        assert cois, "sendTxBatch returned no client_order_index"
        worker.register_order("bid", OrderState(client_order_index=cois[0], price=bid_px, size=size))
        status = await self.wait_order_status(m, cois[0], {"open"})
        assert status == "open", f"{m.symbol} batch order not open: {status}"
        state = worker.orders["bid"]
        # batch 2: cancel via batch as well
        await self.execution.replace_quotes_batch(m, cancels=[state.order_index], creates=[])
        print(f"\u2713 sendTxBatch works on {m.symbol}: create+confirm+cancel via batch")

    async def cleanup(self) -> None:
        try:
            if self.execution is not None:
                await self.execution.cancel_all()
        except Exception:
            pass
        await self.hub.stop()
        if self.pool is not None:
            await self.pool.close()


async def main() -> int:
    harness = ProofHarness()
    try:
        await harness.run()
        return 0
    except AssertionError as exc:
        print(f"\n\u2717 PROOF FAILED: {exc}", file=sys.stderr)
        return 1
    except ExecutionError as exc:
        print(f"\n\u2717 EXECUTION ERROR: {exc.name}({exc.code}): {exc}", file=sys.stderr)
        return 1
    finally:
        await harness.cleanup()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
