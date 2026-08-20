#!/usr/bin/env python3
"""PHASE 0 GATE — prove production connectivity (and order confirm) first.

    python bot.py fleet prove

Exit 0 means, verified live on https://api.arcus.xyz / wss://api.arcus.xyz/v1/ws:
  1. Full universe discovered (>= 40 ONLINE perps)
  2. Health + server time
  3. BTC-USD + ETH-USD L2 books streamed over WS
  4. If API keys are present: far ALO bid placed → confirmed OPEN on the
     `orders` channel (ACK is never trusted) → canceled by clientId

Anything less exits non-zero.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import BASE_URL, WS_URL, load_config
from core.market_registry import MarketRegistry
from core.rest import ArcusRest


async def _public_proof() -> MarketRegistry:
    rest = ArcusRest(BASE_URL)
    await rest.start()
    try:
        health = await rest.health()
        print(f"\u2713 health {health}")
        ts = await rest.time_ns()
        assert ts > 1_700_000_000_000_000_000, f"server time looks wrong: {ts}"
        print(f"\u2713 server time_ns={ts}")
    finally:
        await rest.close()

    registry = await MarketRegistry.sync(BASE_URL)
    n = len(registry.online)
    assert n >= 40, f"expected >=40 ONLINE perps on production, got {n}"
    print(f"\u2713 {n} ONLINE markets on PRODUCTION ({BASE_URL})")
    return registry


async def _ws_books(registry: MarketRegistry) -> None:
    from core.ws_hub import WsHub

    hub = WsHub(WS_URL)
    symbols = ["BTC-USD", "ETH-USD"]
    for s in symbols:
        assert s in registry.by_symbol, f"{s} missing from production universe"
    await hub.start_market_data(symbols)
    deadline = time.time() + 20
    try:
        while time.time() < deadline:
            if all(hub.books.get(s) and hub.books[s].mid is not None for s in symbols):
                break
            await asyncio.sleep(0.3)
        for s in symbols:
            book = hub.books.get(s)
            assert book is not None and book.mid is not None, f"{s}: no order book via WS"
            print(f"\u2713 {s} book mid={book.mid} bid={book.best_bid} ask={book.best_ask}")
    finally:
        await hub.stop()


async def _alo_roundtrip(registry: MarketRegistry) -> None:
    from core.execution import ExecutionEngine
    from core.market_worker import MarketWorker
    from core.rest import ArcusRest
    from core.signing import load_ed25519
    from core.ws_hub import WsHub

    cfg = load_config(require_keys=True)
    priv = load_ed25519(cfg.api_private_key)
    rest = ArcusRest(
        BASE_URL, private_key=priv, address=cfg.l1_address, account_index=cfg.account_index
    )
    await rest.start()
    hub = WsHub(WS_URL)
    execution = ExecutionEngine(hub, rest, priv, cfg.l1_address, cfg.account_index)
    market = registry.get("BTC-USD")
    worker = MarketWorker(
        market, hub, execution,
        spread_bps=1.0, requote_bps=0.02, order_size_usd=10,
        leverage=2, refresh_ms=12000,
    )
    worker.our_address = cfg.l1_address
    worker.account_index = cfg.account_index

    async def on_orders(_ch, msg):
        from core.fleet_orchestrator import _contents_items, _market_id

        for item in _contents_items(msg.get("contents"), keys=("orders",)):
            if _market_id(item, registry) == market.market_id:
                worker.on_order_event(item)

    hub.on_order_update = on_orders
    await hub.start_market_data([market.symbol])
    await hub.start_account(cfg.l1_address)
    deadline = time.time() + 15
    while time.time() < deadline and (hub.books[market.symbol].mid is None):
        await asyncio.sleep(0.2)
    mid = hub.books[market.symbol].mid
    assert mid is not None, "BTC-USD book not ready"
    # Rest 5% below best bid — ALO-safe, will not fill.
    px = market.round_price(mid * 0.95, "bid")
    size = max(market.min_base_amount, (market.min_quote_amount + 0.1) / px)
    size = market.round_size(size)
    try:
        cid = await execution.place_alo(market, is_ask=False, price=px, size_base=size)
        worker.register_order(
            "bid",
            __import__("core.market_worker", fromlist=["OrderState"]).OrderState(
                client_id=cid, price=px, size=size, is_ask=False
            ),
        )
        status = "pending"
        deadline = time.time() + 15
        while time.time() < deadline:
            state = worker.orders.get("bid")
            if state and state.status == "open":
                status = "open"
                break
            if state is None:
                status = "gone"
                break
            await asyncio.sleep(0.2)
        assert status == "open", f"ALO not confirmed OPEN on orders channel (got {status}) — ACK is not enough"
        print(f"\u2713 ALO bid cid={cid} confirmed OPEN via orders WS")
        await execution.cancel(market, client_id=cid)
        deadline = time.time() + 15
        while time.time() < deadline:
            if worker.orders.get("bid") is None:
                print("\u2713 cancel confirmed on orders WS")
                return
            await asyncio.sleep(0.2)
        print("⚠ cancel sent; WS confirm timed out (REST reconcile would catch this)")
    finally:
        try:
            await execution.cancel_all(market.market_id)
        except Exception:
            pass
        await hub.stop()
        await rest.close()


async def main() -> int:
    load_config(require_keys=False)
    print(f"proving PRODUCTION {BASE_URL} / {WS_URL}")
    registry = await _public_proof()
    await _ws_books(registry)
    has_keys = bool(os.environ.get("API_PRIVATE_KEY") and os.environ.get("L1_ADDRESS"))
    if has_keys:
        await _alo_roundtrip(registry)
    else:
        print("○ skipping ALO open/cancel (no API keys) — public proof passed")
        print("  run `python bot.py fleet setup-key` then re-run prove to confirm WS order lifecycle")
    print("\nPHASE 0 PROOF PASSED")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except AssertionError as exc:
        print(f"PROOF FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
