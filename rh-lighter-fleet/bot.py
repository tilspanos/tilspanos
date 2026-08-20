#!/usr/bin/env python3
"""RH Lighter fleet CLI.

    python bot.py fleet setup-key [N] # one-time: register API key N (default 4) + write .env
    python bot.py fleet prove         # Phase-0 production proof (BTC+ETH+LIT)
    python bot.py fleet start         # start fleet + dashboard (foreground)
    python bot.py fleet dashboard     # VIEW-ONLY dashboard, no API keys needed
    python bot.py fleet status        # snapshot from the running fleet
    python bot.py fleet stop          # stop the running fleet (flattens)
    python bot.py fleet flatten SYM   # instant-close one market
    python bot.py fleet flatten-all   # cancel everything + close all positions

`start` runs the orchestrator and the dashboard HTTP API in one process.
`status/stop/flatten*` talk to that process over the local dashboard API;
`flatten-all` also works standalone (direct venue calls) when no fleet
process is running.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib.error
import urllib.request

from core.config import load_config
from core.logging_utils import activity


def _api(cfg, path: str, method: str = "GET", body: dict | None = None) -> dict | None:
    url = f"http://{cfg.dashboard_host}:{cfg.dashboard_port}{path}"
    data = json.dumps(body or {}).encode() if method == "POST" else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, ConnectionError):
        return None


async def cmd_start() -> int:
    cfg = load_config(require_keys=True)
    from core.fleet_orchestrator import FleetOrchestrator
    from web.server import run_dashboard

    fleet = FleetOrchestrator(cfg)
    await fleet.initialize()
    runner = await run_dashboard(fleet, cfg.dashboard_host, cfg.dashboard_port)
    await fleet.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        activity.warn("CTRL", "", "shutting down — flattening fleet")
        await fleet.stop(flatten=True)
        await runner.cleanup()
    return 0


async def cmd_setup_key(index_arg: str | None) -> int:
    """One-time onboarding: generate a fresh API key pair, register it on the
    venue at the chosen index (signed by your L1 wallet key), verify it, and
    write the credentials to .env. The wallet key is used ONCE in memory to
    sign the ChangePubKey transaction and is never stored or logged."""
    import getpass
    from pathlib import Path

    import lighter

    from core.config import BASE_URL, CHAIN_ID, RESERVED_API_KEY_INDICES, load_config
    from core.signer_pool import discover_account_index

    load_config(require_keys=False)  # runs the production-only environment guard

    key_index = int(index_arg or os.environ.get("API_KEY_INDEX") or 4)
    if key_index in RESERVED_API_KEY_INDICES or not (2 <= key_index <= 254):
        print(f"API key index {key_index} is reserved or out of range — use 4-254 (not 157).")
        return 1

    l1_address = os.environ.get("L1_ADDRESS", "").strip() or input("L1 wallet address (0x...): ").strip()
    eth_private_key = os.environ.get("ETH_PRIVATE_KEY", "").strip() or getpass.getpass(
        "L1 wallet PRIVATE key (hidden; used once to sign key registration, never stored): "
    ).strip()
    if not l1_address or not eth_private_key:
        print("Both the wallet address and its private key are required.")
        return 1

    account_index = await discover_account_index(l1_address)
    print(f"account_index={account_index} (production, {BASE_URL})")

    api_private_key, api_public_key, err = lighter.create_api_key()
    if err is not None:
        print(f"key generation failed: {err}")
        return 1

    client = lighter.SignerClient(
        url=BASE_URL,
        account_index=account_index,
        api_private_keys={key_index: api_private_key},
        chain_id=CHAIN_ID,
    )
    try:
        resp, err = await client.change_api_key(
            eth_private_key=eth_private_key,
            new_pubkey=api_public_key,
            api_key_index=key_index,
        )
        if err is not None:
            print(f"key registration rejected by the venue: {err}")
            return 1
        print(f"ChangePubKey accepted (index {key_index}) — waiting for the venue to apply it…")
        verify_err = "not checked"
        for _ in range(15):
            await asyncio.sleep(2)
            verify_err = client.check_client()
            if verify_err is None:
                break
        if verify_err is not None:
            print(f"key not verified yet: {verify_err}")
            print("It can take a little longer — re-run `python bot.py fleet setup-key` to retry,")
            print("or verify manually before trading.")
            return 1
    finally:
        await client.close()

    env_path = Path(__file__).resolve().parent / ".env"
    updates = {
        "L1_ADDRESS": l1_address,
        "ACCOUNT_INDEX": str(account_index),
        "API_KEY_INDEX": str(key_index),
        "API_PRIVATE_KEY": api_private_key,
    }
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    seen = set()
    for i, line in enumerate(lines):
        key = line.split("=", 1)[0].strip()
        if key in updates:
            lines[i] = f"{key}={updates[key]}"
            seen.add(key)
    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n")

    print(f"\nAPI key registered and VERIFIED on production (index {key_index}).")
    print(f"Credentials written to {env_path} — keep that file private.")
    print("\nNext steps:")
    print("  python bot.py fleet prove   # real-order proof gate, must exit 0")
    print("  python bot.py fleet start   # go live")
    return 0


async def cmd_dashboard() -> int:
    """View-only dashboard: full market universe + live production market data,
    no API keys required, no orders possible. Great for checking the UI."""
    cfg = load_config(require_keys=False)
    from core.fleet_orchestrator import FleetOrchestrator
    from web.server import run_dashboard

    fleet = FleetOrchestrator(cfg)
    await fleet.registry.refresh()
    await fleet.start_view_only()
    runner = await run_dashboard(fleet, cfg.dashboard_host, cfg.dashboard_port)
    print(f"\nView-only dashboard: http://{cfg.dashboard_host}:{cfg.dashboard_port}  (Ctrl-C to exit)")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await fleet.hub.stop()
        await runner.cleanup()
    return 0


def cmd_status() -> int:
    cfg = load_config(require_keys=False)
    snap = _api(cfg, "/api/status")
    if snap is None:
        print("Fleet is not running (dashboard API unreachable).")
        return 1
    st = snap["stats"]
    state = "PAUSED" if st["paused"] else ("LIVE" if st["running"] else "OFFLINE")
    print(f"State:        {state}")
    print(f"Markets:      {st['markets_enabled']}/{st['markets_total']} enabled")
    print(f"Session:      {st['session_seconds']}s")
    print(f"Volume:       ${st['volume_usd']}")
    print(f"Fills:        {st['fills']}")
    print(f"PnL:          ${st['pnl']}")
    print(f"Open orders:  {st['open_orders']}")
    print(f"Tx sent:      {st['sent_tx']}")
    print()
    print(f"{'SYMBOL':<10}{'MID':>14}{'BID':>14}{'ASK':>14}{'POS':>12}{'FILLS':>7}{'PNL':>10}")
    for r in snap["markets"]:
        print(
            f"{r['symbol']:<10}{_n(r['mid']):>14}{_n(r['our_bid']):>14}"
            f"{_n(r['our_ask']):>14}{_n(r['position']):>12}{r['fills']:>7}{r['pnl']:>10}"
        )
    return 0


def _n(v) -> str:
    return "-" if v is None else f"{v:g}"


def cmd_stop() -> int:
    cfg = load_config(require_keys=False)
    if _api(cfg, "/api/stop", "POST") is None:
        print("Fleet is not running.")
        return 1
    print("Stop requested — fleet is cancelling quotes and flattening.")
    return 0


def cmd_flatten(symbol: str | None) -> int:
    cfg = load_config(require_keys=False)
    body = {"all": True} if symbol is None else {"symbol": symbol}
    if _api(cfg, "/api/flatten", "POST", body) is not None:
        print("Flatten requested via running fleet.")
        return 0
    if symbol is not None:
        print("Fleet is not running; start it or use flatten-all for standalone mode.")
        return 1
    # Standalone flatten-all: no fleet process — go straight at the venue.
    return asyncio.run(_standalone_flatten_all())


async def _standalone_flatten_all() -> int:
    cfg = load_config(require_keys=True)
    from core.execution import ExecutionEngine
    from core.market_registry import MarketRegistry
    from core.signer_pool import SignerPool, discover_account_index

    registry = await MarketRegistry.sync()
    account_index = cfg.account_index or await discover_account_index(cfg.l1_address)
    pool = SignerPool(cfg, account_index)
    execution = ExecutionEngine(pool)
    try:
        await execution.cancel_all()
        print("cancel_all sent for every market.")
        # Close any open perp positions found via the account endpoint.
        import lighter
        from core.config import BASE_URL

        api_client = lighter.ApiClient(lighter.Configuration(host=BASE_URL))
        try:
            acct = await lighter.AccountApi(api_client).account(by="index", value=str(account_index))
            accounts = getattr(acct, "accounts", None) or []
            positions = []
            for a in accounts:
                positions.extend(getattr(a, "positions", None) or [])
        finally:
            await api_client.close()
        for p in positions:
            size = float(getattr(p, "position", 0) or 0)
            sign = int(getattr(p, "sign", 1) or 1)
            if sign < 0:
                size = -abs(size)
            mid = float(getattr(p, "mark_price", 0) or 0)
            market_id = int(getattr(p, "market_id", getattr(p, "market_index", -1)))
            if abs(size) <= 0 or market_id < 0 or market_id not in registry.markets:
                continue
            market = registry.get(market_id)
            worst = market.round_price(mid * (0.98 if size > 0 else 1.02)) if mid else None
            if worst is None:
                continue
            await execution.place_market_ioc(
                market, is_ask=size > 0, size_base=abs(size), worst_price=worst, reduce_only=True
            )
            print(f"flattened {market.symbol}: {size:+g}")
        print("flatten-all complete.")
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser(prog="bot.py", description="RH Lighter production fleet")
    sub = parser.add_subparsers(dest="scope", required=True)
    fleet = sub.add_parser("fleet", help="fleet operations")
    fleet.add_argument(
        "command",
        choices=["setup-key", "prove", "start", "dashboard", "stop", "status", "flatten", "flatten-all"],
    )
    fleet.add_argument("symbol", nargs="?", help="market symbol for `flatten`, key index for `setup-key`")
    args = parser.parse_args()

    if args.command == "setup-key":
        return asyncio.run(cmd_setup_key(args.symbol))
    if args.command == "prove":
        from scripts.prove_production import main as prove_main
        return asyncio.run(prove_main())
    if args.command == "start":
        return asyncio.run(cmd_start())
    if args.command == "dashboard":
        return asyncio.run(cmd_dashboard())
    if args.command == "status":
        return cmd_status()
    if args.command == "stop":
        return cmd_stop()
    if args.command == "flatten":
        if not args.symbol:
            print("usage: python bot.py fleet flatten SYMBOL")
            return 2
        return cmd_flatten(args.symbol)
    if args.command == "flatten-all":
        return cmd_flatten(None)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
