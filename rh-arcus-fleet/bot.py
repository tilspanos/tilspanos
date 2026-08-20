#!/usr/bin/env python3
"""RH Arcus fleet CLI.

    python bot.py fleet setup-key     # one-time: EIP-712 register Ed25519 key + write .env
    python bot.py fleet prove         # Phase-0 production proof (public + ALO open/cancel)
    python bot.py fleet start         # start fleet + dashboard (foreground)
    python bot.py fleet dashboard     # VIEW-ONLY dashboard, no API keys needed
    python bot.py fleet status        # snapshot from the running fleet
    python bot.py fleet stop          # stop the running fleet (flattens)
    python bot.py fleet flatten SYM   # instant-close one market
    python bot.py fleet flatten-all   # cancel everything + close all positions
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
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
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


async def cmd_setup_key() -> int:
    """One-time onboarding: generate an Ed25519 pair, register it with an
    EIP-712 signature from the L1 wallet (used once in memory, never stored),
    verify it, and write credentials to .env."""
    import getpass
    import time
    from pathlib import Path

    from core.config import BASE_URL, CHAIN_ID, load_config
    from core.rest import ArcusRest
    from core.signing import eip712_create_api_key, generate_ed25519

    load_config(require_keys=False)

    l1_address = os.environ.get("L1_ADDRESS", "").strip() or input("L1 wallet address (0x...): ").strip()
    eth_private_key = os.environ.get("ETH_PRIVATE_KEY", "").strip() or getpass.getpass(
        "L1 wallet PRIVATE key (hidden; used once to sign key registration, never stored): "
    ).strip()
    if not l1_address or not eth_private_key:
        print("Both the wallet address and its private key are required.")
        return 1
    account_index = int(os.environ.get("ACCOUNT_INDEX") or input("Account index [0]: ").strip() or "0")

    priv_hex, pub_hex = generate_ed25519()
    valid_until = int(time.time() * 1000) + 170 * 86_400_000  # within [now+1d, now+180d]
    sig = eip712_create_api_key(
        eth_private_key,
        chain_id=CHAIN_ID,
        public_key_hex_value=pub_hex,
        api_wallet_name="rh-arcus-fleet",
        valid_until_ms=valid_until,
        account_index=account_index,
    )
    rest = ArcusRest(BASE_URL)
    await rest.start()
    try:
        created = await rest.create_api_key(
            {
                "address": l1_address,
                "publicKey": pub_hex,
                "apiWalletName": "rh-arcus-fleet",
                "validUntil": valid_until,
                "accountIndex": account_index,
                "signature": sig,
            }
        )
        print(f"createApiKey accepted: {created}")
        live = False
        for _ in range(30):
            keys = await rest.api_keys(l1_address)
            if any((k.get("apiKey") or k.get("publicKey") or "").lower() == pub_hex.lower() for k in keys):
                live = True
                break
            await asyncio.sleep(1)
        if not live:
            print("key not visible on GET /v1/apiKeys yet — re-run setup-key or wait and retry.")
            return 1
    finally:
        await rest.close()

    env_path = Path(__file__).resolve().parent / ".env"
    updates = {
        "L1_ADDRESS": l1_address,
        "ACCOUNT_INDEX": str(account_index),
        "API_PRIVATE_KEY": priv_hex,
        "API_PUBLIC_KEY": pub_hex,
    }
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    seen: set[str] = set()
    for i, line in enumerate(lines):
        key = line.split("=", 1)[0].strip()
        if key in updates:
            lines[i] = f"{key}={updates[key]}"
            seen.add(key)
    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n")

    print(f"\nAPI key registered and VERIFIED on production (accountIndex={account_index}).")
    print(f"Credentials written to {env_path} — keep that file private.")
    print("\nNext steps:")
    print("  python bot.py fleet prove   # production proof gate")
    print("  python bot.py fleet start   # go live")
    return 0


async def cmd_dashboard() -> int:
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
    print(f"Net PnL:      {st.get('net_pnl')}")
    print(f"Cost / $1M:   {st.get('cost_per_million')}")
    print(f"Open orders:  {st['open_orders']}")
    print(f"Tx sent:      {st['sent_tx']}")
    print()
    print(f"{'SYMBOL':<12}{'MID':>14}{'BID':>14}{'ASK':>14}{'POS':>12}{'FILLS':>7}{'PNL':>10}")
    for r in snap["markets"]:
        print(
            f"{r['symbol']:<12}{_n(r['mid']):>14}{_n(r['our_bid']):>14}"
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
    return asyncio.run(_standalone_flatten_all())


async def _standalone_flatten_all() -> int:
    cfg = load_config(require_keys=True)
    from core.execution import ExecutionEngine
    from core.market_registry import MarketRegistry
    from core.rest import ArcusRest
    from core.signing import load_ed25519
    from core.ws_hub import WsHub

    registry = await MarketRegistry.sync()
    priv = load_ed25519(cfg.api_private_key)
    rest = ArcusRest(
        private_key=priv, address=cfg.l1_address, account_index=cfg.account_index
    )
    await rest.start()
    hub = WsHub()
    execution = ExecutionEngine(hub, rest, priv, cfg.l1_address, cfg.account_index)
    try:
        await execution.cancel_all()
        print("cancel_all sent for every market.")
        try:
            positions = await rest.positions()
        except Exception as exc:
            print(f"could not list positions: {exc}")
            positions = []
        for p in positions:
            mid = int(p.get("marketId") or -1)
            if mid not in registry.markets:
                continue
            market = registry.get(mid)
            from core.fleet_orchestrator import _signed_pos

            size = _signed_pos(p)
            mid_px = float(p.get("markPrice") or market.mark_price or 0)
            if abs(size) <= 0 or not mid_px:
                continue
            worst = market.round_price(mid_px * (0.98 if size > 0 else 1.02))
            await hub.start_market_data([market.symbol])
            await asyncio.sleep(0.5)
            await execution.place_market_ioc(
                market, is_ask=size > 0, size_base=abs(size), worst_price=worst, reduce_only=True
            )
            print(f"flattened {market.symbol}: {size:+g}")
        print("flatten-all complete.")
        return 0
    finally:
        await hub.stop()
        await rest.close()


def main() -> int:
    parser = argparse.ArgumentParser(prog="bot.py", description="RH Arcus production fleet")
    sub = parser.add_subparsers(dest="scope", required=True)
    fleet = sub.add_parser("fleet", help="fleet operations")
    fleet.add_argument(
        "command",
        choices=["setup-key", "prove", "start", "dashboard", "stop", "status", "flatten", "flatten-all"],
    )
    fleet.add_argument("symbol", nargs="?", help="market symbol for `flatten`")
    args = parser.parse_args()

    if args.command == "setup-key":
        return asyncio.run(cmd_setup_key())
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
