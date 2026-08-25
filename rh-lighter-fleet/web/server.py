"""Dashboard API + static server (aiohttp).

Endpoints:
  GET  /               dashboard (web/index.html)
  GET  /styles.css     dashboard stylesheet
  GET  /dashboard.js   dashboard UI (no trading logic)
  GET  /assets/*       venue wordmarks
  GET  /api/status     full fleet snapshot (stats, markets, fills, universe)
  GET  /api/markets    market universe + enabled set
  POST /api/start      start the fleet
  POST /api/pause      pause quoting (resting orders stay)
  POST /api/resume     resume quoting
  POST /api/stop       stop the fleet (cancels + flattens)
  POST /api/flatten    {"symbol": "ETH"} one market, or {"all": true}
  POST /api/toggle     {"symbol": "ETH", "enabled": true}
  POST /api/config     live fleet settings {"order_size_usd", "spread_bps", ...}
  WS   /api/stream     1s snapshots + live activity events
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from aiohttp import WSMsgType, web

from core.fleet_orchestrator import FleetOrchestrator
from core.logging_utils import activity

WEB_DIR = Path(__file__).resolve().parent
INDEX_PATH = WEB_DIR / "index.html"
_NO_STORE = {"Cache-Control": "no-store"}


def _decorate(fleet: FleetOrchestrator, snap: dict) -> dict:
    """Attach venue chrome for the dashboard. Does not change trading state."""
    out = dict(snap)
    out["venue"] = {
        "name": "Lighter",
        "partner": "Robinhood",
        "chain_id": 466324,
        "wallet": getattr(fleet.cfg, "l1_address", "") or "",
        "collateral": "USDG",
        "host": "api.rh.lighter.xyz",
    }
    return out


def build_app(fleet: FleetOrchestrator) -> web.Application:
    app = web.Application()
    app["fleet"] = fleet

    async def index(_request: web.Request) -> web.FileResponse:
        # no-store: browsers must always fetch the current dashboard code
        return web.FileResponse(INDEX_PATH, headers=_NO_STORE)

    async def stylesheet(_request: web.Request) -> web.FileResponse:
        return web.FileResponse(WEB_DIR / "styles.css", headers=_NO_STORE)

    async def script(_request: web.Request) -> web.FileResponse:
        return web.FileResponse(WEB_DIR / "dashboard.js", headers=_NO_STORE)

    async def status(_request: web.Request) -> web.Response:
        return web.json_response(_decorate(fleet, fleet.snapshot()))

    async def markets(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "universe": [m.as_dict() for m in fleet.registry.all],
                "enabled_ids": sorted(fleet.enabled_market_ids),
            }
        )

    def _view_only_block() -> web.Response | None:
        if fleet.view_only:
            return web.json_response(
                {
                    "ok": False,
                    "error": "view-only mode — set L1_ADDRESS + API_PRIVATE_KEY in .env "
                    "and run `python bot.py fleet start` to trade",
                },
                status=409,
            )
        return None

    async def start(_request: web.Request) -> web.Response:
        if (blocked := _view_only_block()) is not None:
            return blocked
        if not fleet.running:
            asyncio.create_task(fleet.start())
        else:
            fleet.resume()
        return web.json_response({"ok": True})

    async def pause(_request: web.Request) -> web.Response:
        fleet.pause()
        return web.json_response({"ok": True})

    async def resume(_request: web.Request) -> web.Response:
        fleet.resume()
        return web.json_response({"ok": True})

    async def stop(_request: web.Request) -> web.Response:
        if (blocked := _view_only_block()) is not None:
            return blocked
        asyncio.create_task(fleet.stop(flatten=True))
        return web.json_response({"ok": True})

    async def flatten(request: web.Request) -> web.Response:
        if (blocked := _view_only_block()) is not None:
            return blocked
        body = await request.json()
        if body.get("all"):
            await fleet.flatten_all()
        else:
            await fleet.flatten_market(body["symbol"])
        return web.json_response({"ok": True})

    async def toggle(request: web.Request) -> web.Response:
        body = await request.json()
        await fleet.set_market_enabled(body["symbol"], bool(body.get("enabled", True)))
        return web.json_response({"ok": True})

    async def config(request: web.Request) -> web.Response:
        body = await request.json()
        try:
            effective = await fleet.apply_settings(body)
        except (ValueError, TypeError) as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        return web.json_response({"ok": True, "config": effective})

    async def stream(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        last_seq = 0
        try:
            while not ws.closed:
                events = activity.since(last_seq)
                if events:
                    last_seq = events[-1]["seq"]
                await ws.send_str(
                    json.dumps({"snapshot": _decorate(fleet, fleet.snapshot()), "events": events})
                )
                # drain any client pings without blocking the push cadence
                try:
                    msg = await ws.receive(timeout=1.0)
                    if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                        break
                except asyncio.TimeoutError:
                    pass
        finally:
            await ws.close()
        return ws

    app.router.add_get("/", index)
    app.router.add_get("/styles.css", stylesheet)
    app.router.add_get("/dashboard.js", script)
    app.router.add_static("/assets", WEB_DIR / "assets")
    app.router.add_get("/api/status", status)
    app.router.add_get("/api/markets", markets)
    app.router.add_post("/api/start", start)
    app.router.add_post("/api/pause", pause)
    app.router.add_post("/api/resume", resume)
    app.router.add_post("/api/stop", stop)
    app.router.add_post("/api/flatten", flatten)
    app.router.add_post("/api/toggle", toggle)
    app.router.add_post("/api/config", config)
    app.router.add_get("/api/stream", stream)
    return app


async def run_dashboard(fleet: FleetOrchestrator, host: str, port: int) -> web.AppRunner:
    app = build_app(fleet)
    # No HTTP access log: it drowns the trading activity log in the terminal.
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    activity.ok("WEB", "", f"dashboard live at http://{host}:{port}")
    return runner
