"""Dashboard API + static server (aiohttp).

Endpoints:
  GET  /               dashboard (web/index.html)
  GET  /api/status     full fleet snapshot (stats, markets, fills, universe)
  GET  /api/markets    market universe + enabled set
  POST /api/start      start the fleet
  POST /api/pause      pause quoting (resting orders stay)
  POST /api/resume     resume quoting
  POST /api/stop       stop the fleet (cancels + flattens)
  POST /api/flatten    {"symbol": "ETH"} one market, or {"all": true}
  POST /api/toggle     {"symbol": "ETH", "enabled": true}
  WS   /api/stream     1s snapshots + live activity events
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from aiohttp import WSMsgType, web

from core.fleet_orchestrator import FleetOrchestrator
from core.logging_utils import activity

INDEX_PATH = Path(__file__).resolve().parent / "index.html"


def build_app(fleet: FleetOrchestrator) -> web.Application:
    app = web.Application()
    app["fleet"] = fleet

    async def index(_request: web.Request) -> web.FileResponse:
        return web.FileResponse(INDEX_PATH)

    async def status(_request: web.Request) -> web.Response:
        return web.json_response(fleet.snapshot())

    async def markets(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "universe": [m.as_dict() for m in fleet.registry.all],
                "enabled_ids": sorted(fleet.enabled_market_ids),
            }
        )

    async def start(_request: web.Request) -> web.Response:
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
        asyncio.create_task(fleet.stop(flatten=True))
        return web.json_response({"ok": True})

    async def flatten(request: web.Request) -> web.Response:
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
                    json.dumps({"snapshot": fleet.snapshot(), "events": events})
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
    app.router.add_get("/api/status", status)
    app.router.add_get("/api/markets", markets)
    app.router.add_post("/api/start", start)
    app.router.add_post("/api/pause", pause)
    app.router.add_post("/api/resume", resume)
    app.router.add_post("/api/stop", stop)
    app.router.add_post("/api/flatten", flatten)
    app.router.add_post("/api/toggle", toggle)
    app.router.add_get("/api/stream", stream)
    return app


async def run_dashboard(fleet: FleetOrchestrator, host: str, port: int) -> web.AppRunner:
    app = build_app(fleet)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    activity.ok("WEB", "", f"dashboard live at http://{host}:{port}")
    return runner
