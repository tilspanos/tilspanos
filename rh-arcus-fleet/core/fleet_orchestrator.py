"""Fleet orchestrator: workers, capital allocation, WS routing, dashboard API."""

from __future__ import annotations

import asyncio
import random
import time
from pathlib import Path

import yaml

from .config import BASE_URL, FleetConfig
from .execution import ExecutionEngine, ExecutionError
from .logging_utils import activity
from .market_registry import Market, MarketRegistry
from .market_worker import MarketWorker
from .rest import ArcusRest, RestError
from .risk import RiskManager
from .signing import load_ed25519
from .watchdog import Watchdog
from .ws_hub import WsHub

PRESETS_PATH = Path(__file__).resolve().parent.parent / "configs" / "market_presets.yaml"
RECONCILE_INTERVAL_S = 30


def load_presets(path: Path = PRESETS_PATH) -> dict:
    if not path.exists():
        return {"defaults": {}}
    with open(path) as f:
        return yaml.safe_load(f) or {"defaults": {}}


def preset_for(presets: dict, symbol: str) -> dict:
    merged = dict(presets.get("defaults") or {})
    for section, entries in presets.items():
        if section == "defaults" or not isinstance(entries, dict):
            continue
        override = entries.get(symbol)
        if isinstance(override, dict):
            merged.update(override)
    return merged


class FleetOrchestrator:
    def __init__(self, cfg: FleetConfig) -> None:
        self.cfg = cfg
        self.presets = load_presets()
        self.registry = MarketRegistry()
        self.hub = WsHub()
        self.rest: ArcusRest | None = None
        self.execution: ExecutionEngine | None = None
        self.risk = RiskManager(cfg.daily_loss_usd)
        self.watchdog = Watchdog(self)

        self.workers: dict[int, MarketWorker] = {}
        self.enabled_market_ids: set[int] = set()
        self.running = False
        self.paused = False
        self.view_only = False
        self.start_collateral: float | None = None
        self.session_start: float = 0.0
        self._tasks: dict[str, asyncio.Task] = {}
        self._worker_tasks: dict[int, asyncio.Task] = {}
        self._priv = None

    async def initialize(self) -> None:
        await self.registry.refresh()
        activity.ok(
            "BOOT", "",
            f"{len(self.registry.online)} ONLINE markets on PRODUCTION "
            f"({len(self.registry.perps)} perps)",
        )
        self._priv = load_ed25519(self.cfg.api_private_key)
        self.rest = ArcusRest(
            BASE_URL,
            private_key=self._priv,
            address=self.cfg.l1_address,
            account_index=self.cfg.account_index,
        )
        await self.rest.start()
        self.execution = ExecutionEngine(
            self.hub, self.rest, self._priv, self.cfg.l1_address, self.cfg.account_index
        )
        self.registry.on_new_market = self._on_new_market
        self.hub.on_order_update = self._route_order_update
        self.hub.on_fill_update = self._route_fill_update
        self.hub.on_account_update = self._route_account_update
        self.hub.on_position_update = self._route_position_update

    def _select_markets(self) -> list[Market]:
        candidates = self.registry.resolve_groups(self.cfg.enabled_groups)
        candidates.sort(key=lambda m: (-m.daily_quote_volume, m.market_id))
        return candidates[: self.cfg.max_concurrent_markets]

    def _build_worker(self, market: Market) -> MarketWorker:
        p = preset_for(self.presets, market.symbol)
        worker = MarketWorker(
            market,
            self.hub,
            self.execution,
            spread_bps=float(p.get("spread_bps", self.cfg.spread_bps)),
            requote_bps=float(p.get("requote_bps", self.cfg.requote_bps)),
            order_size_usd=float(p.get("order_size_usd", self.cfg.order_size_usd)),
            leverage=int(p.get("leverage", self.cfg.leverage)),
            refresh_ms=int(p.get("refresh_ms", self.cfg.refresh_ms)),
            max_hold_s=float(p.get("max_hold_s", self.cfg.max_hold_s)),
            adverse_stop_bps=float(p.get("adverse_stop_bps", self.cfg.adverse_stop_bps)),
            market_loss_usd=float(p.get("market_loss_usd", self.cfg.market_loss_usd)),
            min_edge_bps=float(p.get("min_edge_bps", 0.5)),
            vol_breaker_bps=float(p.get("vol_breaker_bps", 2.5)),
            inventory_mult=float(p.get("inventory_mult", 3.0)),
            quote_rwa_off_hours=self.cfg.quote_rwa_off_hours,
        )
        worker.account_index = self.cfg.account_index
        worker.our_address = self.cfg.l1_address or None
        return worker

    async def start_view_only(self) -> None:
        self.view_only = True
        if not self.registry.markets:
            await self.registry.refresh()
        for market in self._select_markets():
            self.workers[market.market_id] = self._build_worker(market)
            self.enabled_market_ids.add(market.market_id)
        await self.hub.start_market_data([w.market.symbol for w in self.workers.values()])
        for w in self.workers.values():
            self.hub.books_by_id[w.market.market_id] = self.hub.books[w.market.symbol]
        activity.ok(
            "BOOT", "",
            f"VIEW-ONLY dashboard: {len(self.registry.all)} markets discovered, "
            f"streaming {len(self.enabled_market_ids)} — add API keys to trade",
        )

    async def start(self) -> None:
        if self.running:
            self.paused = False
            return
        if self.execution is None:
            await self.initialize()

        selected = self._select_markets()
        if not selected:
            raise SystemExit(
                f"FATAL: enabled groups {self.cfg.enabled_groups} matched zero ONLINE markets."
            )
        for market in selected:
            worker = self._build_worker(market)
            worker.enabled = self.cfg.quote_on_start
            self.workers[market.market_id] = worker
            if worker.enabled:
                self.enabled_market_ids.add(market.market_id)

        await self.hub.start_market_data([w.market.symbol for w in self.workers.values()])
        for w in self.workers.values():
            self.hub.books_by_id[w.market.market_id] = self.hub.books[w.market.symbol]
        await self.hub.start_account(self.cfg.l1_address)

        self.running = True
        self.paused = False
        self.session_start = time.time()
        self.execution.last_send_ts = time.time()

        await self._reconcile_once()
        self.start_collateral = self.risk.collateral
        if self.start_collateral is not None:
            activity.ok("BOOT", "", f"session baseline: ${self.start_collateral:.2f} USDG on the venue")

        for mid in list(self.enabled_market_ids):
            self._spawn_worker_loop(mid)
        self._tasks["resync"] = asyncio.create_task(self.registry.run_resync_loop())
        self._tasks["risk"] = asyncio.create_task(self._risk_loop())
        self._tasks["reconcile"] = asyncio.create_task(self._reconcile_loop())
        self.watchdog.start()

        active = self.active_workers()
        if active:
            names = ", ".join(w.market.symbol for w in active)
            activity.ok("BOOT", "", f"fleet LIVE — quoting {len(active)} markets: {names}")
        else:
            activity.ok(
                "BOOT", "",
                f"fleet LIVE — {len(self.workers)} markets ready, NONE quoting yet. "
                "Enable markets from the dashboard pills to start trading.",
            )

    def _spawn_worker_loop(self, market_id: int) -> None:
        old = self._worker_tasks.get(market_id)
        if old and not old.done():
            old.cancel()
        worker = self.workers[market_id]
        self._worker_tasks[market_id] = asyncio.create_task(
            self._worker_loop(worker), name=f"worker-{worker.market.symbol}"
        )

    async def _worker_loop(self, worker: MarketWorker) -> None:
        await asyncio.sleep(random.uniform(0, min(2.0, worker.refresh_ms / 1000)))
        if self.execution:
            try:
                await self.execution.set_leverage(worker.market, worker.leverage)
            except ExecutionError as exc:
                activity.warn("CTRL", worker.market.symbol, f"leverage init failed: {exc} — quoting anyway")
        while self.running:
            try:
                await worker.tick(self.paused)
            except asyncio.CancelledError:
                raise
            except ExecutionError:
                pass
            except Exception as exc:
                activity.err("TICK", worker.market.symbol, f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(worker.refresh_ms / 1000)

    async def stop(self, flatten: bool = True) -> None:
        self.running = False
        pending = list(self._worker_tasks.values()) + list(self._tasks.values())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._worker_tasks.clear()
        self._tasks.clear()
        await self.watchdog.stop()
        if flatten and self.execution is not None:
            await self.flatten_all()
        await self.hub.stop()
        if self.rest is not None:
            await self.rest.close()
        activity.ok("BOOT", "", "fleet stopped")

    def pause(self) -> None:
        self.paused = True
        activity.warn("CTRL", "", "fleet PAUSED — resting orders stay, no new quotes")

    def resume(self) -> None:
        self.paused = False
        activity.ok("CTRL", "", "fleet RESUMED")

    async def flatten_market(self, key: str | int) -> None:
        market = self.registry.get(key)
        worker = self.workers.get(market.market_id)
        if worker is None:
            worker = self._build_worker(market)
        activity.warn("CTRL", market.symbol, "instant close requested")
        await worker.cancel_both_sides()
        await worker.instant_close_ioc()

    async def flatten_all(self) -> None:
        activity.warn("CTRL", "", "FLATTEN ALL — cancel everything, close every position")
        if self.execution:
            try:
                await self.execution.cancel_all()
            except ExecutionError as exc:
                activity.err("CTRL", "", f"cancel_all failed: {exc}")
        for worker in self.workers.values():
            worker.orders = {"bid": None, "ask": None}
            if abs(worker.position.size) >= worker.market.min_size_step:
                await worker.instant_close_ioc()

    def active_workers(self) -> list[MarketWorker]:
        return [w for w in self.workers.values() if w.enabled]

    async def restart_worker(self, market_id: int) -> None:
        worker = self.workers.get(market_id)
        if worker is None:
            return
        await worker.cancel_both_sides()
        fresh = self._build_worker(worker.market)
        fresh.position = worker.position
        fresh.fill_count = worker.fill_count
        fresh.volume_usd = worker.volume_usd
        fresh.realized_pnl = worker.realized_pnl
        self.workers[market_id] = fresh
        if self.running:
            self._spawn_worker_loop(market_id)

    async def restart_all_workers(self) -> None:
        for mid in list(self.workers):
            await self.restart_worker(mid)

    async def set_market_enabled(self, key: str | int, enabled: bool) -> None:
        market = self.registry.get(key)
        mid = market.market_id
        if enabled:
            if mid not in self.workers:
                self.workers[mid] = self._build_worker(market)
            self.workers[mid].enabled = True
            self.enabled_market_ids.add(mid)
            if self.running or self.view_only:
                await self.hub.add_market(market.symbol, mid)
            if self.running:
                self._spawn_worker_loop(mid)
            activity.ok("CTRL", market.symbol, "market enabled")
        else:
            worker = self.workers.get(mid)
            if worker:
                worker.enabled = False
                await worker.cancel_both_sides()
            self.enabled_market_ids.discard(mid)
            activity.warn("CTRL", market.symbol, "market disabled — quotes pulled")

    async def _on_new_market(self, market: Market) -> None:
        activity.ok("SYNC", market.symbol, f"new market {market.market_id} discovered — hot-adding")
        if "all" in self.cfg.enabled_groups or market.group in self.cfg.enabled_groups:
            if self.cfg.quote_on_start and len(self.active_workers()) < self.cfg.max_concurrent_markets:
                await self.set_market_enabled(market.market_id, True)
            elif market.market_id not in self.workers:
                self.workers[market.market_id] = self._build_worker(market)
                self.workers[market.market_id].enabled = False
                if self.running:
                    await self.hub.add_market(market.symbol, market.market_id)

    async def _risk_loop(self) -> None:
        while True:
            await asyncio.sleep(10)
            if not self.running:
                continue
            stats = self.stats()
            if self.risk.check_daily_loss(stats["pnl"]):
                self.paused = True
                await self.flatten_all()
            if self.risk.margin_headroom_low():
                actives = sorted(self.active_workers(), key=lambda w: w.market.daily_quote_volume)
                for worker in actives[: max(1, len(actives) // 4)]:
                    await self.set_market_enabled(worker.market.market_id, False)
            self.risk.open_order_count = stats["open_orders"]
            if self.risk.order_cap_pressure() and self.execution:
                try:
                    await self.execution.cancel_all()
                except ExecutionError as exc:
                    activity.err("RISK", "", f"stale sweep failed: {exc}")

    async def _reconcile_once(self) -> dict | None:
        """Overwrite local trackers with venue REST truth every 30s.

        Local fill/position math drifts. Phantom inventory would otherwise
        loop forever on reduce-only orders the engine silently cancels.
        """
        if self.rest is None or not self.cfg.l1_address:
            return None
        try:
            acct = await self.rest.account()
        except RestError as exc:
            if exc.status == 404:
                activity.warn("RECON", "", "account has no activity yet (undeposited) — reconcile skipped")
                return None
            activity.warn("RECON", "", f"account REST failed: {exc}")
            return None
        except Exception:
            return None
        self.risk.on_account_stats(acct if isinstance(acct, dict) else {})
        positions = acct.get("positions") if isinstance(acct, dict) else None
        if not positions:
            try:
                positions = await self.rest.positions()
            except Exception:
                positions = []
        if isinstance(positions, dict):
            positions = [{"marketId": int(k), **(v if isinstance(v, dict) else {})} for k, v in positions.items()]
        seen: set[int] = set()
        for pos in positions or []:
            mid = pos.get("marketId") or pos.get("market_id")
            if mid is None:
                continue
            mid = int(mid)
            seen.add(mid)
            worker = self.workers.get(mid)
            if worker is None:
                continue
            venue_size = _signed_pos(pos)
            local_size = worker.position.size
            if abs(venue_size - local_size) >= worker.market.min_size_step:
                activity.warn(
                    "RECON", worker.market.symbol,
                    f"local position {local_size:+.6g} != venue {venue_size:+.6g} — corrected to venue",
                )
            worker.on_position_event(pos)
        for mid, worker in self.workers.items():
            if mid not in seen and abs(worker.position.size) >= worker.market.min_size_step:
                activity.warn(
                    "RECON", worker.market.symbol,
                    f"local position {worker.position.size:+.6g} missing on venue — zeroed",
                )
                worker.on_position_event({"marketId": mid, "size": "0", "side": "LONG"})
        try:
            opens = await self.rest.open_orders()
            self.risk.open_order_count = len(opens)
        except Exception:
            pass
        return acct if isinstance(acct, dict) else None

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL_S)
            if self.running:
                await self._reconcile_once()

    async def _route_order_update(self, _channel: str, msg: dict) -> None:
        for item in _contents_items(msg.get("contents"), keys=("orders",)):
            mid = _market_id(item, self.registry)
            worker = self.workers.get(mid) if mid is not None else None
            if worker:
                worker.on_order_event(item)

    async def _route_fill_update(self, _channel: str, msg: dict) -> None:
        for item in _contents_items(msg.get("contents"), keys=("fills", "trades")):
            mid = _market_id(item, self.registry)
            worker = self.workers.get(mid) if mid is not None else None
            if worker:
                worker.on_trade_event(item)

    async def _route_account_update(self, _channel: str, msg: dict) -> None:
        contents = msg.get("contents")
        if isinstance(contents, dict):
            self.risk.on_account_stats(contents)
            if contents.get("type") == "REJECTED":
                reason = contents.get("rejectionReason") or ""
                activity.err("ORDER", "", f"engine REJECTED — {reason}")
            for item in _contents_items(contents.get("positions"), keys=()):
                mid = _market_id(item, self.registry)
                worker = self.workers.get(mid) if mid is not None else None
                if worker:
                    worker.on_position_event(item)

    async def _route_position_update(self, _channel: str, msg: dict) -> None:
        for item in _contents_items(msg.get("contents"), keys=("positions",)):
            mid = _market_id(item, self.registry)
            worker = self.workers.get(mid) if mid is not None else None
            if worker:
                worker.on_position_event(item)

    def config_dict(self) -> dict:
        return {
            "order_size_usd": self.cfg.order_size_usd,
            "spread_bps": self.cfg.spread_bps,
            "requote_bps": self.cfg.requote_bps,
            "refresh_ms": self.cfg.refresh_ms,
            "leverage": self.cfg.leverage,
            "max_concurrent_markets": self.cfg.max_concurrent_markets,
            "daily_loss_usd": self.cfg.daily_loss_usd,
            "max_hold_s": self.cfg.max_hold_s,
            "adverse_stop_bps": self.cfg.adverse_stop_bps,
        }

    async def apply_settings(self, settings: dict) -> dict:
        cfg = self.cfg

        def _num(key, lo, hi, cast=float):
            if key not in settings or settings[key] in (None, ""):
                return None
            value = cast(settings[key])
            if not (lo <= value <= hi):
                raise ValueError(f"{key} must be between {lo} and {hi}")
            return value

        order_size = _num("order_size_usd", 5, 100_000)
        spread = _num("spread_bps", 0.01, 500)
        requote = _num("requote_bps", 0.0, 100)
        refresh = _num("refresh_ms", 1_000, 300_000, int)
        leverage = _num("leverage", 1, 50, int)
        max_markets = _num("max_concurrent_markets", 1, 200, int)
        daily_loss = _num("daily_loss_usd", 1, 1_000_000)
        max_hold = _num("max_hold_s", 10, 3600)
        adverse_stop = _num("adverse_stop_bps", 1, 500)

        if order_size is not None:
            cfg.order_size_usd = order_size
        if spread is not None:
            cfg.spread_bps = spread
        if requote is not None:
            cfg.requote_bps = requote
        if refresh is not None:
            cfg.refresh_ms = refresh
        if leverage is not None:
            cfg.leverage = leverage
        if max_markets is not None:
            cfg.max_concurrent_markets = max_markets
        if daily_loss is not None:
            cfg.daily_loss_usd = daily_loss
            self.risk.daily_loss_usd = daily_loss
        if max_hold is not None:
            cfg.max_hold_s = max_hold
        if adverse_stop is not None:
            cfg.adverse_stop_bps = adverse_stop

        changed = []
        for worker in self.workers.values():
            if order_size is not None:
                worker.order_size_usd = order_size
            if spread is not None:
                worker.spread_bps = spread
            if requote is not None:
                worker.requote_bps = requote
            if refresh is not None:
                worker.refresh_ms = refresh
            if max_hold is not None:
                worker.max_hold_s = max_hold
            if adverse_stop is not None:
                worker.adverse_stop_bps = adverse_stop
            if leverage is not None and worker.leverage != leverage:
                worker.leverage = leverage
                changed.append(worker)

        if changed and self.running and not self.view_only and self.execution:
            for worker in changed:
                if not worker.enabled:
                    continue
                try:
                    await self.execution.set_leverage(worker.market, cfg.leverage)
                except ExecutionError as exc:
                    activity.err("CTRL", worker.market.symbol, f"leverage update failed: {exc}")

        applied = ", ".join(f"{k}={v}" for k, v in self.config_dict().items())
        activity.ok("CTRL", "", f"settings applied: {applied}")
        return self.config_dict()

    def stats(self) -> dict:
        workers = list(self.workers.values())
        open_orders = sum(
            1
            for w in workers
            for o in w.orders.values()
            if o is not None and o.status in ("pending", "open")
        )
        return {
            "running": self.running,
            "paused": self.paused,
            "view_only": self.view_only,
            "session_start": self.session_start,
            "session_seconds": int(time.time() - self.session_start) if self.session_start else 0,
            "markets_enabled": len(self.active_workers()),
            "markets_total": len(self.registry.all),
            "volume_usd": round(sum(w.volume_usd for w in workers), 2),
            "fills": sum(w.fill_count for w in workers),
            "pnl": round(sum(w.realized_pnl + w.position.unrealized_pnl for w in workers), 4),
            "open_orders": open_orders,
            "margin_used": self.risk.margin_used_fraction,
            "halted": self.risk.halted,
            "sent_tx": self.execution.sent_tx_count if self.execution else 0,
            "balance": self.risk.collateral,
            "net_pnl": (
                round(self.risk.collateral - self.start_collateral, 4)
                if self.risk.collateral is not None and self.start_collateral is not None
                else None
            ),
        } | self._cost_metrics(workers)

    def _cost_metrics(self, workers) -> dict:
        volume = sum(w.volume_usd for w in workers)
        net = (
            self.risk.collateral - self.start_collateral
            if self.risk.collateral is not None and self.start_collateral is not None
            else None
        )
        cost = None
        if net is not None and volume >= 100:
            cost = round(-net / volume * 1_000_000, 2)
        return {"cost_per_million": cost}

    def snapshot(self) -> dict:
        rows = [w.status_row() for w in self.workers.values() if w.enabled]
        rows.sort(key=lambda r: r["market_id"])
        fills = []
        for w in self.workers.values():
            for f in w.fills[-10:]:
                fills.append(
                    {
                        "ts": f.ts,
                        "symbol": w.market.symbol,
                        "side": f.side,
                        "size": f.size,
                        "price": f.price,
                        "notional": round(f.notional, 2),
                        "role": f.role,
                    }
                )
        fills.sort(key=lambda f: -f["ts"])
        return {
            "stats": self.stats(),
            "config": self.config_dict(),
            "markets": rows,
            "fills": fills[:50],
            "universe": [m.as_dict() for m in self.registry.all],
            "enabled_ids": sorted(self.enabled_market_ids),
        }


def _contents_items(contents, keys: tuple[str, ...] = ()) -> list[dict]:
    if contents is None:
        return []
    if isinstance(contents, list):
        return [c for c in contents if isinstance(c, dict)]
    if not isinstance(contents, dict):
        return []
    if contents.get("isSnapshot") and not any(k in contents for k in ("orderId", "tradeId", "marketId", "side", "fillPrice")):
        for key in keys or ("orders", "fills", "trades", "positions"):
            inner = contents.get(key)
            if inner is not None:
                return _contents_items(inner)
        return []
    for key in keys:
        if key in contents and isinstance(contents[key], (list, dict)):
            return _contents_items(contents[key])
    if any(k in contents for k in ("orderId", "tradeId", "clientId", "fillPrice", "marketId", "size", "status")):
        return [contents]
    return []


def _market_id(item: dict, registry: MarketRegistry) -> int | None:
    mid = item.get("marketId") or item.get("market_id")
    if mid is not None:
        try:
            return int(mid)
        except (TypeError, ValueError):
            pass
    name = item.get("market") or item.get("marketDisplayName")
    if name and str(name).upper() in registry.by_symbol:
        return registry.by_symbol[str(name).upper()].market_id
    return None


def _signed_pos(pos: dict) -> float:
    size = float(pos.get("size") or pos.get("position") or pos.get("positionSize") or 0)
    side = str(pos.get("side") or pos.get("positionSide") or "").upper()
    if side in ("SHORT", "SELL") and size > 0:
        size = -size
    sign = pos.get("sign")
    if sign is not None and int(sign) < 0:
        size = -abs(size)
    return size
