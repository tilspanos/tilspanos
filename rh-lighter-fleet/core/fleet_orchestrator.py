"""Fleet orchestrator: schedules every market worker, allocates capital,
routes account-stream confirmations, and exposes the control surface used by
the CLI and dashboard (start / pause / resume / stop / flatten / flatten-all).
"""

from __future__ import annotations

import asyncio
import random
import time
from pathlib import Path

import aiohttp
import yaml

from .config import FleetConfig
from .execution import ExecutionEngine, ExecutionError
from .logging_utils import activity
from .market_registry import Market, MarketRegistry
from .market_worker import MarketWorker
from .risk import RiskManager
from .signer_pool import SignerPool, discover_account_index
from .watchdog import Watchdog
from .ws_hub import WsHub

PRESETS_PATH = Path(__file__).resolve().parent.parent / "configs" / "market_presets.yaml"
AUTH_REFRESH_S = 45 * 60  # tokens live 1h; refresh comfortably before expiry
RECONCILE_INTERVAL_S = 30  # venue-truth position sync cadence


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
        self.pool: SignerPool | None = None
        self.execution: ExecutionEngine | None = None
        self.risk = RiskManager(cfg.daily_loss_usd)
        self.watchdog = Watchdog(self)

        self.workers: dict[int, MarketWorker] = {}
        self.enabled_market_ids: set[int] = set()
        self.running = False
        self.paused = False
        self.view_only = False  # dashboard preview: live market data, no signing keys
        self.start_collateral: float | None = None  # session baseline for venue net PnL
        self.session_start: float = 0.0
        self._tasks: dict[str, asyncio.Task] = {}
        self._worker_tasks: dict[int, asyncio.Task] = {}

    # ── boot ─────────────────────────────────────────────────────────────────
    async def initialize(self) -> None:
        await self.registry.refresh()
        activity.ok(
            "BOOT", "",
            f"{len(self.registry.all)} active markets on PRODUCTION "
            f"({len(self.registry.perps)} perps, {len(self.registry.spots)} spot)",
        )
        account_index = self.cfg.account_index
        if account_index is None:
            account_index = await discover_account_index(self.cfg.l1_address)
            activity.ok("BOOT", "", f"account_index={account_index} discovered via accountsByL1Address")
        self.cfg.account_index = account_index
        self.pool = SignerPool(self.cfg, account_index)
        self.execution = ExecutionEngine(self.pool)
        self.registry.on_new_market = self._on_new_market

        # Route account-stream confirmations to workers.
        self.hub.on_order_update = self._route_order_update
        self.hub.on_trade_update = self._route_trade_update
        self.hub.on_account_update = self._route_account_update

    def _select_markets(self) -> list[Market]:
        candidates = self.registry.resolve_groups(self.cfg.enabled_groups)
        # Capital allocation: most-liquid first, capped by max_concurrent_markets.
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
            strategy=str(p.get("strategy", self.cfg.strategy)),
            strategy_params=p,  # preset dict doubles as strategy params
        )
        worker.account_index = self.cfg.account_index
        return worker

    async def start_view_only(self) -> None:
        """Dashboard preview: live production market data for the configured
        groups, no keys, no orders. Used by `python bot.py fleet dashboard`."""
        self.view_only = True
        if not self.registry.markets:
            await self.registry.refresh()
        for market in self._select_markets():
            self.workers[market.market_id] = self._build_worker(market)
            self.enabled_market_ids.add(market.market_id)
        await self.hub.start_market_data(sorted(self.enabled_market_ids))
        activity.ok(
            "BOOT", "",
            f"VIEW-ONLY dashboard: {len(self.registry.all)} markets discovered, "
            f"streaming {len(self.enabled_market_ids)} — add API keys to trade",
        )

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def start(self) -> None:
        if self.running:
            self.paused = False
            return
        if self.execution is None:
            await self.initialize()

        selected = self._select_markets()
        if not selected:
            raise SystemExit(
                f"FATAL: enabled groups {self.cfg.enabled_groups} matched zero markets."
            )
        for market in selected:
            worker = self._build_worker(market)
            # Markets are OPT-IN: unless FLEET_QUOTE_ON_START=true, nothing
            # quotes until the user enables it from the dashboard pills.
            worker.enabled = self.cfg.quote_on_start
            self.workers[market.market_id] = worker
            if worker.enabled:
                self.enabled_market_ids.add(market.market_id)

        # Subscribe books for every candidate market (cheap) so enabling a
        # pill later quotes immediately with a warm order book.
        await self.hub.start_market_data(sorted(self.workers))
        auth_token = self.pool.create_auth_token()
        await self.hub.start_account(self.cfg.account_index, auth_token)

        self.running = True
        self.paused = False
        self.session_start = time.time()
        self.execution.last_send_ts = time.time()

        # Seed venue truth (balance + positions) and set the session baseline
        # for net PnL. Also cleans up any position left from a previous run.
        await self._reconcile_once()
        self.start_collateral = self.risk.collateral
        if self.start_collateral is not None:
            activity.ok("BOOT", "", f"session baseline: ${self.start_collateral:.2f} USDG on the venue")

        for mid in list(self.enabled_market_ids):
            self._spawn_worker_loop(mid)
        self._tasks["resync"] = asyncio.create_task(self.registry.run_resync_loop())
        self._tasks["auth"] = asyncio.create_task(self._auth_refresh_loop())
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
        # Jitter startup so 20+ workers don't sign in lockstep.
        await asyncio.sleep(random.uniform(0, min(2.0, worker.refresh_ms / 1000)))
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
                pass  # already logged + veto-recorded by the worker
            except Exception as exc:
                activity.err("TICK", worker.market.symbol, f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(worker.refresh_ms / 1000)

    async def stop(self, flatten: bool = True) -> None:
        self.running = False
        pending = list(self._worker_tasks.values()) + list(self._tasks.values())
        for task in pending:
            task.cancel()
        # Await everything we cancelled: otherwise asyncio tears the loop down
        # under still-pending tasks and floods the terminal with
        # "Task was destroyed but it is pending!" noise that hides the real
        # shutdown reason.
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._worker_tasks.clear()
        self._tasks.clear()
        await self.watchdog.stop()
        if flatten and self.execution is not None:
            await self.flatten_all()
        await self.hub.stop()
        if self.pool is not None:
            await self.pool.close()
        activity.ok("BOOT", "", "fleet stopped")

    def pause(self) -> None:
        self.paused = True
        activity.warn("CTRL", "", "fleet PAUSED — resting orders stay, no new quotes")

    def resume(self) -> None:
        self.paused = False
        activity.ok("CTRL", "", "fleet RESUMED")

    # ── flatten ──────────────────────────────────────────────────────────────
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
        try:
            await self.execution.cancel_all()
        except ExecutionError as exc:
            activity.err("CTRL", "", f"cancel_all failed: {exc}")
        for worker in self.workers.values():
            worker.orders = {"bid": None, "ask": None}
            if abs(worker.position.size) >= worker.market.min_size_step:
                await worker.instant_close_ioc()

    # ── worker restarts (watchdog hooks) ─────────────────────────────────────
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

    # ── market enable/disable from the dashboard ─────────────────────────────
    async def set_market_enabled(self, key: str | int, enabled: bool) -> None:
        market = self.registry.get(key)
        mid = market.market_id
        if enabled:
            if mid not in self.workers:
                self.workers[mid] = self._build_worker(market)
            self.workers[mid].enabled = True
            self.enabled_market_ids.add(mid)
            if self.running or self.view_only:
                await self.hub.add_market(mid)
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
        activity.ok("SYNC", market.symbol, f"new market {market.market_id} discovered — hot-adding with default preset")
        if "all" in self.cfg.enabled_groups or market.group in self.cfg.enabled_groups:
            # Never auto-quote a new market unless the user opted into
            # quote-on-start; otherwise it just appears as a selectable pill.
            if self.cfg.quote_on_start and len(self.active_workers()) < self.cfg.max_concurrent_markets:
                await self.set_market_enabled(market.market_id, True)
            elif market.market_id not in self.workers:
                self.workers[market.market_id] = self._build_worker(market)
                self.workers[market.market_id].enabled = False
                if self.running:
                    await self.hub.add_market(market.market_id)

    # ── background loops ─────────────────────────────────────────────────────
    async def _auth_refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(AUTH_REFRESH_S)
            try:
                token = self.pool.create_auth_token()
                self.hub.refresh_auth(token)
                await self.hub.account_conn.force_reconnect()  # resubscribe with fresh token
                activity.ok("AUTH", "", "WS auth token refreshed")
            except Exception as exc:
                activity.err("AUTH", "", f"auth refresh failed: {exc}")

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
                # pause the least-liquid active markets first
                actives = sorted(self.active_workers(), key=lambda w: w.market.daily_quote_volume)
                for worker in actives[: max(1, len(actives) // 4)]:
                    await self.set_market_enabled(worker.market.market_id, False)
            self.risk.open_order_count = stats["open_orders"]
            if self.risk.order_cap_pressure():
                try:
                    await self.execution.cancel_all()
                except ExecutionError as exc:
                    activity.err("RISK", "", f"stale sweep failed: {exc}")

    async def _reconcile_once(self) -> dict | None:
        """Pull the VENUE's view of balance + positions over public REST and
        overwrite the local trackers. Guarantees the fleet converges to
        exchange truth (a phantom local position can otherwise loop forever
        on reduce-only orders the sequencer silently cancels)."""
        from .config import BASE_URL

        url = f"{BASE_URL.rstrip('/')}/api/v1/account"
        params = {"by": "index", "value": str(self.cfg.account_index)}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    data = await resp.json()
        except Exception:
            return None  # transient; next cycle
        accounts = data.get("accounts") or []
        if not accounts:
            return None
        stats = accounts[0]
        self.risk.on_account_stats(stats)
        for pos in stats.get("positions") or []:
            worker = self.workers.get(pos.get("market_id"))
            if worker is None:
                continue
            venue_size = abs(float(pos.get("position", 0) or 0))
            if int(pos.get("sign", 1) or 1) < 0:
                venue_size = -venue_size
            local_size = worker.position.size
            if abs(venue_size - local_size) >= worker.market.min_size_step:
                activity.warn(
                    "RECON", worker.market.symbol,
                    f"local position {local_size:+.6g} != venue {venue_size:+.6g} — corrected to venue",
                )
            worker.on_position_event(pos)
        return stats

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL_S)
            if self.running:
                await self._reconcile_once()

    # ── account stream routing ───────────────────────────────────────────────
    async def _route_order_update(self, mtype: str, msg: dict) -> None:
        for market_id, orders in _per_market(msg.get("orders")):
            worker = self.workers.get(market_id)
            if worker:
                for order in orders:
                    worker.on_order_event(order)

    async def _route_trade_update(self, mtype: str, msg: dict) -> None:
        for market_id, trades in _per_market(msg.get("trades")):
            worker = self.workers.get(market_id)
            if worker:
                for trade in trades:
                    # Only count our fills (channel is account-scoped already).
                    worker.on_trade_event(trade)

    async def _route_account_update(self, mtype: str, msg: dict) -> None:
        for market_id, positions in _per_market(msg.get("positions")):
            worker = self.workers.get(market_id)
            if worker:
                for pos in positions:
                    worker.on_position_event(pos)
        stats = msg.get("stats") or msg.get("account_stats")
        if isinstance(stats, dict):
            self.risk.on_account_stats(stats)

    # ── live settings (dashboard "Fleet Settings" panel) ─────────────────────
    def config_dict(self) -> dict:
        return {
            "strategy": self.cfg.strategy,
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
        """Apply dashboard settings live to the fleet config and every enabled
        worker. Overrides per-market presets until restart. Returns the
        effective config."""
        cfg = self.cfg

        def _num(key, lo, hi, cast=float):
            if key not in settings or settings[key] in (None, ""):
                return None
            value = cast(settings[key])
            if not (lo <= value <= hi):
                raise ValueError(f"{key} must be between {lo} and {hi}")
            return value

        from .strategies import STRATEGIES

        strategy = settings.get("strategy")
        if strategy is not None and strategy != "":
            strategy = str(strategy).lower()
            if strategy not in STRATEGIES:
                raise ValueError(f"strategy must be one of {', '.join(STRATEGIES)}")
        else:
            strategy = None

        order_size = _num("order_size_usd", 10, 100_000)
        # negative spread is legal in mid mode (tread.fi scale −50..+50bps)
        spread = _num("spread_bps", -50, 500)
        requote = _num("requote_bps", 0.0, 100)
        refresh = _num("refresh_ms", 1_000, 300_000, int)
        leverage = _num("leverage", 1, 50, int)
        max_markets = _num("max_concurrent_markets", 1, 200, int)
        daily_loss = _num("daily_loss_usd", 1, 1_000_000)
        max_hold = _num("max_hold_s", 10, 3600)
        adverse_stop = _num("adverse_stop_bps", 1, 500)

        if strategy is not None:
            cfg.strategy = strategy
            activity.ok("CTRL", "", f"strategy switched to '{strategy}' on all markets")
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
            if strategy is not None:
                worker.strategy = strategy
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

        # Push leverage to the venue only when actually trading.
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

    # ── dashboard state ──────────────────────────────────────────────────────
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
        """Cost per $1M routed — THE efficiency metric for this bot.
        Positive = you pay to route volume; negative = you get paid."""
        volume = sum(w.volume_usd for w in workers)
        net = (
            self.risk.collateral - self.start_collateral
            if self.risk.collateral is not None and self.start_collateral is not None
            else None
        )
        cost = None
        if net is not None and volume >= 100:  # need some volume for a meaningful ratio
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


def _per_market(payload) -> list[tuple[int, list[dict]]]:
    """Normalise `{market_id: [...]}` / `[...]` shapes from account channels."""
    out: list[tuple[int, list[dict]]] = []
    if isinstance(payload, dict):
        for key, items in payload.items():
            try:
                mid = int(key)
            except (TypeError, ValueError):
                continue
            if isinstance(items, dict):
                items = [items]
            out.append((mid, list(items or [])))
    elif isinstance(payload, list):
        by_mid: dict[int, list[dict]] = {}
        for item in payload:
            mid = item.get("market_index", item.get("market_id"))
            if mid is None:
                continue
            by_mid.setdefault(int(mid), []).append(item)
        out = list(by_mid.items())
    return out
