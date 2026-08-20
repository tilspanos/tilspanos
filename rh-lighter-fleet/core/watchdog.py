"""Watchdogs — the fleet must NEVER fail silently.

| Watchdog     | Trigger                            | Action                          |
|--------------|------------------------------------|---------------------------------|
| No-order     | 60s with zero sendTx while running | CRASH + dump last 20 vetoes     |
| WS stale     | 30s silence on a connection        | Force reconnect that connection |
| Idle market  | 5 min no fills on one market       | Restart that worker             |
| Idle fleet   | 15 min no fills fleet-wide         | Restart all (unless paused)     |
"""

from __future__ import annotations

import asyncio
import time

from .logging_utils import activity

NO_ORDER_CRASH_S = 60
IDLE_MARKET_S = 300
IDLE_FLEET_S = 900
CHECK_INTERVAL_S = 5


class WatchdogCrash(SystemExit):
    pass


class Watchdog:
    def __init__(self, fleet) -> None:  # fleet: FleetOrchestrator (circular-safe)
        self.fleet = fleet
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="watchdog")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(CHECK_INTERVAL_S)
            try:
                await self._check()
            except WatchdogCrash:
                raise
            except Exception as exc:
                activity.warn("WATCH", "", f"watchdog check error: {exc}")

    async def _check(self) -> None:
        fleet = self.fleet
        now = time.time()

        # 1. WS stale → reconnect that channel.
        await fleet.hub.reconnect_stale()

        if not fleet.running or fleet.paused or not fleet.active_workers():
            # Paused, stopped, or user has no markets enabled (opt-in mode):
            # keep the send-clock fresh so we don't crash the moment quoting resumes.
            fleet.execution.last_send_ts = max(fleet.execution.last_send_ts, now - 1)
            return

        # Healthy resting quotes count as activity: in a calm market the
        # right move is to NOT touch orders, which sends zero tx by design.
        has_resting = any(
            o is not None and o.status in ("pending", "open")
            for w in fleet.active_workers()
            for o in w.orders.values()
        )
        if has_resting:
            fleet.execution.last_send_ts = max(fleet.execution.last_send_ts, now - 1)

        # 2. No-order watchdog: zero sendTx AND zero resting quotes for 60s
        #    while live → the execution path is actually broken → CRASH loudly.
        silent_for = now - fleet.execution.last_send_ts
        if silent_for > NO_ORDER_CRASH_S:
            vetoes = fleet.execution.recent_vetoes(20)
            activity.err(
                "WATCH", "",
                f"NO ORDERS SENT for {int(silent_for)}s — crashing. Last veto reasons:",
            )
            for v in vetoes or ["<no vetoes recorded — execution path is broken>"]:
                activity.err("WATCH", "", f"  veto: {v}")
            raise WatchdogCrash(
                f"watchdog: zero sendTx for {int(silent_for)}s — see veto dump above"
            )

        # 3. Idle market: no fills for 5 min → restart that worker.
        for worker in fleet.active_workers():
            if now - worker.last_fill_ts > IDLE_MARKET_S and now - worker.last_tick_ts < 60:
                activity.warn(
                    "WATCH", worker.market.symbol,
                    f"no fills for {IDLE_MARKET_S // 60} min — restarting worker",
                )
                await fleet.restart_worker(worker.market.market_id)
                worker.last_fill_ts = now  # avoid immediate re-trigger

        # 4. Idle fleet: no fills anywhere for 15 min → restart everything.
        workers = fleet.active_workers()
        if workers and all(now - w.last_fill_ts > IDLE_FLEET_S for w in workers):
            activity.warn("WATCH", "", "fleet idle 15 min — restarting all workers")
            await fleet.restart_all_workers()
            for w in workers:
                w.last_fill_ts = now
