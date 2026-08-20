"""Watchdogs — the fleet must NEVER fail silently.

Deliberate no-quote decisions (vol breaker, min-edge, contrarian gate,
stale book, RWA off-hours) count as liveness via ``record_veto``, not failure.
Healthy resting quotes also count as activity.
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


def silent_for_s(now: float, last_send_ts: float, last_veto_ts: float) -> float:
    return now - max(last_send_ts, last_veto_ts)


def should_crash_on_silence(
    *,
    running: bool,
    paused: bool,
    has_active_workers: bool,
    has_resting: bool,
    silent_for: float,
    crash_after_s: float = NO_ORDER_CRASH_S,
) -> bool:
    """Pure decision used by the loop and by unit tests."""
    if not running or paused or not has_active_workers or has_resting:
        return False
    return silent_for > crash_after_s


class Watchdog:
    def __init__(self, fleet) -> None:
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
        await fleet.hub.reconnect_stale()

        if not fleet.running or fleet.paused or not fleet.active_workers():
            if fleet.execution:
                fleet.execution.last_send_ts = max(fleet.execution.last_send_ts, now - 1)
            return

        has_resting = any(
            o is not None and o.status in ("pending", "open")
            for w in fleet.active_workers()
            for o in w.orders.values()
        )
        if has_resting and fleet.execution:
            fleet.execution.last_send_ts = max(fleet.execution.last_send_ts, now - 1)

        silent_for = silent_for_s(
            now, fleet.execution.last_send_ts, fleet.execution.last_veto_ts
        )
        if should_crash_on_silence(
            running=fleet.running,
            paused=fleet.paused,
            has_active_workers=bool(fleet.active_workers()),
            has_resting=has_resting,
            silent_for=silent_for,
        ):
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

        for worker in fleet.active_workers():
            if now - worker.last_fill_ts > IDLE_MARKET_S and now - worker.last_tick_ts < 60:
                activity.warn(
                    "WATCH", worker.market.symbol,
                    f"no fills for {IDLE_MARKET_S // 60} min — restarting worker",
                )
                await fleet.restart_worker(worker.market.market_id)
                worker.last_fill_ts = now

        workers = fleet.active_workers()
        if workers and all(now - w.last_fill_ts > IDLE_FLEET_S for w in workers):
            activity.warn("WATCH", "", "fleet idle 15 min — restarting all workers")
            await fleet.restart_all_workers()
            for w in workers:
                w.last_fill_ts = now
