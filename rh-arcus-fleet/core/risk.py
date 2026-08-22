"""Fleet risk manager: margin headroom, daily loss halt, order-cap sweeps.

Protects the fleet — must never become the reason zero orders go out.
"""

from __future__ import annotations

import time

from .config import ORDER_POOL_START
from .logging_utils import activity

ORDER_CAP_SWEEP_THRESHOLD = 900
MARGIN_PAUSE_FRACTION = 0.15


class RiskManager:
    def __init__(self, daily_loss_usd: float) -> None:
        self.daily_loss_usd = daily_loss_usd
        self.day_start = time.time()
        self.halted = False
        self.halt_reason = ""
        self.collateral: float | None = None
        self.available_balance: float | None = None
        self.margin_used_fraction: float | None = None
        self.open_order_count: int = 0

    def on_account_stats(self, stats: dict) -> None:
        # Balance = TRUE equity only (REST /v1/account carries it).
        # Never fall back to netQuoteBalance: that is cash accounting and is
        # inflated by short-sale proceeds / deflated by long purchases —
        # opening a $50 short made the "balance" jump +$50. Messages without
        # an equity field (e.g. the WS account channel) leave it untouched.
        equity = _f(stats.get("equity"))
        if equity is not None:
            self.collateral = equity
        free = _f(stats.get("freeCollateral") or stats.get("available_balance"))
        if free is not None:
            self.available_balance = free
        if self.collateral and self.available_balance is not None and self.collateral > 0:
            self.margin_used_fraction = 1 - (self.available_balance / self.collateral)

    def roll_day(self) -> None:
        if time.time() - self.day_start > 86_400:
            self.day_start = time.time()
            if self.halted and self.halt_reason == "daily-loss":
                self.halted = False
                self.halt_reason = ""
                activity.ok("RISK", "", "daily loss window rolled — fleet re-enabled")

    def check_daily_loss(self, fleet_pnl: float) -> bool:
        self.roll_day()
        if fleet_pnl <= -abs(self.daily_loss_usd) and not self.halted:
            self.halted = True
            self.halt_reason = "daily-loss"
            activity.err(
                "RISK", "",
                f"DAILY LOSS LIMIT hit ({fleet_pnl:.2f} <= -{self.daily_loss_usd}) — HALT + FLATTEN ALL",
            )
            return True
        return False

    def margin_headroom_low(self) -> bool:
        if self.margin_used_fraction is None:
            return False
        headroom = 1 - self.margin_used_fraction
        if headroom < MARGIN_PAUSE_FRACTION:
            activity.warn(
                "RISK", "",
                f"margin headroom {headroom:.0%} < {MARGIN_PAUSE_FRACTION:.0%} — pausing lowest-priority markets",
            )
            return True
        return False

    def order_cap_pressure(self) -> bool:
        if self.open_order_count > ORDER_CAP_SWEEP_THRESHOLD:
            activity.warn(
                "RISK", "",
                f"{self.open_order_count} active orders (pool start {ORDER_POOL_START}) — global stale sweep",
            )
            return True
        return False


def _f(v) -> float | None:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None
