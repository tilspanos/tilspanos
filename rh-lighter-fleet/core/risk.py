"""Fleet risk manager: margin headroom, daily loss halt, order-cap sweeps.

The risk manager PROTECTS the fleet — it must never become the reason zero
orders go out. It only intervenes on hard, observable conditions.
"""

from __future__ import annotations

import time

from .config import MAX_ACTIVE_ORDERS_PER_ACCOUNT
from .logging_utils import activity

ORDER_CAP_SWEEP_THRESHOLD = 900  # sweep stale orders before hitting 1500/account
MARGIN_PAUSE_FRACTION = 0.15     # pause lowest-priority markets under 15% headroom


class RiskManager:
    def __init__(self, daily_loss_usd: float) -> None:
        self.daily_loss_usd = daily_loss_usd
        self.day_start = time.time()
        self.halted = False
        self.halt_reason = ""
        # account stats fed from the WS account stream
        self.collateral: float | None = None
        self.available_balance: float | None = None
        self.margin_used_fraction: float | None = None
        self.open_order_count: int = 0

    def on_account_stats(self, stats: dict) -> None:
        self.collateral = _f(stats.get("collateral") or stats.get("total_asset_value"))
        self.available_balance = _f(stats.get("available_balance") or stats.get("cross_asset_value"))
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
        """Returns True when the fleet must HALT + flatten."""
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
                f"{self.open_order_count} active orders (cap {MAX_ACTIVE_ORDERS_PER_ACCOUNT}) — global stale sweep",
            )
            return True
        return False


def _f(v) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None
