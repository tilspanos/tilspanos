"""Market auto-discovery against the production Arcus API.

Never hardcode a symbol: the registry is built at boot from GET /v1/markets
and re-synced every 5 minutes. New markets are hot-added by the orchestrator.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Awaitable, Callable

import aiohttp

from .config import BASE_URL
from .decimalx import D, snap

RESYNC_INTERVAL_S = 300

_GROUP_MAP = {
    "CRYPTO": "crypto",
    "EQUITIES": "equities",
    "EQUITY": "equities",
    "COMMODITIES": "commodities",
    "COMMODITY": "commodities",
    "INDICES": "indices",
    "INDEX": "indices",
}


@dataclass
class Market:
    symbol: str
    market_id: int
    market_type: str  # "perp"
    status: str
    category: str
    group: str
    tick_size: Decimal
    step_size: Decimal
    min_base_amount: float
    max_base_amount: float
    min_quote_amount: float
    tick_tiers: tuple[tuple[Decimal | None, Decimal], ...] = ()
    mark_price: float | None = None
    oracle_price: float | None = None
    last_trade_price: float | None = None
    daily_quote_volume: float = 0.0
    open_interest: float = 0.0
    funding_rate: float | None = None
    is_outside_rth: bool = False
    upper_bound: float | None = None
    lower_bound: float | None = None
    initial_margin_fraction: float = 1.0

    @property
    def min_tick(self) -> float:
        return float(self.tick_size)

    @property
    def min_size_step(self) -> float:
        return float(self.step_size)

    @property
    def is_rwa(self) -> bool:
        return self.group != "crypto"

    def tick_at(self, price: float) -> Decimal:
        px = D(price)
        if not self.tick_tiers:
            return self.tick_size
        for up_to, tick in self.tick_tiers:
            if up_to is None or px <= up_to:
                return tick
        return self.tick_tiers[-1][1]

    def round_price(self, human: float, side: str | None = None) -> float:
        tick = self.tick_at(human)
        mode = "nearest"
        if side in ("bid", "BUY"):
            mode = "down"
        elif side in ("ask", "SELL"):
            mode = "up"
        return float(snap(D(human), tick, mode))

    def round_size(self, human: float) -> float:
        qty = float(snap(D(human), self.step_size, "down"))
        if qty < self.min_base_amount:
            return 0.0
        if self.max_base_amount > 0:
            qty = min(qty, self.max_base_amount)
        return qty

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "market_id": self.market_id,
            "market_type": self.market_type,
            "group": self.group,
            "status": self.status,
            "tick_size": str(self.tick_size),
            "step_size": str(self.step_size),
            "min_base_amount": self.min_base_amount,
            "min_quote_amount": self.min_quote_amount,
            "mark_price": self.mark_price,
            "last_trade_price": self.last_trade_price,
            "daily_quote_volume": self.daily_quote_volume,
            "open_interest": self.open_interest,
            "is_outside_rth": self.is_outside_rth,
        }


def parse_market(raw: dict) -> Market:
    tiers = tuple(
        (D(t["upToPrice"]) if t.get("upToPrice") else None, D(t["tick"]))
        for t in (raw.get("tickTiers") or [])
    )
    category = str(raw.get("category") or "CRYPTO")
    return Market(
        symbol=raw.get("marketDisplayName") or raw.get("ticker") or str(raw["marketId"]),
        market_id=int(raw["marketId"]),
        market_type="perp" if str(raw.get("type") or "PERPETUAL").upper() == "PERPETUAL" else str(raw.get("type")),
        status=str(raw.get("status") or ""),
        category=category,
        group=_GROUP_MAP.get(category.upper(), category.lower()),
        tick_size=D(raw["tickSize"]),
        step_size=D(raw["stepSize"]),
        min_base_amount=float(raw.get("minOrderSize") or raw["stepSize"]),
        max_base_amount=float(raw.get("maxOrderSize") or 0),
        min_quote_amount=float(raw.get("minOrderNotional") or 0),
        tick_tiers=tiers,
        mark_price=_f(raw.get("markPrice")),
        oracle_price=_f(raw.get("oraclePrice")),
        last_trade_price=_f(raw.get("lastTradePrice")),
        daily_quote_volume=_f(raw.get("volume24hNotional")) or 0.0,
        open_interest=_f(raw.get("openInterest")) or 0.0,
        funding_rate=_f(raw.get("fundingRate")),
        is_outside_rth=bool(raw.get("isOutsideRth")),
        upper_bound=_f(raw.get("upperTradingBound")),
        lower_bound=_f(raw.get("lowerTradingBound")),
        initial_margin_fraction=float(raw.get("initialMarginFraction") or 1),
    )


class MarketRegistry:
    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.markets: dict[int, Market] = {}
        self.by_symbol: dict[str, Market] = {}
        self.last_sync_ts: float = 0.0
        self.on_new_market: Callable[[Market], Awaitable[None]] | None = None
        self.on_sync: Callable[[], Awaitable[None]] | None = None

    @classmethod
    async def sync(cls, base_url: str = BASE_URL) -> "MarketRegistry":
        reg = cls(base_url)
        await reg.refresh()
        return reg

    async def refresh(self) -> list[Market]:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/v1/markets") as resp:
                data = await resp.json()
        rows = data.get("markets") or []
        new_markets: list[Market] = []
        seen: dict[int, Market] = {}
        for raw in rows:
            market = parse_market(raw)
            if market.status.upper() not in ("ONLINE", "ACTIVE"):
                # Keep OFFLINE names visible but not quoteable.
                if market.status.upper() == "OFFLINE":
                    seen[market.market_id] = market
                continue
            seen[market.market_id] = market
            if market.market_id not in self.markets:
                new_markets.append(market)
        # Preserve previously-known markets that vanished this cycle? No —
        # venue list is authoritative.
        self.markets = seen
        self.by_symbol = {m.symbol.upper(): m for m in seen.values()}
        self.last_sync_ts = time.time()
        if self.on_sync is not None:
            await self.on_sync()
        return new_markets

    async def run_resync_loop(self, interval_s: int = RESYNC_INTERVAL_S) -> None:
        while True:
            import asyncio

            await asyncio.sleep(interval_s)
            try:
                new = await self.refresh()
            except Exception:
                continue
            if new and self.on_new_market:
                for market in new:
                    await self.on_new_market(market)

    @property
    def all(self) -> list[Market]:
        return sorted(self.markets.values(), key=lambda m: m.market_id)

    @property
    def online(self) -> list[Market]:
        return [m for m in self.all if m.status.upper() in ("ONLINE", "ACTIVE")]

    @property
    def perps(self) -> list[Market]:
        return [m for m in self.online if m.market_type == "perp"]

    def group(self, name: str) -> list[Market]:
        name = name.lower()
        if name == "all":
            return self.online
        if name in ("stocks", "equity"):
            name = "equities"
        if name in ("commodity",):
            name = "commodities"
        if name in ("index",):
            name = "indices"
        return [m for m in self.online if m.group == name]

    def get(self, key: str | int) -> Market:
        if isinstance(key, int):
            return self.markets[key]
        return self.by_symbol[key.upper()]

    def resolve_groups(self, groups: list[str]) -> list[Market]:
        out: dict[int, Market] = {}
        for g in groups:
            for m in self.group(g):
                out[m.market_id] = m
        return sorted(out.values(), key=lambda m: m.market_id)


def _f(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
