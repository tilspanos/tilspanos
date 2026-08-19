"""Market auto-discovery against the production Robinhood Lighter API.

Never hardcode a symbol: the registry is built at boot from
GET /api/v1/orderBooks + GET /api/v1/orderBookDetails and re-synced every
5 minutes. New markets appearing on the venue are hot-added by the
orchestrator through the `on_new_market` callback.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import aiohttp

from .config import BASE_URL

RESYNC_INTERVAL_S = 300

# Symbols treated as memes / pre-IPO for preset purposes (grouping only —
# discovery itself is never filtered by symbol).
MEME_PREIPO_SYMBOLS = {"LIT", "CASHCAT", "ANTHROPIC", "ANSEM", "SKHY", "VVV", "SPCX", "CRWV"}
CRYPTO_PERP_MAX_ID = 9  # ids 0-9 are crypto perps on this deployment


@dataclass
class Market:
    symbol: str
    market_id: int
    market_type: str  # "perp" | "spot"
    status: str
    price_decimals: int
    size_decimals: int
    min_base_amount: float
    min_quote_amount: float
    order_quote_limit: float
    maker_fee: float
    taker_fee: float
    mark_price: float | None = None
    index_price: float | None = None
    last_trade_price: float | None = None
    daily_quote_volume: float = 0.0
    open_interest: float = 0.0
    group: str = "crypto"  # crypto | stocks | spot
    is_meme: bool = False

    @property
    def min_tick(self) -> float:
        return 10 ** (-self.price_decimals)

    @property
    def min_size_step(self) -> float:
        return 10 ** (-self.size_decimals)

    def to_price_int(self, human: float) -> int:
        return int(round(human * (10 ** self.price_decimals)))

    def to_size_int(self, human: float) -> int:
        return int(round(human * (10 ** self.size_decimals)))

    def from_price_int(self, raw: int) -> float:
        return raw / (10 ** self.price_decimals)

    def from_size_int(self, raw: int) -> float:
        return raw / (10 ** self.size_decimals)

    def round_price(self, human: float) -> float:
        return self.from_price_int(self.to_price_int(human))

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "market_id": self.market_id,
            "market_type": self.market_type,
            "group": self.group,
            "is_meme": self.is_meme,
            "status": self.status,
            "price_decimals": self.price_decimals,
            "size_decimals": self.size_decimals,
            "min_base_amount": self.min_base_amount,
            "min_quote_amount": self.min_quote_amount,
            "mark_price": self.mark_price,
            "last_trade_price": self.last_trade_price,
            "daily_quote_volume": self.daily_quote_volume,
            "open_interest": self.open_interest,
        }


def _classify(market_id: int, market_type: str, symbol: str) -> tuple[str, bool]:
    if market_type == "spot" or market_id >= 2048:
        return "spot", False
    is_meme = symbol.upper() in MEME_PREIPO_SYMBOLS
    if market_id <= CRYPTO_PERP_MAX_ID:
        return "crypto", is_meme
    return "stocks", is_meme


class MarketRegistry:
    """Live view of every active market on the production venue."""

    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.markets: dict[int, Market] = {}
        self.by_symbol: dict[str, Market] = {}
        self.last_sync_ts: float = 0.0
        self.on_new_market: Callable[[Market], Awaitable[None]] | None = None

    # ── discovery ────────────────────────────────────────────────────────────
    @classmethod
    async def sync(cls, base_url: str = BASE_URL) -> "MarketRegistry":
        reg = cls(base_url)
        await reg.refresh()
        return reg

    async def refresh(self) -> list[Market]:
        """Pull orderBooks + orderBookDetails, rebuild the registry.
        Returns markets that are new since the previous sync."""
        async with aiohttp.ClientSession() as session:
            books_task = session.get(f"{self.base_url}/api/v1/orderBooks")
            details_task = session.get(f"{self.base_url}/api/v1/orderBookDetails")
            books_resp, details_resp = await asyncio.gather(books_task, details_task)
            books_json = await books_resp.json()
            details_json = await details_resp.json()

        details_by_id: dict[int, dict] = {}
        for key in ("order_book_details", "spot_order_book_details"):
            for d in details_json.get(key) or []:
                details_by_id[d["market_id"]] = d

        new_markets: list[Market] = []
        seen: dict[int, Market] = {}
        for ob in books_json.get("order_books", []):
            if ob.get("status") != "active":
                continue
            mid = ob["market_id"]
            det = details_by_id.get(mid, {})
            group, is_meme = _classify(mid, ob.get("market_type", "perp"), ob["symbol"])
            market = Market(
                symbol=ob["symbol"],
                market_id=mid,
                market_type=ob.get("market_type", "perp"),
                status=ob["status"],
                price_decimals=int(ob["supported_price_decimals"]),
                size_decimals=int(ob["supported_size_decimals"]),
                min_base_amount=float(ob["min_base_amount"]),
                min_quote_amount=float(ob["min_quote_amount"]),
                order_quote_limit=float(ob.get("order_quote_limit", 0) or 0),
                maker_fee=float(ob.get("maker_fee", 0) or 0),
                taker_fee=float(ob.get("taker_fee", 0) or 0),
                mark_price=_f(det.get("mark_price")),
                index_price=_f(det.get("index_price")),
                last_trade_price=_f(det.get("last_trade_price")),
                daily_quote_volume=_f(det.get("daily_quote_token_volume")) or 0.0,
                open_interest=_f(det.get("open_interest")) or 0.0,
                group=group,
                is_meme=is_meme,
            )
            seen[mid] = market
            if mid not in self.markets:
                new_markets.append(market)

        self.markets = seen
        self.by_symbol = {m.symbol.upper(): m for m in seen.values()}
        self.last_sync_ts = time.time()
        return new_markets

    async def run_resync_loop(self, interval_s: int = RESYNC_INTERVAL_S) -> None:
        """Background loop: re-sync every 5 minutes, hot-add new markets."""
        while True:
            await asyncio.sleep(interval_s)
            try:
                new = await self.refresh()
            except Exception:
                continue  # transient network error; watchdog covers persistent ones
            if new and self.on_new_market:
                for market in new:
                    await self.on_new_market(market)

    # ── views ────────────────────────────────────────────────────────────────
    @property
    def all(self) -> list[Market]:
        return sorted(self.markets.values(), key=lambda m: m.market_id)

    @property
    def perps(self) -> list[Market]:
        return [m for m in self.all if m.market_type == "perp"]

    @property
    def spots(self) -> list[Market]:
        return [m for m in self.all if m.market_type != "perp"]

    def group(self, name: str) -> list[Market]:
        name = name.lower()
        if name == "all":
            return self.all
        if name in ("memes", "meme", "preipo"):
            return [m for m in self.all if m.is_meme]
        if name in ("majors", "crypto_majors"):
            return [m for m in self.all if m.group == "crypto" and not m.is_meme]
        return [m for m in self.all if m.group == name]

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
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
