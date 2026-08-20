"""WebSocket hub — one market-data connection + one authenticated account
connection against wss://api.rh.lighter.xyz/stream.

Design constraints from docs/websocket.md:
  * 255 connections/IP, 500 subscriptions/connection → 66 order_book subs +
    market_stats/all fit comfortably on ONE public connection.
  * keepalive: some client message at least every 2 minutes → we ping every 60s.
  * account state fleet-wide comes from account_all / account_all_orders /
    account_all_trades on ONE authenticated connection.

Message shapes (verified live against production):
  {"type":"connected", "session_id": ...}
  {"type":"subscribed/order_book", "channel":"order_book:1", "order_book":{"asks":[{"price","size"}],"bids":[...]}}
  {"type":"update/order_book",     "channel":"order_book:1", ...deltas, size "0" removes a level}
  {"type":"subscribed/market_stats","channel":"market_stats:all","market_stats":{"0":{...}}}
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable

import aiohttp

from .config import WS_URL
from .logging_utils import activity

# Ping must be MORE frequent than the staleness threshold: the venue answers
# app-level pings with a "pong" TEXT message, so an idle-but-healthy account
# stream stays visibly fresh instead of tripping false stale reconnects.
PING_INTERVAL_S = 20
STALE_AFTER_S = 45
RECONNECT_BASE_DELAY_S = 1.0
RECONNECT_MAX_DELAY_S = 30.0

Handler = Callable[[str, dict], Awaitable[None]]


class BookSide:
    """Price → size map maintained from snapshot + deltas."""

    def __init__(self, is_ask: bool) -> None:
        self.is_ask = is_ask
        self.levels: dict[float, float] = {}

    def apply(self, entries: list[dict], snapshot: bool) -> None:
        if snapshot:
            self.levels.clear()
        for e in entries:
            price = float(e["price"])
            size = float(e["size"])
            if size <= 0:
                self.levels.pop(price, None)
            else:
                self.levels[price] = size

    def best(self) -> float | None:
        if not self.levels:
            return None
        return min(self.levels) if self.is_ask else max(self.levels)

    def depth_usd(self, n: int = 5) -> float:
        prices = sorted(self.levels, reverse=not self.is_ask)[:n]
        return sum(p * self.levels[p] for p in prices)


class OrderBookState:
    def __init__(self, market_id: int) -> None:
        self.market_id = market_id
        self.bids = BookSide(is_ask=False)
        self.asks = BookSide(is_ask=True)
        self.last_update_ts: float = 0.0
        self.offset: int = 0

    @property
    def best_bid(self) -> float | None:
        return self.bids.best()

    @property
    def best_ask(self) -> float | None:
        return self.asks.best()

    @property
    def mid(self) -> float | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    @property
    def age_ms(self) -> float:
        if not self.last_update_ts:
            return float("inf")
        return (time.time() - self.last_update_ts) * 1000

    def apply(self, payload: dict, snapshot: bool) -> None:
        book = payload.get("order_book") or {}
        self.bids.apply(book.get("bids") or [], snapshot)
        self.asks.apply(book.get("asks") or [], snapshot)
        self.offset = payload.get("offset", self.offset)
        self.last_update_ts = time.time()


class WsConnection:
    """One resilient WS connection with auto-resubscribe and staleness watch."""

    def __init__(self, name: str, hub: "WsHub") -> None:
        self.name = name
        self.hub = hub
        self.channels: set[str] = set()
        self.auth_token: str | None = None
        self.last_msg_ts: float = 0.0
        self.connected = False
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task | None = None
        self.reconnect_count = 0
        self._readonly_fallback = False

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=f"ws-{self.name}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self.connected = False

    async def subscribe(self, channel: str) -> None:
        self.channels.add(channel)
        if self._ws is not None and not self._ws.closed:
            await self._send_subscribe(channel)

    async def _send_subscribe(self, channel: str) -> None:
        msg: dict = {"type": "subscribe", "channel": channel}
        if self.auth_token:
            msg["auth"] = self.auth_token
        await self._ws.send_json(msg)

    @property
    def stale(self) -> bool:
        return self.connected and (time.time() - self.last_msg_ts) > STALE_AFTER_S

    async def force_reconnect(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    def _url(self) -> str:
        url = self.hub.url
        if self._readonly_fallback and "readonly=true" not in url:
            url += ("&" if "?" in url else "?") + "readonly=true"
        return url

    async def _run(self) -> None:
        delay = RECONNECT_BASE_DELAY_S
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self._url(), heartbeat=PING_INTERVAL_S) as ws:
                        self._ws = ws
                        self.connected = True
                        self.last_msg_ts = time.time()
                        delay = RECONNECT_BASE_DELAY_S
                        activity.ok("WS", "", f"{self.name} connected ({len(self.channels)} subs)")
                        for channel in sorted(self.channels):
                            await self._send_subscribe(channel)
                        ping_task = asyncio.create_task(self._keepalive(ws))
                        try:
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    self.last_msg_ts = time.time()
                                    await self.hub.dispatch(json.loads(msg.data))
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        finally:
                            ping_task.cancel()
                            try:
                                await ping_task
                            except (asyncio.CancelledError, Exception):
                                pass
            except asyncio.CancelledError:
                raise
            except aiohttp.WSServerHandshakeError as exc:
                # Venue-restricted regions reject the trading stream; the
                # read-only stream still serves market data (no tx over WS).
                if not self._readonly_fallback:
                    self._readonly_fallback = True
                    reason = "region restricted" if exc.status in (400, 403) else f"status {exc.status}"
                    activity.warn(
                        "WS", "",
                        f"{self.name} handshake rejected ({reason}) — "
                        "falling back to read-only stream",
                    )
                else:
                    activity.warn("WS", "", f"{self.name} handshake error: {exc}")
            except Exception as exc:
                activity.warn("WS", "", f"{self.name} error: {type(exc).__name__}: {exc}")
            self.connected = False
            self.reconnect_count += 1
            activity.warn("WS", "", f"{self.name} reconnecting in {delay:.1f}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    async def _keepalive(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # Docs: any client message at least every 2 minutes keeps the session.
        while not ws.closed:
            await asyncio.sleep(PING_INTERVAL_S)
            try:
                await ws.send_json({"type": "ping"})
            except Exception:
                return


class WsHub:
    """Routes production stream messages to the fleet."""

    def __init__(self, url: str = WS_URL) -> None:
        self.url = url
        self.books: dict[int, OrderBookState] = {}
        self.market_stats: dict[int, dict] = {}
        self.market_conn = WsConnection("market-data", self)
        self.account_conn = WsConnection("account", self)
        self.on_order_update: Handler | None = None
        self.on_trade_update: Handler | None = None
        self.on_account_update: Handler | None = None
        self.on_stats_update: Callable[[dict[int, dict]], None] | None = None

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def start_market_data(self, market_ids: list[int]) -> None:
        for mid in market_ids:
            self.books.setdefault(mid, OrderBookState(mid))
            await self.market_conn.subscribe(f"order_book/{mid}")
        await self.market_conn.subscribe("market_stats/all")
        self.market_conn.start()

    async def add_market(self, market_id: int) -> None:
        self.books.setdefault(market_id, OrderBookState(market_id))
        await self.market_conn.subscribe(f"order_book/{market_id}")

    async def start_account(self, account_index: int, auth_token: str) -> None:
        self.account_conn.auth_token = auth_token
        for channel in (
            f"account_all/{account_index}",
            f"account_all_orders/{account_index}",
            f"account_all_trades/{account_index}",
        ):
            await self.account_conn.subscribe(channel)
        self.account_conn.start()

    def refresh_auth(self, auth_token: str) -> None:
        self.account_conn.auth_token = auth_token

    async def stop(self) -> None:
        await self.market_conn.stop()
        await self.account_conn.stop()

    # ── dispatch ──────────────────────────────────────────────────────────────
    async def dispatch(self, msg: dict) -> None:
        mtype = msg.get("type", "")
        channel = msg.get("channel", "")
        if mtype in ("connected", "pong", "ping"):
            return
        if mtype.startswith("error"):
            activity.err("WS", "", f"stream error: {json.dumps(msg)[:300]}")
            return

        kind, _, suffix = channel.partition(":")
        snapshot = mtype.startswith("subscribed")

        if kind == "order_book":
            mid = int(suffix)
            book = self.books.setdefault(mid, OrderBookState(mid))
            book.apply(msg, snapshot)
        elif kind == "market_stats":
            stats = msg.get("market_stats") or {}
            if "market_id" in stats:  # single-market form
                stats = {str(stats["market_id"]): stats}
            for key, value in stats.items():
                self.market_stats[int(key)] = value
            if self.on_stats_update:
                self.on_stats_update(self.market_stats)
        elif kind == "account_all_orders" and self.on_order_update:
            await self.on_order_update(mtype, msg)
        elif kind == "account_all_trades" and self.on_trade_update:
            await self.on_trade_update(mtype, msg)
        elif kind in ("account_all", "user_stats") and self.on_account_update:
            await self.on_account_update(mtype, msg)

    # ── convenience views ────────────────────────────────────────────────────
    def stats_for(self, market_id: int) -> dict:
        return self.market_stats.get(market_id, {})

    def mark_price(self, market_id: int) -> float | None:
        v = self.stats_for(market_id).get("mark_price")
        return float(v) if v is not None else None

    def stale_connections(self) -> list[str]:
        return [c.name for c in (self.market_conn, self.account_conn) if c.stale]

    async def reconnect_stale(self) -> None:
        for conn in (self.market_conn, self.account_conn):
            if conn.stale:
                activity.warn("WS", "", f"{conn.name} stale >{STALE_AFTER_S}s — forcing reconnect")
                await conn.force_reconnect()
