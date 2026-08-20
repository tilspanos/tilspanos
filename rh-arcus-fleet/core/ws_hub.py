"""WebSocket hub — one multiplexed production socket.

Arcus multiplexes market-data channels and signed order RPCs on
wss://api.arcus.xyz/v1/ws. 202 ACK is never treated as an open order;
channel_data on `orders` / `userFills` is the source of truth.

Order-book discipline (docs): snapshot lastSequenceId is the last applied
delta; apply only strictly-greater continuous ids; a gap forces resubscribe.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Awaitable, Callable

import aiohttp

from .config import MAX_INFLIGHT_POSTS, MAX_WS_SUBS_PER_CONN, WS_URL
from .logging_utils import activity

PING_INTERVAL_S = 20
STALE_AFTER_S = 45
RECONNECT_BASE_DELAY_S = 1.0
RECONNECT_MAX_DELAY_S = 30.0

Handler = Callable[[str, dict], Awaitable[None] | None]


class BookSide:
    def __init__(self, is_ask: bool) -> None:
        self.is_ask = is_ask
        self.levels: dict[float, float] = {}

    def apply(self, entries: list, snapshot: bool) -> None:
        if snapshot:
            self.levels.clear()
        for e in entries or []:
            if isinstance(e, (list, tuple)) and len(e) >= 2:
                price, size = float(e[0]), float(e[1])
            else:
                price, size = float(e["price"]), float(e["size"])
            if size <= 0:
                self.levels.pop(price, None)
            else:
                self.levels[price] = size

    def best(self) -> float | None:
        if not self.levels:
            return None
        return min(self.levels) if self.is_ask else max(self.levels)

    def size_at(self, price: float) -> float:
        return self.levels.get(price, 0.0)


class OrderBookState:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids = BookSide(is_ask=False)
        self.asks = BookSide(is_ask=True)
        self.last_update_ts: float = 0.0
        self.last_sequence_id: int | None = None
        self.ready = False
        self.gap = False

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

    def apply_snapshot(self, contents: dict) -> None:
        self.bids.apply(contents.get("bids") or [], True)
        self.asks.apply(contents.get("asks") or [], True)
        seq = contents.get("lastSequenceId")
        self.last_sequence_id = int(seq) if seq is not None else None
        self.last_update_ts = time.time()
        self.ready = True
        self.gap = False

    def apply_delta(self, contents: dict) -> bool:
        """Return False if a sequence gap requires a fresh snapshot."""
        seq = contents.get("lastSequenceId")
        if not self.ready or self.last_sequence_id is None:
            self.gap = True
            return False
        if seq is not None:
            seq = int(seq)
            if seq <= self.last_sequence_id:
                return True
            if seq != self.last_sequence_id + 1:
                self.gap = True
                return False
            self.last_sequence_id = seq
        self.bids.apply(contents.get("bids") or [], False)
        self.asks.apply(contents.get("asks") or [], False)
        self.last_update_ts = time.time()
        return True


class WsHub:
    def __init__(self, url: str = WS_URL) -> None:
        self.url = url
        self.books: dict[str, OrderBookState] = {}
        self.books_by_id: dict[int, OrderBookState] = {}
        self.market_stats: dict[int, dict] = {}
        self.connected = False
        self.last_msg_ts: float = 0.0
        self.reconnect_count = 0
        self.on_order_update: Handler | None = None
        self.on_fill_update: Handler | None = None
        self.on_account_update: Handler | None = None
        self.on_position_update: Handler | None = None

        self._subs: dict[str, dict] = {}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task | None = None
        self._pending: dict[int, asyncio.Future] = {}
        self._next_id = 1
        self._post_sem = asyncio.Semaphore(MAX_INFLIGHT_POSTS)
        self._session: aiohttp.ClientSession | None = None

    @property
    def stale(self) -> bool:
        return self.connected and (time.time() - self.last_msg_ts) > STALE_AFTER_S

    def _alloc_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="ws-hub")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self.connected = False
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def subscribe(self, channel: str, sub_id: str | None = None, **extra: Any) -> None:
        if len(self._subs) >= MAX_WS_SUBS_PER_CONN:
            activity.warn("WS", "", f"subscription cap {MAX_WS_SUBS_PER_CONN} — skipping {channel}:{sub_id}")
            return
        key = f"{channel}:{sub_id or ''}"
        msg: dict = {"type": "subscribe", "channel": channel, **extra}
        if sub_id is not None:
            msg["id"] = sub_id
        self._subs[key] = msg
        if self._ws is not None and not self._ws.closed:
            await self._ws.send_json(msg)

    async def unsubscribe(self, channel: str, sub_id: str | None = None) -> None:
        key = f"{channel}:{sub_id or ''}"
        self._subs.pop(key, None)
        if self._ws is not None and not self._ws.closed:
            msg: dict = {"type": "unsubscribe", "channel": channel}
            if sub_id is not None:
                msg["id"] = sub_id
            await self._ws.send_json(msg)

    async def refresh_subscription(self, channel: str, sub_id: str | None = None) -> None:
        key = f"{channel}:{sub_id or ''}"
        msg = self._subs.get(key)
        if msg is None or self._ws is None or self._ws.closed:
            return
        unsub: dict = {"type": "unsubscribe", "channel": channel}
        if sub_id is not None:
            unsub["id"] = sub_id
        await self._ws.send_json(unsub)
        await self._ws.send_json(msg)

    async def start_market_data(self, symbols: list[str]) -> None:
        await self.subscribe("markets")
        await self.subscribe("oraclePrices")
        for symbol in symbols:
            self.books.setdefault(symbol, OrderBookState(symbol))
            await self.subscribe("l2OrderbookUpdates", symbol, nLevels=10)
        self.start()

    async def add_market(self, symbol: str, market_id: int | None = None) -> None:
        book = self.books.setdefault(symbol, OrderBookState(symbol))
        if market_id is not None:
            self.books_by_id[market_id] = book
        await self.subscribe("l2OrderbookUpdates", symbol, nLevels=10)

    async def start_account(self, address: str) -> None:
        for channel in ("account", "positions", "orders", "userFills"):
            await self.subscribe(channel, address)
        self.start()

    async def force_reconnect(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    async def reconnect_stale(self) -> None:
        if self.stale:
            activity.warn("WS", "", f"socket stale >{STALE_AFTER_S}s — forcing reconnect")
            await self.force_reconnect()

    async def _run(self) -> None:
        delay = RECONNECT_BASE_DELAY_S
        while True:
            try:
                self._session = aiohttp.ClientSession()
                async with self._session.ws_connect(self.url, heartbeat=PING_INTERVAL_S, max_msg_size=2**24) as ws:
                    self._ws = ws
                    self.connected = True
                    self.last_msg_ts = time.time()
                    delay = RECONNECT_BASE_DELAY_S
                    activity.ok("WS", "", f"connected ({len(self._subs)} subs)")
                    for msg in self._subs.values():
                        await ws.send_json(msg)
                    ping_task = asyncio.create_task(self._keepalive(ws))
                    try:
                        async for raw in ws:
                            if raw.type == aiohttp.WSMsgType.TEXT:
                                self.last_msg_ts = time.time()
                                await self.dispatch(json.loads(raw.data))
                            elif raw.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except (asyncio.CancelledError, Exception):
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                activity.warn("WS", "", f"error: {type(exc).__name__}: {exc}")
            finally:
                self.connected = False
                self._ws = None
                self._fail_pending(ConnectionError("websocket disconnected"))
                if self._session is not None:
                    await self._session.close()
                    self._session = None
            self.reconnect_count += 1
            activity.warn("WS", "", f"reconnecting in {delay:.1f}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    async def _keepalive(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while not ws.closed:
            await asyncio.sleep(PING_INTERVAL_S)
            try:
                await ws.send_json({"type": "ping"})
            except Exception:
                return

    def _fail_pending(self, exc: Exception) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    async def rpc(self, envelope: dict, timeout: float = 10.0) -> dict:
        if self._ws is None or self._ws.closed:
            raise ConnectionError("websocket not connected")
        rid = envelope.setdefault("id", self._alloc_id())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[rid] = fut
        kind = envelope.get("type")
        if kind == "post":
            await self._post_sem.acquire()
        try:
            await self._ws.send_json(envelope)
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(rid, None)
            if kind == "post":
                self._post_sem.release()

    async def post(
        self,
        method: str,
        payload: dict,
        *,
        api_key: str,
        timestamp_ns: int,
        signature: str,
        timeout: float = 10.0,
    ) -> dict:
        return await self.rpc(
            {
                "type": "post",
                "id": self._alloc_id(),
                "request": {
                    "type": method,
                    "payload": payload,
                    "apiKey": api_key,
                    "timestamp": str(timestamp_ns),
                    "signature": signature,
                },
            },
            timeout=timeout,
        )

    async def dispatch(self, msg: dict) -> None:
        mtype = msg.get("type", "")
        if mtype in ("connected", "pong", "ping", "subscribed", "unsubscribed"):
            return
        rid = msg.get("id")
        if isinstance(rid, int) and rid in self._pending and mtype not in ("channel_data", "subscribed"):
            fut = self._pending.get(rid)
            if fut and not fut.done():
                fut.set_result(msg)
            if mtype not in ("channel_data",):
                return
        if mtype.startswith("error"):
            activity.err("WS", "", f"stream error: {json.dumps(msg)[:300]}")
            return
        if mtype != "channel_data":
            return

        channel = msg.get("channel", "")
        contents = msg.get("contents")
        sub_id = msg.get("id") if not isinstance(msg.get("id"), int) else None
        # subscription id for per-market channels is the market symbol
        if channel in ("l2Orderbook", "l2OrderbookUpdates"):
            symbol = str(sub_id or (contents or {}).get("market") or "")
            if not symbol:
                return
            book = self.books.setdefault(symbol, OrderBookState(symbol))
            payload = contents if isinstance(contents, dict) else {}
            is_snapshot = channel == "l2Orderbook" or bool(payload.get("isSnapshot")) or book.last_sequence_id is None
            if is_snapshot and (payload.get("bids") or payload.get("asks") or payload.get("isSnapshot")):
                book.apply_snapshot(payload)
            else:
                if not book.apply_delta(payload):
                    activity.warn("WS", symbol, "book sequence gap — resubscribing for snapshot")
                    await self.refresh_subscription("l2OrderbookUpdates", symbol)
        elif channel in ("markets", "oraclePrices"):
            self._ingest_stats(contents if isinstance(contents, dict) else {})
        elif channel == "orders" and self.on_order_update:
            await _maybe(self.on_order_update, channel, msg)
        elif channel == "userFills" and self.on_fill_update:
            await _maybe(self.on_fill_update, channel, msg)
        elif channel == "account" and self.on_account_update:
            await _maybe(self.on_account_update, channel, msg)
        elif channel == "positions" and self.on_position_update:
            await _maybe(self.on_position_update, channel, msg)

    def _ingest_stats(self, contents: dict) -> None:
        markets = contents.get("markets") or contents.get("prices") or contents
        if not isinstance(markets, dict):
            return
        for key, value in markets.items():
            if not isinstance(value, dict):
                continue
            mid = value.get("marketId", key)
            try:
                mid_i = int(mid)
            except (TypeError, ValueError):
                continue
            prev = self.market_stats.get(mid_i, {})
            prev.update(value)
            self.market_stats[mid_i] = prev
            symbol = value.get("marketDisplayName") or prev.get("marketDisplayName")
            book = self.books.get(symbol) if symbol else None
            if book is not None:
                self.books_by_id[mid_i] = book

    def mark_price(self, market_id: int) -> float | None:
        v = self.market_stats.get(market_id, {}).get("markPrice")
        return float(v) if v not in (None, "") else None

    def oracle_price(self, market_id: int) -> float | None:
        v = self.market_stats.get(market_id, {}).get("oraclePrice")
        return float(v) if v not in (None, "") else None

    def book_for(self, symbol: str | None = None, market_id: int | None = None) -> OrderBookState | None:
        if symbol and symbol in self.books:
            return self.books[symbol]
        if market_id is not None:
            return self.books_by_id.get(market_id)
        return None


async def _maybe(handler: Handler, channel: str, msg: dict) -> None:
    res = handler(channel, msg)
    if asyncio.iscoroutine(res):
        await res
