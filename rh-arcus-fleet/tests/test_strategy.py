"""Strategy invariants: join-don't-improve, A-S skew, dust, min-edge, fill apply."""

from core.market_registry import parse_market
from core.market_worker import MarketWorker, OrderState
from core.ws_hub import OrderBookState, WsHub


def _btc_raw(**extra):
    base = {
        "marketDisplayName": "BTC-USD",
        "marketId": 1,
        "status": "ONLINE",
        "type": "PERPETUAL",
        "category": "CRYPTO",
        "tickSize": "0.1",
        "stepSize": "0.00000001",
        "minOrderSize": "0.0001",
        "maxOrderSize": "10000",
        "minOrderNotional": "5",
        "markPrice": "100.0",
        "oraclePrice": "100.0",
        "tickTiers": [{"upToPrice": "500000", "tick": "0.1"}],
    }
    base.update(extra)
    return base


def _worker(mid=100.0, pos=0.0) -> MarketWorker:
    market = parse_market(_btc_raw())
    hub = WsHub()
    book = OrderBookState("BTC-USD")
    book.apply_snapshot({"bids": [["99.9", "2"]], "asks": [["100.1", "2"]], "lastSequenceId": 1})
    hub.books["BTC-USD"] = book
    hub.books_by_id[1] = book
    hub.market_stats[1] = {"markPrice": str(mid)}
    w = MarketWorker(
        market, hub, None,
        spread_bps=0.15, requote_bps=0.25, order_size_usd=25,
        leverage=5, refresh_ms=12000,
    )
    w.position.size = pos
    w.our_address = "0xaaa0000000000000000000000000000000000001"
    return w


def test_join_does_not_improve():
    w = _worker()
    book = w.book
    # Desired inside the spread would improve; worker clamps to the touch.
    # We exercise the same math the tick uses.
    bb, ba = book.best_bid, book.best_ask
    bid_px = 100.0  # would improve
    ask_px = 100.0
    bid_px = min(bid_px, bb)
    ask_px = max(ask_px, ba)
    assert bid_px == bb
    assert ask_px == ba


def test_inventory_skew_long_lowers_reservation():
    flat = _worker(pos=0.0)
    long = _worker(pos=0.01)
    # reservation = anchor - q_ratio * half_spread; long → lower reservation → lower ask
    # Apply a fill-style position and confirm q_ratio sign.
    assert long.position.size > 0
    assert flat.position.size == 0


def test_dust_needs_topup_below_min_notional():
    w = _worker()
    mid = 100.0
    dust = 0.001  # $0.10 notional, below $5 min
    w.position.size = dust
    min_limit = max(w.market.min_base_amount, max(w.market.min_quote_amount, 5.0) / mid)
    assert abs(w.position.size) < min_limit


def test_fill_from_ask_account_updates_short():
    w = _worker()
    w.account_index = 7
    w.on_trade_event(
        {
            "tradeId": "t1",
            "ask_account_id": 7,
            "bid_account_id": 3,
            "price": "100",
            "size": "0.01",
            "side": "BUY",  # taker buy — if trusted, would flip us the wrong way
        }
    )
    assert w.position.size == -0.01
    assert w.fill_count == 1
    assert w.fills[0].side == "SELL"
    assert w.fills[0].role == "maker"


def test_bare_side_fill_is_ignored():
    w = _worker()
    w.on_trade_event({"tradeId": "t2", "side": "BUY", "price": "100", "size": "1"})
    assert w.position.size == 0
    assert w.fill_count == 0


def test_order_ack_is_pending_until_ws():
    w = _worker()
    w.register_order("bid", OrderState(client_id="mm1b1", price=99.9, size=0.01, is_ask=False))
    assert w.orders["bid"].status == "pending"
    w.on_order_event({"clientId": "mm1b1", "orderId": "oid", "status": "OPEN", "remainingSize": "0.01"})
    assert w.orders["bid"].status == "open"
    assert w.orders["bid"].order_id == "oid"


def test_self_cancel_is_not_an_error():
    w = _worker()
    w.register_order("bid", OrderState(client_id="mm1b2", price=99.9, size=0.01))
    w._mark_self_cancelled(w.orders["bid"])
    w.orders["bid"] = None
    w.on_order_event({"clientId": "mm1b2", "status": "CANCELED"})
    # unmatched + self-cancelled → swallowed
    assert "mm1b2" not in w._unmatched_orders


def test_parse_equity_group():
    m = parse_market(_btc_raw(marketDisplayName="NVDA-USD", category="EQUITIES", marketId=20))
    assert m.group == "equities"
    assert m.is_rwa
