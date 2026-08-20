"""Never trust a bare `side` field — these cases are the day-one contract."""

from core.fills import derive_fill_side

OUR = "0xaaa0000000000000000000000000000000000001"
OTHER = "0xbbb0000000000000000000000000000000000002"


def test_ask_account_id_means_we_sold():
    got = derive_fill_side(
        {"ask_account_id": 7, "bid_account_id": 3, "side": "BUY", "price": "1", "size": "1"},
        our_account_index=7,
    )
    assert got is not None
    assert got.side == "SELL"
    assert got.source == "ask_account_id"


def test_bid_account_id_means_we_bought():
    got = derive_fill_side(
        {"askAccountId": 3, "bidAccountId": 7, "side": "SELL"},
        our_account_index=7,
    )
    assert got is not None and got.side == "BUY"


def test_not_our_account_index_is_ignored():
    assert (
        derive_fill_side(
            {"ask_account_id": 1, "bid_account_id": 2, "side": "BUY"},
            our_account_index=9,
        )
        is None
    )


def test_maker_of_taker_buy_is_sell():
    # Public trades: `side` is the TAKER side.
    got = derive_fill_side(
        {"side": "BUY", "makerAddress": OUR, "takerAddress": OTHER, "price": "10", "size": "1"},
        our_address=OUR,
    )
    assert got is not None
    assert got.side == "SELL"
    assert got.role == "maker"


def test_taker_buy_is_buy():
    got = derive_fill_side(
        {"side": "BUY", "makerAddress": OTHER, "takerAddress": OUR},
        our_address=OUR,
    )
    assert got is not None
    assert got.side == "BUY"
    assert got.role == "taker"


def test_bare_side_is_never_trusted():
    assert derive_fill_side({"side": "BUY", "price": "1", "size": "1"}, our_address=OUR) is None
    assert derive_fill_side({"orderSide": "SELL", "fillSize": "1"}, our_address=OUR) is None


def test_resting_order_match_without_addresses():
    got = derive_fill_side(
        {"orderId": "ord-1", "clientId": "mm1b1", "side": "SELL"},  # lying side
        our_order_id="ord-1",
        our_resting_side="BUY",
    )
    assert got is not None
    assert got.side == "BUY"
    assert got.source == "resting_order_id"


def test_foreign_fill_by_address_rejected():
    assert (
        derive_fill_side(
            {"side": "BUY", "makerAddress": OTHER, "takerAddress": OTHER},
            our_address=OUR,
        )
        is None
    )
