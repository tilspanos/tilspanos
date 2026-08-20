from core.ws_hub import OrderBookState


def test_snapshot_then_delta():
    book = OrderBookState("BTC-USD")
    book.apply_snapshot({"bids": [["100", "1"]], "asks": [["101", "2"]], "lastSequenceId": 10})
    assert book.best_bid == 100.0
    assert book.best_ask == 101.0
    assert book.mid == 100.5
    ok = book.apply_delta({"bids": [["100", "0"]], "asks": [], "lastSequenceId": 11})
    assert ok
    assert book.best_bid is None


def test_sequence_gap_requires_resync():
    book = OrderBookState("BTC-USD")
    book.apply_snapshot({"bids": [["1", "1"]], "asks": [["2", "1"]], "lastSequenceId": 5})
    ok = book.apply_delta({"bids": [["1.1", "1"]], "lastSequenceId": 8})
    assert not ok
    assert book.gap


def test_stale_duplicate_delta_ignored():
    book = OrderBookState("BTC-USD")
    book.apply_snapshot({"bids": [["1", "1"]], "asks": [["2", "1"]], "lastSequenceId": 5})
    assert book.apply_delta({"lastSequenceId": 4, "bids": [["9", "1"]]})
    assert book.best_bid == 1.0
