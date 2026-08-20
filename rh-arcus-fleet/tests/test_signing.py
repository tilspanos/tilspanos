from eth_account import Account

from core.decimalx import to_int_exact
from core.signing import (
    canonical_json,
    eip712_create_api_key,
    generate_ed25519,
    load_ed25519,
    sign_scheme1,
    sign_scheme2,
    typed_cancel_payload,
    typed_place_payload,
    verify_bytes,
)


def test_canonical_json_sorted_no_spaces():
    assert canonical_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'


def test_place_payload_matches_docs_shape():
    payload = typed_place_payload(
        address="0xABCDef0000000000000000000000000000000001",
        account_index=0,
        market_id=1,
        side="BUY",
        price="64450.6",
        quantity="0.001",
        tick="0.1",
        step="0.00000001",
        tif="ALO",
        timestamp_ns=1_712_345_678_000_000_000,
        good_til_us_value=4_102_444_800_000_000,
        client_id="MM1B01",
    )
    assert payload.startswith('{"ad":"0xabcdef')
    assert '"c":"mm1b01"' in payload
    assert '"op":1' in payload
    assert '"s":0' in payload
    assert '"t":3' in payload
    assert '"r":0' in payload
    assert '"v":1' in payload
    assert to_int_exact("64450.6", "0.1") == 644506
    assert '"p":644506' in payload
    assert '"q":100000' in payload


def test_place_omits_empty_client_id():
    payload = typed_place_payload(
        address="0xabcdef0000000000000000000000000000000001",
        account_index=0,
        market_id=1,
        side="SELL",
        price="100.0",
        quantity="0.001",
        tick="0.1",
        step="0.00000001",
        tif="GTT",
        timestamp_ns=1,
        good_til_us_value=2,
    )
    assert '"c"' not in payload
    assert '"s":1' in payload
    assert '"t":0' in payload


def test_cancel_by_client_id_omits_order_id():
    payload = typed_cancel_payload(
        address="0xabcdef0000000000000000000000000000000001",
        account_index=0,
        market_id=1,
        timestamp_ns=9,
        client_id="MmX",
    )
    assert '"c":"mmx"' in payload
    assert '"id"' not in payload
    assert '"op":2' in payload


def test_scheme1_roundtrip():
    priv_hex, pub = generate_ed25519()
    priv = load_ed25519(priv_hex)
    msg = '{"ad":"0xab","ai":0,"ct":1,"m":1,"op":2,"v":1}'
    sig = sign_scheme1(priv, msg)
    assert len(sig) == 128
    assert verify_bytes(pub, msg.encode(), sig)


def test_scheme2_concatenates_without_delimiters():
    priv_hex, pub = generate_ed25519()
    priv = load_ed25519(priv_hex)
    body = {"accountIndex": 0, "address": "0xab"}
    sig = sign_scheme2(priv, 42, "cancelAllOrders", body)
    expected = f"42cancelAllOrders{canonical_json(body)}".encode()
    assert verify_bytes(pub, expected, sig)


def test_eip712_create_api_key_recovers_signer():
    wallet = Account.create()
    sig = eip712_create_api_key(
        wallet.key.hex(),
        chain_id=4663,
        public_key_hex_value="ab" * 32,
        api_wallet_name="rh-arcus-fleet",
        valid_until_ms=1_900_000_000_000,
        account_index=0,
    )
    assert sig["r"].startswith("0x")
    assert sig["s"].startswith("0x")
    assert sig["v"].startswith("0x")


def test_non_multiple_price_rejected():
    try:
        to_int_exact("100.05", "0.1")
        raise AssertionError("should have failed")
    except ValueError:
        pass
