"""Ed25519 request signing + EIP-712 API-key registration.

Two live schemes (docs.arcus.xyz/api-reference/authentication):

* Scheme 1 (place/cancel/modify, each batch element):
  ``ed25519(canonical typed payload)``. ``ct`` equals the nanosecond timestamp.
* Scheme 2 (cancelAllOrders, setLeverage, WS authenticate):
  ``ed25519(timestamp + action + canonical_json(body))``.
"""

from __future__ import annotations

import json
import time
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from eth_account import Account
from eth_account.messages import encode_typed_data

from .decimalx import D, to_int_exact

OP_PLACE = 1
OP_CANCEL = 2
OP_MODIFY = 3
SIDE_BUY = 0
SIDE_SELL = 1
TIF_GTT = 0
TIF_FOK = 1
TIF_IOC = 2
TIF_ALO = 3

TIF_CODE = {"GTT": TIF_GTT, "FOK": TIF_FOK, "IOC": TIF_IOC, "ALO": TIF_ALO}
SIDE_CODE = {"BUY": SIDE_BUY, "SELL": SIDE_SELL, "BID": SIDE_BUY, "ASK": SIDE_SELL}


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def now_ns() -> int:
    return time.time_ns()


def now_us() -> int:
    return time.time_ns() // 1_000


def good_til_us(days: float = 40.0) -> int:
    return now_us() + int(days * 86_400 * 1_000_000)


def load_ed25519(hex_key: str) -> Ed25519PrivateKey:
    raw = bytes.fromhex(hex_key.removeprefix("0x"))
    if len(raw) != 32:
        raise ValueError("Ed25519 private key must be 32 bytes (64 hex chars)")
    return Ed25519PrivateKey.from_private_bytes(raw)


def generate_ed25519() -> tuple[str, str]:
    priv = Ed25519PrivateKey.generate()
    return priv.private_bytes_raw().hex(), priv.public_key().public_bytes_raw().hex()


def public_key_hex(priv: Ed25519PrivateKey) -> str:
    return priv.public_key().public_bytes_raw().hex()


def sign_bytes(priv: Ed25519PrivateKey, message: bytes) -> str:
    return priv.sign(message).hex()


def verify_bytes(pub_hex: str, message: bytes, sig_hex: str) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_hex)).verify(
            bytes.fromhex(sig_hex), message
        )
        return True
    except Exception:
        return False


def sign_scheme1(priv: Ed25519PrivateKey, payload: str) -> str:
    return sign_bytes(priv, payload.encode())


def sign_scheme2(priv: Ed25519PrivateKey, timestamp_ns: int, action: str, body: dict[str, Any]) -> str:
    return sign_bytes(priv, f"{timestamp_ns}{action}{canonical_json(body)}".encode())


def auth_headers(api_key: str, timestamp_ns: int, signature: str) -> dict[str, str]:
    return {
        "X-API-Key": api_key,
        "X-Timestamp": str(timestamp_ns),
        "X-Signature": signature,
        "Content-Type": "application/json",
    }


def typed_place_payload(
    *,
    address: str,
    account_index: int,
    market_id: int,
    side: str,
    price,
    quantity,
    tick,
    step,
    tif: str,
    timestamp_ns: int,
    good_til_us_value: int,
    reduce_only: bool = False,
    client_id: str | None = None,
    op: int = OP_PLACE,
    order_id: str | None = None,
) -> str:
    obj: dict[str, Any] = {
        "ad": address.lower(),
        "ai": int(account_index),
        "ct": int(timestamp_ns),
        "g": int(good_til_us_value) * 1000,
        "m": int(market_id),
        "op": int(op),
        "p": to_int_exact(price, tick),
        "q": to_int_exact(quantity, step),
        "r": 1 if reduce_only else 0,
        "s": SIDE_CODE[side.upper()],
        "t": TIF_CODE[tif.upper()],
        "v": 1,
    }
    if client_id:
        obj["c"] = client_id.lower()
    if order_id and op in (OP_MODIFY, OP_CANCEL):
        obj["id"] = order_id
    return canonical_json(obj)


def typed_cancel_payload(
    *,
    address: str,
    account_index: int,
    market_id: int,
    timestamp_ns: int,
    order_id: str | None = None,
    client_id: str | None = None,
) -> str:
    if bool(order_id) == bool(client_id):
        raise ValueError("cancel must specify exactly one of order_id or client_id")
    obj: dict[str, Any] = {
        "ad": address.lower(),
        "ai": int(account_index),
        "ct": int(timestamp_ns),
        "m": int(market_id),
        "op": OP_CANCEL,
        "v": 1,
    }
    if client_id:
        obj["c"] = client_id.lower()
    if order_id:
        obj["id"] = order_id
    return canonical_json(obj)


# --- EIP-712 createApiKey ---------------------------------------------------

CREATE_API_KEY_DOMAIN_NAME = "Arcus API Key"
CREATE_API_KEY_TYPES = {
    "CreateApiKey": [
        {"name": "apiWalletName", "type": "string"},
        {"name": "apiWalletPublicKey", "type": "string"},
        {"name": "validUntil", "type": "uint256"},
        {"name": "accountIndex", "type": "uint8"},
    ]
}
CREATE_API_KEY_TYPES_INDEX0 = {
    "CreateApiKey": [
        {"name": "apiWalletName", "type": "string"},
        {"name": "apiWalletPublicKey", "type": "string"},
        {"name": "validUntil", "type": "uint256"},
    ]
}


def eip712_create_api_key(
    eth_private_key: str,
    *,
    chain_id: int,
    public_key_hex_value: str,
    api_wallet_name: str,
    valid_until_ms: int,
    account_index: int = 0,
) -> dict[str, str]:
    domain = {"name": CREATE_API_KEY_DOMAIN_NAME, "version": "1", "chainId": chain_id}
    if account_index == 0:
        types = CREATE_API_KEY_TYPES_INDEX0
        message = {
            "apiWalletName": api_wallet_name,
            "apiWalletPublicKey": public_key_hex_value,
            "validUntil": valid_until_ms,
        }
    else:
        types = CREATE_API_KEY_TYPES
        message = {
            "apiWalletName": api_wallet_name,
            "apiWalletPublicKey": public_key_hex_value,
            "validUntil": valid_until_ms,
            "accountIndex": account_index,
        }
    encoded = encode_typed_data(domain, types, message)
    signed = Account.from_key(eth_private_key).sign_message(encoded)
    return {"r": hex(signed.r), "s": hex(signed.s), "v": hex(signed.v)}


def tick_at(tick_size, tick_tiers: list[tuple], price) -> Any:
    """Resolve the tick-tier quantum at ``price``."""
    px = D(price)
    if not tick_tiers:
        return D(tick_size)
    for up_to, tick in tick_tiers:
        if up_to is None or px <= D(up_to):
            return D(tick)
    return D(tick_tiers[-1][1])
