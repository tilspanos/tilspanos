"""Fill-side derivation.

Hard-won rule (Lighter, applied here from day one): NEVER treat a bare
``side`` field as *our* side.

Public Arcus trades carry ``side`` as the *taker* side plus maker/taker
addresses. Lighter-style fills carry ask/bid account ids. userFills may
echo a side, but we only trust it after we know our role from addresses
or from a resting order we placed.
"""

from __future__ import annotations

from dataclasses import dataclass


def _norm_addr(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s.startswith("0x") and len(s) == 42:
        return s
    return s


def _eq(a, b) -> bool:
    na, nb = _norm_addr(a), _norm_addr(b)
    return na is not None and nb is not None and na == nb


def _taker_side(fill: dict) -> str | None:
    raw = str(
        fill.get("takerSide")
        or fill.get("taker_side")
        or fill.get("aggressorSide")
        or ""
    ).upper()
    if raw in ("BUY", "SELL"):
        return raw
    # Public /v1/trades `side` is the taker side — only used when we already
    # know we are maker or taker from addresses.
    raw = str(fill.get("side") or fill.get("orderSide") or "").upper()
    if raw in ("BUY", "SELL"):
        return raw
    return None


def _flip(side: str) -> str:
    return "SELL" if side == "BUY" else "BUY"


@dataclass(frozen=True)
class FillSide:
    side: str  # BUY | SELL — OUR side
    role: str  # maker | taker
    source: str


def derive_fill_side(
    fill: dict,
    *,
    our_address: str | None = None,
    our_account_index: int | None = None,
    our_order_id: str | None = None,
    our_client_id: str | None = None,
    our_resting_side: str | None = None,
) -> FillSide | None:
    """Return our (side, role) or None if this fill is not ours / unresolvable.

    Priority:
      1. ask/bid account id or address (Lighter-shaped + Arcus aliases)
      2. maker/taker address + taker-side interpretation
      3. match a resting order we placed (orderId / clientId)
    A bare ``side`` is never trusted on its own.
    """
    our = _norm_addr(our_address)

    ask = (
        fill.get("askAccountId")
        or fill.get("ask_account_id")
        or fill.get("askAddress")
        or fill.get("ask_address")
        or fill.get("askAccount")
    )
    bid = (
        fill.get("bidAccountId")
        or fill.get("bid_account_id")
        or fill.get("bidAddress")
        or fill.get("bid_address")
        or fill.get("bidAccount")
    )
    # Numeric account-index form (Lighter: ask_account_id == account_index).
    if our_account_index is not None and ask is not None and bid is not None:
        try:
            ask_i, bid_i = int(ask), int(bid)
        except (TypeError, ValueError):
            ask_i = bid_i = None
        else:
            if ask_i == our_account_index:
                return FillSide("SELL", "maker", "ask_account_id")
            if bid_i == our_account_index:
                return FillSide("BUY", "maker", "bid_account_id")
            return None
    if our and (ask is not None or bid is not None):
        if _eq(ask, our):
            return FillSide("SELL", "maker", "ask_address")
        if _eq(bid, our):
            return FillSide("BUY", "maker", "bid_address")
        if ask is not None and bid is not None:
            return None

    maker = fill.get("makerAddress") or fill.get("maker_address")
    taker = fill.get("takerAddress") or fill.get("taker_address")
    if our and (maker is not None or taker is not None):
        taker_side = _taker_side(fill)
        if _eq(taker, our):
            if taker_side is None:
                return None
            return FillSide(taker_side, "taker", "taker_address")
        if _eq(maker, our):
            if taker_side is None:
                return None
            return FillSide(_flip(taker_side), "maker", "maker_address")
        return None

    oid = str(fill.get("orderId") or fill.get("order_id") or "")
    cid = str(fill.get("clientId") or fill.get("client_id") or "").lower()
    if our_order_id and oid and oid == str(our_order_id):
        if our_resting_side in ("BUY", "SELL"):
            return FillSide(our_resting_side, "maker", "resting_order_id")
    if our_client_id and cid and cid == our_client_id.lower():
        if our_resting_side in ("BUY", "SELL"):
            return FillSide(our_resting_side, "maker", "resting_client_id")

    return None
