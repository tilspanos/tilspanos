"""Switchable quoting strategies (tread.fi-style reference price modes).

Every strategy answers one question per tick: WHERE do the bid/ask go and
HOW are the sizes tilted. Everything else — fat-finger clamps, post-only
safety, inventory caps, reduce-only exemption, vol breaker, backstops —
is shared machinery in MarketWorker and applies to ALL strategies.

Strategies (params come from the worker's strategy_params dict):

  avellaneda  A-S reservation-price inventory skew around the microprice.
              The research-grade default (see HANDOFF.md).
  mid         Quotes around the book mid at spread_bps offset. tread.fi
              semantics: offset may be NEGATIVE (−50..+50bps) — negative
              means "as aggressive as post-only allows" (pins to the touch)
              for maximum fill speed; positive rests deeper for margin.
              Optional `bias` (−1..+1) tilts sizes directionally.
  grid        Anchors each leg on the OPPOSITE leg's last fill price:
              after a buy at P the ask goes to P×(1+spread); after a sell
              at Q the bid goes to Q×(1−spread). Range/chop harvester.
              `grid_reset_bps`: soft reset — if the market drifts that far
              from the exposure price, the lagging leg reprices to mid.
  rgrid       Reverse grid: anchors on the rolling VWAP of recent fills;
              bid triggers ABOVE it, ask BELOW it (breakout/trend capture),
              executed with capped taker IOCs like tread.fi's RGrid.
  dgrid       Auto-toggles grid (calm) <-> rgrid (trending) using the
              momentum/volatility estimates.
  signal      RSI-driven tilt: quotes around mid but the anchor and the
              sizes lean with (RSI−50), long bias when oversold bounces up,
              short bias when overbought rolls over.
"""

from __future__ import annotations

from dataclasses import dataclass, field

STRATEGIES = ("avellaneda", "mid", "grid", "rgrid", "dgrid", "signal")


@dataclass
class QuoteContext:
    """Everything a strategy may look at (read-only)."""

    mid: float
    anchor: float            # microprice (size-weighted top of book)
    half_spread: float       # vol-adjusted half spread in PRICE units
    spread_bps: float        # raw configured spread (may be negative for mid)
    best_bid: float | None
    best_ask: float | None
    min_tick: float
    position: float          # signed base
    q_ratio: float           # inventory / max inventory, clamped [-1, 1]
    mom_bps: float           # signed momentum EWMA (bps/tick)
    vol_bps: float           # |move| EWMA (bps/tick)
    imbalance: float         # top-of-book size imbalance [-1, 1]
    last_buy_px: float | None    # our last buy fill
    last_sell_px: float | None   # our last sell fill
    exposure_px: float | None    # rolling VWAP of our recent fills (both legs)
    rsi: float | None            # RSI(14) of tick mids, 0-100
    params: dict = field(default_factory=dict)


@dataclass
class QuotePlan:
    bid_px: float | None = None   # None = don't quote this side
    ask_px: float | None = None
    bid_size_mult: float = 1.0    # multiplies the base size (before inventory tilt)
    ask_size_mult: float = 1.0
    use_contrarian_gate: bool = True   # momentum strategies opt out
    # taker actions (rgrid): list of (is_ask, size_fraction_of_base)
    taker: list[tuple[bool, float]] = field(default_factory=list)
    note: str = ""


def compute(name: str, ctx: QuoteContext) -> QuotePlan:
    name = (name or "avellaneda").lower()
    if name == "mid":
        return _mid(ctx)
    if name == "grid":
        return _grid(ctx)
    if name == "rgrid":
        return _rgrid(ctx)
    if name == "dgrid":
        return _dgrid(ctx)
    if name == "signal":
        return _signal(ctx)
    return _avellaneda(ctx)


# ── avellaneda (default) ──────────────────────────────────────────────────────
def _avellaneda(ctx: QuoteContext) -> QuotePlan:
    reservation = ctx.anchor - ctx.q_ratio * ctx.half_spread
    return QuotePlan(
        bid_px=reservation - ctx.half_spread,
        ask_px=reservation + ctx.half_spread,
        note=f"AS r={reservation:.6g}",
    )


# ── mid (tread.fi Mid Mode, spread −50..+50bps, optional directional bias) ───
def _mid(ctx: QuoteContext) -> QuotePlan:
    offset = ctx.mid * (ctx.spread_bps / 10_000)
    if ctx.spread_bps < 0:
        # negative offset = maximum aggression: pin to the touch (post-only
        # clamps in the worker keep it legal); this is the "speed" end of
        # tread.fi's scale, for volume/points farming.
        bid = ctx.best_ask - ctx.min_tick if ctx.best_ask else ctx.mid
        ask = ctx.best_bid + ctx.min_tick if ctx.best_bid else ctx.mid
    else:
        bid = ctx.mid - max(offset, ctx.min_tick)
        ask = ctx.mid + max(offset, ctx.min_tick)
    bias = max(-1.0, min(1.0, float(ctx.params.get("bias", 0.0))))
    return QuotePlan(
        bid_px=bid,
        ask_px=ask,
        bid_size_mult=1.0 + max(bias, 0.0),   # long bias front-loads buys
        ask_size_mult=1.0 + max(-bias, 0.0),  # short bias front-loads sells
        note=f"mid off={ctx.spread_bps}bps bias={bias:+.2f}",
    )


# ── grid (anchor = opposite leg's last fill; ping-pong the range) ────────────
def _grid(ctx: QuoteContext) -> QuotePlan:
    spread = abs(ctx.spread_bps) / 10_000
    reset_bps = float(ctx.params.get("grid_reset_bps", 25.0))

    # ask anchored on our last BUY, bid anchored on our last SELL;
    # before any fill, bootstrap from mid.
    ask = (ctx.last_buy_px or ctx.mid) * (1 + spread)
    bid = (ctx.last_sell_px or ctx.mid) * (1 - spread)

    note = "grid"
    # Soft reset (tread.fi Grid Reset Threshold): if the market drifted too
    # far from our exposure, reprice the lagging leg AT MID so it can fill
    # and rebalance instead of waiting for the stop loss.
    if ctx.exposure_px:
        drift_bps = (ctx.mid - ctx.exposure_px) / ctx.exposure_px * 10_000
        if drift_bps > reset_bps and ctx.position < 0:
            bid = ctx.mid - ctx.min_tick  # short stuck under a rally: buy at mid
            note = "grid RESET bid->mid"
        elif drift_bps < -reset_bps and ctx.position > 0:
            ask = ctx.mid + ctx.min_tick  # long stuck in a dump: sell at mid
            note = "grid RESET ask->mid"

    return QuotePlan(bid_px=bid, ask_px=ask, note=note)


# ── rgrid (anchor = exposure VWAP; trend capture with capped takers) ─────────
def _rgrid(ctx: QuoteContext) -> QuotePlan:
    spread = abs(ctx.spread_bps) / 10_000
    anchor = ctx.exposure_px or ctx.mid
    buy_trigger = anchor * (1 + spread)
    sell_trigger = anchor * (1 - spread)
    plan = QuotePlan(use_contrarian_gate=False, note="rgrid wait")

    # tread.fi RGrid fills when price moves THROUGH the trigger — that needs
    # taker orders (a resting post-only above mid would instantly cross).
    if ctx.mid >= buy_trigger and ctx.q_ratio < 1.0:
        plan.taker.append((False, 1.0))  # buy the breakout
        plan.note = f"rgrid taker BUY (mid {ctx.mid:.6g} >= {buy_trigger:.6g})"
    elif ctx.mid <= sell_trigger and ctx.q_ratio > -1.0:
        plan.taker.append((True, 1.0))   # sell the breakdown
        plan.note = f"rgrid taker SELL (mid {ctx.mid:.6g} <= {sell_trigger:.6g})"
    return plan


# ── dgrid (auto grid <-> rgrid on regime) ─────────────────────────────────────
def _dgrid(ctx: QuoteContext) -> QuotePlan:
    trend_bps = float(ctx.params.get("dgrid_trend_bps", 1.0))
    trending = abs(ctx.mom_bps) > trend_bps and ctx.vol_bps > trend_bps
    plan = _rgrid(ctx) if trending else _grid(ctx)
    plan.note = f"dgrid[{'rgrid' if trending else 'grid'}] " + plan.note
    return plan


# ── signal (RSI tilt around mid, tread.fi Signal Mode) ────────────────────────
def _signal(ctx: QuoteContext) -> QuotePlan:
    offset = max(ctx.mid * (abs(ctx.spread_bps) / 10_000), ctx.min_tick)
    rsi = ctx.rsi if ctx.rsi is not None else 50.0
    tilt = (50.0 - rsi) / 50.0  # oversold -> +1 (lean long), overbought -> -1
    shift = tilt * offset       # move the whole quote band with the signal
    return QuotePlan(
        bid_px=ctx.mid + shift - offset,
        ask_px=ctx.mid + shift + offset,
        bid_size_mult=1.0 + max(tilt, 0.0),
        ask_size_mult=1.0 + max(-tilt, 0.0),
        use_contrarian_gate=False,
        note=f"signal RSI={rsi:.0f} tilt={tilt:+.2f}",
    )
