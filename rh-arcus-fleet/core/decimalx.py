"""Exact decimal conversion for Arcus ticks / quantums.

The engine signs integer `p` (price ÷ tick) and `q` (size ÷ step). A remainder
is a rejected order, so conversion fails loudly instead of rounding silently.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_HALF_EVEN, Decimal

ZERO = Decimal(0)


def D(value: str | int | float | Decimal | None) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(value)


def snap(value: Decimal, quantum: Decimal, mode: str = "nearest") -> Decimal:
    if quantum <= 0:
        raise ValueError(f"quantum must be positive, got {quantum}")
    rounding = {"nearest": ROUND_HALF_EVEN, "down": ROUND_DOWN, "up": ROUND_CEILING}[mode]
    steps = (value / quantum).quantize(Decimal(1), rounding=rounding)
    return (steps * quantum).normalize() + ZERO


def to_int_exact(value: str | int | float | Decimal, quantum: str | Decimal) -> int:
    n = D(value) / D(quantum)
    if n != n.to_integral_value():
        raise ValueError(f"{value} is not an exact multiple of {quantum}")
    return int(n)


def fmt_decimal(value: Decimal) -> str:
    s = format(value.normalize(), "f")
    return "0" if s in ("-0", "-0.0") else s
