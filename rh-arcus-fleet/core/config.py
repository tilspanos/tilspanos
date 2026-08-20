"""Production-only configuration for the Robinhood Chain Arcus fleet.

Venue endpoints are HARDCODED. There is no way to point this bot at a
testnet, sandbox, or paper environment. If any banned endpoint or
simulation flag leaks into the environment, boot aborts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

# ── Production venue (the ONLY venue this codebase can talk to) ──────────────
BASE_URL = "https://api.arcus.xyz"
WS_URL = "wss://api.arcus.xyz/v1/ws"
CHAIN_ID = 4663  # Robinhood Chain (Arcus production EIP-712 domain)
COLLATERAL = "USDG"
APP_URL = "https://app.arcus.xyz"

_BANNED_ENDPOINTS = (
    "api.testnet.arcus",
    "testnet.arcus",
    "api.staging.arcus",
    "staging.arcus",
)
_BANNED_FLAG_NAMES = ("DRY_RUN", "PAPER_MODE", "PAPER_TRADING", "SANDBOX_MODE", "SIMULATE", "SIMULATION")

# Venue caps (docs.arcus.xyz/api-reference/rate-limits)
MAX_BATCH_ORDERS = 39  # floor(N/40)==0 IP weight
MAX_INFLIGHT_POSTS = 45  # server cap is 50; leave headroom
MAX_WS_SUBS_PER_CONN = 100
ORDER_POOL_START = 20_000
CANCEL_POOL_START = 40_000
CANCEL_ALL_POOL_COST = 1_000
MIN_QUOTE_USDG = 5.0  # minOrderNotional on every market today
GOOD_TIL_DAYS = 40.0  # engine requires >= ~1 month, including IOC/FOK
MAX_MARKET_SLIPPAGE = 0.10  # MARKET price must be within 10% of mark


def _assert_production_env() -> None:
    for key, value in os.environ.items():
        for banned in _BANNED_ENDPOINTS:
            if banned.lower() in value.lower():
                raise SystemExit(
                    f"FATAL: banned non-production endpoint '{banned}' found in "
                    f"environment variable '{key}'. This bot is production-only. "
                    "Remove the variable and restart."
                )
        if key.upper() in _BANNED_FLAG_NAMES:
            raise SystemExit(
                f"FATAL: banned simulation flag '{key}' found in the environment. "
                "This bot is production-only — there is no dry-run/paper/sandbox "
                "mode. Remove the variable and restart."
            )


@dataclass
class FleetConfig:
    l1_address: str
    account_index: int
    api_private_key: str
    api_public_key: str

    enabled_groups: list[str] = field(default_factory=lambda: ["crypto"])
    quote_on_start: bool = False
    quote_rwa_off_hours: bool = False
    order_size_usd: float = 25.0
    spread_bps: float = 0.15
    requote_bps: float = 0.25
    refresh_ms: int = 12_000
    max_concurrent_markets: int = 20
    daily_loss_usd: float = 100.0
    leverage: int = 5
    max_hold_s: float = 300.0
    adverse_stop_bps: float = 10.0
    market_loss_usd: float = 10.0

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8900


def load_config(require_keys: bool = True) -> FleetConfig:
    """Read fleet config from the environment. Aborts on non-production setup."""
    _assert_production_env()

    for var, expected in (("ARCUS_API_URL", BASE_URL), ("ARCUS_WS_URL", WS_URL)):
        override = os.environ.get(var, "").rstrip("/")
        if override and override.rstrip("/") != expected.rstrip("/"):
            raise SystemExit(
                f"FATAL: {var}={override!r} does not point at the Arcus "
                "production API. This bot only trades production."
            )
    chain = os.environ.get("ARCUS_CHAIN_ID")
    if chain and int(chain) != CHAIN_ID:
        raise SystemExit(f"FATAL: ARCUS_CHAIN_ID={chain} != {CHAIN_ID} (Robinhood Chain).")

    l1_address = os.environ.get("L1_ADDRESS", "").strip()
    if l1_address.lower() in ("0xyourwallet", "0x..."):
        raise SystemExit(
            "FATAL: L1_ADDRESS still holds the placeholder from .env.example — "
            "replace it with your real wallet address."
        )

    api_private_key = os.environ.get("API_PRIVATE_KEY", "").strip().removeprefix("0x")
    api_public_key = os.environ.get("API_PUBLIC_KEY", "").strip().removeprefix("0x")
    if api_private_key and api_public_key and api_private_key.lower() == api_public_key.lower():
        raise SystemExit(
            "FATAL: API_PRIVATE_KEY and API_PUBLIC_KEY are identical — you "
            "pasted the public key into the private key slot."
        )
    if api_private_key and len(api_private_key) != 64:
        raise SystemExit(
            "FATAL: API_PRIVATE_KEY must be 32 bytes (64 hex chars) — an Ed25519 seed."
        )

    if require_keys:
        if not l1_address:
            raise SystemExit(
                "FATAL: L1_ADDRESS is not set. Set your wallet address in .env "
                "(see .env.example) or run `python bot.py fleet setup-key`."
            )
        if not api_private_key:
            raise SystemExit(
                "FATAL: no API key configured. Run `python bot.py fleet setup-key` "
                "or set API_PRIVATE_KEY in .env."
            )

    account_index_env = os.environ.get("ACCOUNT_INDEX", "").strip()
    account_index = int(account_index_env) if account_index_env else 0
    if not 0 <= account_index <= 9:
        raise SystemExit("FATAL: ACCOUNT_INDEX must be 0–9.")

    groups = [
        g.strip().lower()
        for g in os.environ.get("FLEET_ENABLED_GROUPS", "crypto").split(",")
        if g.strip()
    ]

    return FleetConfig(
        l1_address=l1_address,
        account_index=account_index,
        api_private_key=api_private_key,
        api_public_key=api_public_key,
        enabled_groups=groups,
        quote_on_start=os.environ.get("FLEET_QUOTE_ON_START", "false").strip().lower()
        in ("1", "true", "yes"),
        quote_rwa_off_hours=os.environ.get("FLEET_QUOTE_RWA_OFF_HOURS", "false").strip().lower()
        in ("1", "true", "yes"),
        order_size_usd=float(os.environ.get("FLEET_ORDER_SIZE_USD", "25")),
        spread_bps=float(os.environ.get("FLEET_SPREAD_BPS", "0.15")),
        requote_bps=float(os.environ.get("FLEET_REQUOTE_BPS", "0.25")),
        refresh_ms=int(os.environ.get("FLEET_REFRESH_MS", "12000")),
        max_concurrent_markets=int(os.environ.get("FLEET_MAX_CONCURRENT_MARKETS", "20")),
        daily_loss_usd=float(os.environ.get("FLEET_DAILY_LOSS_USD", "100")),
        leverage=int(os.environ.get("FLEET_LEVERAGE", "5")),
        max_hold_s=float(os.environ.get("FLEET_MAX_HOLD_S", "300")),
        adverse_stop_bps=float(os.environ.get("FLEET_ADVERSE_STOP_BPS", "10")),
        market_loss_usd=float(os.environ.get("FLEET_MARKET_LOSS_USD", "10")),
        dashboard_host=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
        dashboard_port=int(os.environ.get("DASHBOARD_PORT", "8900")),
    )
