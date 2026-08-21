"""Production-only configuration for the Robinhood Lighter fleet.

The venue endpoints are HARDCODED constants. There is intentionally no way to
point this bot at a testnet, sandbox, or paper environment. If any banned
endpoint or simulation flag leaks into the environment, boot aborts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

# ── Production venue (the ONLY venue this codebase can talk to) ──────────────
BASE_URL = "https://api.rh.lighter.xyz/"
WS_URL = "wss://api.rh.lighter.xyz/stream"
EXPLORER_URL = "https://explorerapi.rh.lighter.xyz/api/"
CHAIN_ID = 466324
COLLATERAL = "USDG"

# Endpoints that must never appear in any environment value.
_BANNED_ENDPOINTS = (
    "rh-testnet.lighter",
    "testnet.zklighter",
    "mainnet.zklighter",
)
# Simulation flags that must never exist as environment variables.
_BANNED_FLAG_NAMES = ("DRY_RUN", "PAPER_MODE", "PAPER_TRADING", "SANDBOX_MODE", "SIMULATE", "SIMULATION")

# API key indices 0-3 and 157 are reserved for the Lighter front-end.
RESERVED_API_KEY_INDICES = frozenset({0, 1, 2, 3, 157})

# Hard venue caps (docs/rate-limits.md)
MAX_ACTIVE_ORDERS_PER_MARKET = 1_000
MAX_ACTIVE_ORDERS_PER_ACCOUNT = 1_500
MAX_PENDING_ORDERS_PER_MARKET = 16
MAX_PENDING_ORDERS_PER_ACCOUNT = 500
MAX_BATCH_TX_REST = 50
MAX_BATCH_TX_WS = 15
MIN_QUOTE_USDG = 10.0  # min_quote_amount on every market today


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
class KeySlot:
    """One API key usable for signing. Nonce state is per key on the venue."""

    api_key_index: int
    private_key: str


@dataclass
class FleetConfig:
    l1_address: str
    account_index: int | None
    keys: list[KeySlot]

    enabled_groups: list[str] = field(default_factory=lambda: ["crypto"])
    # quoting strategy: avellaneda | mid | grid | rgrid | dgrid | signal
    strategy: str = "avellaneda"
    # If False (default), `fleet start` boots with market data live but NO
    # quoting — the user enables individual markets from the dashboard pills.
    quote_on_start: bool = False
    order_size_usd: float = 25.0
    spread_bps: float = 0.15
    requote_bps: float = 0.02
    refresh_ms: int = 12_000
    max_concurrent_markets: int = 20
    daily_loss_usd: float = 100.0
    leverage: int = 5
    # inventory management (profitability core)
    max_hold_s: float = 300.0        # inventory backstop patience before taker flatten
    adverse_stop_bps: float = 10.0   # IOC flatten if position moves this far against us
    market_loss_usd: float = 10.0    # auto-disable a market after this realized loss

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8899

    @property
    def key_indices(self) -> list[int]:
        return [k.api_key_index for k in self.keys]


def load_config(require_keys: bool = True) -> FleetConfig:
    """Read the fleet config from the environment. Aborts on anything that
    smells like a non-production setup."""
    _assert_production_env()

    # Refuse an environment that tries to override the venue.
    for var in ("LIGHTER_API_URL", "LIGHTER_WS_URL"):
        override = os.environ.get(var, "")
        if override and "api.rh.lighter.xyz" not in override:
            raise SystemExit(
                f"FATAL: {var}={override!r} does not point at the Robinhood "
                "Lighter production API. This bot only trades production."
            )
    chain = os.environ.get("LIGHTER_CHAIN_ID")
    if chain and int(chain) != CHAIN_ID:
        raise SystemExit(
            f"FATAL: LIGHTER_CHAIN_ID={chain} != {CHAIN_ID} (Robinhood Lighter L2)."
        )

    l1_address = os.environ.get("L1_ADDRESS", "").strip()
    if l1_address.lower() == "0xyourwallet":
        raise SystemExit(
            "FATAL: L1_ADDRESS still holds the placeholder from .env.example — "
            "replace it with your real wallet address."
        )

    keys: list[KeySlot] = []
    base_idx = os.environ.get("API_KEY_INDEX")
    base_key = os.environ.get("API_PRIVATE_KEY", "").strip()
    public_key = os.environ.get("API_PUBLIC_KEY", "").strip()
    if base_key and public_key and base_key.lower() == public_key.lower():
        raise SystemExit(
            "FATAL: API_PRIVATE_KEY and API_PUBLIC_KEY are identical — you "
            "pasted the public key into the private key slot. The bot signs "
            "with the PRIVATE key; copy it from Lighter's key creation screen."
        )
    if base_idx and base_key:
        keys.append(KeySlot(int(base_idx), base_key))
    # Optional extra keys: API_KEY_INDEX_1/API_PRIVATE_KEY_1, _2, ... _15
    for i in range(1, 16):
        idx = os.environ.get(f"API_KEY_INDEX_{i}")
        pk = os.environ.get(f"API_PRIVATE_KEY_{i}", "").strip()
        if idx and pk:
            keys.append(KeySlot(int(idx), pk))

    for slot in keys:
        if slot.api_key_index in RESERVED_API_KEY_INDICES:
            raise SystemExit(
                f"FATAL: API key index {slot.api_key_index} is reserved for the "
                "Lighter front-end (0-3, 157). Use index >= 4."
            )

    if require_keys:
        if not l1_address:
            raise SystemExit(
                "FATAL: L1_ADDRESS is not set. Set your wallet address in .env "
                "(see .env.example)."
            )
        if not keys:
            raise SystemExit(
                "FATAL: no API keys configured. Set API_KEY_INDEX (>=4) and "
                "API_PRIVATE_KEY in .env (see .env.example)."
            )

    account_index_env = os.environ.get("ACCOUNT_INDEX", "").strip()
    account_index = int(account_index_env) if account_index_env else None

    groups = [
        g.strip().lower()
        for g in os.environ.get("FLEET_ENABLED_GROUPS", "crypto").split(",")
        if g.strip()
    ]

    return FleetConfig(
        l1_address=l1_address,
        account_index=account_index,
        keys=keys,
        enabled_groups=groups,
        strategy=os.environ.get("FLEET_STRATEGY", "avellaneda").strip().lower(),
        quote_on_start=os.environ.get("FLEET_QUOTE_ON_START", "false").strip().lower()
        in ("1", "true", "yes"),
        order_size_usd=float(os.environ.get("FLEET_ORDER_SIZE_USD", "25")),
        spread_bps=float(os.environ.get("FLEET_SPREAD_BPS", "0.15")),
        requote_bps=float(os.environ.get("FLEET_REQUOTE_BPS", "0.02")),
        refresh_ms=int(os.environ.get("FLEET_REFRESH_MS", "12000")),
        max_concurrent_markets=int(os.environ.get("FLEET_MAX_CONCURRENT_MARKETS", "20")),
        daily_loss_usd=float(os.environ.get("FLEET_DAILY_LOSS_USD", "100")),
        leverage=int(os.environ.get("FLEET_LEVERAGE", "5")),
        max_hold_s=float(os.environ.get("FLEET_MAX_HOLD_S", "300")),
        adverse_stop_bps=float(os.environ.get("FLEET_ADVERSE_STOP_BPS", "10")),
        market_loss_usd=float(os.environ.get("FLEET_MARKET_LOSS_USD", "10")),
        dashboard_host=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
        dashboard_port=int(os.environ.get("DASHBOARD_PORT", "8899")),
    )
