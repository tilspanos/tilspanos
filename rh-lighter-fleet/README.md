# RH Lighter — Full-Universe Production Trading Fleet

Post-only market-making fleet for **every active market** on Robinhood Lighter
— crypto perps, stock/index/RWA perps, and spot USDG pairs — with a
Breads-Bakery-style live dashboard.

- **Venue (hardcoded, production only):** `https://api.rh.lighter.xyz/` · `wss://api.rh.lighter.xyz/stream` · chain `466324` · collateral USDG
- **Docs:** https://apidocs.rh.lighter.xyz/docs/get-started
- There is **no** testnet, sandbox, paper, or dry-run mode in this codebase. Boot aborts if any such flag or endpoint appears in the environment.

## Quick start

```bash
cd rh-lighter-fleet
pip install -r requirements.txt
cp .env.example .env        # fill in L1_ADDRESS, API_KEY_INDEX (>=4), API_PRIVATE_KEY

# Just want to see the dashboard? View-only mode needs NO keys:
# full universe + live production market data, orders disabled.
python bot.py fleet dashboard      # → http://127.0.0.1:8899

# One-time onboarding: generates an API key pair, registers it on the venue
# (you paste your wallet key once — it signs in memory, never stored),
# verifies it, and writes .env for you.
python bot.py fleet setup-key      # registers at index 4 by default

# Phase 0 — prove real production order flow (BTC+ETH+LIT open/cancel,
# ETH $10 market fill, SOL sendTxBatch). Must exit 0 before running the fleet.
python bot.py fleet prove

# Start the fleet + dashboard (http://127.0.0.1:8899)
python bot.py fleet start
```

## CLI

```
python bot.py fleet setup-key [N] # one-time API key registration + .env write
python bot.py fleet prove         # Phase-0 production proof gate
python bot.py fleet start         # fleet + dashboard, foreground
python bot.py fleet dashboard     # view-only dashboard, no API keys needed
python bot.py fleet status        # live snapshot of the running fleet
python bot.py fleet stop          # stop (cancels quotes, flattens positions)
python bot.py fleet flatten ETH   # instant-close one market
python bot.py fleet flatten-all   # cancel everything + close all positions
                                  #   (works standalone if no fleet is running)
```

## How it works

- `core/market_registry.py` auto-discovers **all** active markets from
  `GET /api/v1/orderBooks` + `orderBookDetails` at boot and re-syncs every
  5 minutes; new markets are hot-added with default presets. Nothing is
  hardcoded per symbol.
- `core/market_worker.py` runs the quoting loop per market with a
  **switchable strategy** (`core/strategies.py`, tread.fi-style modes):
  `avellaneda` (default, inventory-skew MM), `mid` (±offset around mid,
  negative = aggressive), `grid` (last-fill ping-pong + soft reset),
  `rgrid` (exposure-VWAP breakout capture with capped takers), `dgrid`
  (auto grid↔rgrid by regime), `signal` (RSI tilt). Switch live from the
  dashboard's Strategy dropdown; all risk rails apply to every mode.
- `core/execution.py` sends everything through `sendTx` / `sendTxBatch` and
  handles every documented error code (fat finger, post-only cross, margin,
  order caps, nonce, rate limit). **`code=200` is never treated as an open
  order** — order state is confirmed on the authenticated WebSocket
  `account_all_orders` stream.
- `core/signer_pool.py` pins each market to one API key (round-robin) so
  parallel workers never contend on a nonce; add keys in `.env` for 20+
  markets.
- `core/ws_hub.py` runs one public connection (all `order_book/{id}` subs +
  `market_stats/all`) and one authenticated connection (`account_all*`),
  with auto-reconnect, resubscribe, and 60s keepalive.
- `core/watchdog.py`: 60s zero-sendTx → **crash loudly** with the last 20
  veto reasons; stale WS → reconnect; idle market 5 min → restart worker;
  idle fleet 15 min → restart all.
- `core/risk.py`: daily loss limit → halt + flatten all; low margin headroom
  → pause least-liquid markets; >900 active orders → global stale sweep.
- `web/` — dashboard with pair pills for the full universe, stat cards
  (volume routed, fills, spread PnL, open orders, margin), live market table,
  recent fills, color-coded activity log, and Start / Pause / Stop /
  Flatten-All controls. Live via WebSocket with polling fallback.
  The **Fleet Settings** panel edits order size, spread, requote threshold,
  refresh cadence, leverage, max markets, and the daily loss halt **live** —
  no restart or `.env` edit needed (leverage is pushed to the venue on perps
  when trading). `.env` and `configs/market_presets.yaml` set the defaults.

## Layout

```
rh-lighter-fleet/
├── bot.py                       # CLI entrypoint
├── scripts/prove_production.py  # Phase-0 gate: real orders before anything else
├── core/
│   ├── config.py                # production-only env, banned-token guard
│   ├── market_registry.py       # full-universe auto-discovery + 5min resync
│   ├── signer_pool.py           # multi-key signing, per-key nonce isolation
│   ├── ws_hub.py                # market-data + account streams
│   ├── execution.py             # sendTx/sendTxBatch, error-code handling
│   ├── market_worker.py         # per-market Breads MM loop
│   ├── fleet_orchestrator.py    # scheduling, allocation, controls
│   ├── risk.py                  # margin / daily-loss / order-cap
│   └── watchdog.py              # no-order crash, WS stale, idle restarts
├── web/                         # dashboard (index.html + aiohttp API)
├── configs/market_presets.yaml  # per-market spread/size/leverage
└── .env.example
```

## Notes

- API key indices 0–3 and 157 are reserved for the Lighter front-end; use ≥4.
- Every market currently enforces a $10 USDG minimum order notional; sizing
  clamps to `max(min_base_amount, $10)` automatically.
- Restricted regions can only open the read-only stream
  (`?readonly=true`) — run the fleet from an allowed region
  (docs recommend AWS Tokyo `ap-northeast-1a` for colocation).
