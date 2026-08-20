# RH Arcus — Production Perp Market-Making Fleet

Post-only market-making fleet for **every ONLINE perpetual** on
[Arcus](https://arcus.xyz) (dYdX-built CLOB on Robinhood Chain) — crypto,
equities, commodities, and indices — with the same live dashboard and
execution contract as `rh-lighter-fleet`.

- **Venue (hardcoded, production only):** `https://api.arcus.xyz` ·
  `wss://api.arcus.xyz/v1/ws` · chain `4663` · collateral USDG
- **Docs:** https://docs.arcus.xyz/ · **Help:** https://help.arcus.xyz/
- There is **no** testnet, sandbox, paper, or dry-run mode in this codebase.
  Boot aborts if any such flag or endpoint appears in the environment.

> Arcus has **no cancel-on-disconnect**. A dead process leaves resting ALO
> quotes until `goodTilTime` (≥ 1 month). `fleet stop` / Ctrl-C cancel and
> flatten; run an external `cancel-all` if the host dies.

## Lessons applied from day one

These were paid for on Robinhood Lighter. They are structural here, not later patches:

| Lesson | Implementation |
| --- | --- |
| `sendTx` / API **accepted ≠ executed** | `placeOrder` `202`/`200` is an ACK only. Open/fill/cancel is confirmed on the `orders` + `userFills` WS streams. |
| Local trackers drift | `GET /v1/account` + `GET /v1/positions` overwrite inventory and venue-truth balance **every 30s**. |
| Never assume a `side` field | Public trades: `side` is the **taker** side. We derive **our** side from ask/bid account ids, then maker/taker addresses, then a resting order we placed. A bare `side` is ignored. |
| MARKET min sizes silent-cancel dust | Sub-minimum positions are **topped up** to a legal size before a reduce-only IOC close. |
| A-S inventory skew | Reservation price unloads passively. Taker flatten is an emergency backstop only (max-hold / adverse stop). |
| Join, don't improve | Quotes clamp to the touch. Improving the book is first in line for informed flow. |
| Contrarian gate | Skip the side pressed by momentum + book imbalance. |
| Vol breaker + min-edge | Pull quotes when per-tick vol is violent or the book spread cannot pay for the risk. |
| Maker-maker round trips | Every quote is `LIMIT` + `ALO` (skips the 50 ms taker speed bump). |
| Metrics that matter | Dashboard: **venue-truth Net PnL** (equity delta) and **Cost per $1M routed**. |
| Watchdogs | Deliberate no-quote (veto) and healthy resting quotes are **liveness**, not failure. Crash only on true silence. |

## Quick start

```bash
cd rh-arcus-fleet
pip install -r requirements.txt
cp .env.example .env        # fill in L1_ADDRESS after setup-key

# View-only dashboard — no keys, live production books:
python bot.py fleet dashboard      # → http://127.0.0.1:8900

# One-time onboarding: generate Ed25519, EIP-712 register (wallet key
# signs in memory, never stored), verify, write .env.
python bot.py fleet setup-key

# Phase 0 — prove production: universe + WS books; with keys, ALO
# open confirmed on the orders channel then cancel. Must exit 0.
python bot.py fleet prove

# Start the fleet + dashboard
python bot.py fleet start
```

Markets are **opt-in**. `fleet start` streams data and quotes nothing until
you enable pills on the dashboard (or set `FLEET_QUOTE_ON_START=true`).

## CLI

```
python bot.py fleet setup-key     # register Ed25519 API key on production
python bot.py fleet prove         # Phase-0 production proof
python bot.py fleet start         # fleet + dashboard, foreground
python bot.py fleet dashboard     # view-only, no API keys
python bot.py fleet status        # live snapshot
python bot.py fleet stop          # cancel quotes + flatten
python bot.py fleet flatten SYM   # instant-close one market
python bot.py fleet flatten-all   # cancel everything + close positions
```

## How it works

- `core/market_registry.py` auto-discovers **all** markets from
  `GET /v1/markets` at boot and re-syncs every 5 minutes; new ONLINE markets
  are hot-added. Nothing is hardcoded per symbol.
- `core/market_worker.py` runs the A-S MM loop per market: join-at-touch
  ALO both sides, requote on drift, inventory skew, gates, dust top-up.
  Equities/commodities/indices are withheld when `isOutsideRth` unless
  `FLEET_QUOTE_RWA_OFF_HOURS=true`.
- `core/execution.py` signs typed canonical payloads (Scheme 1) for
  place/cancel and Scheme 2 for `cancelAllOrders` / `setLeverage`. Batches
  of ≤39 are free on the IP weight layer. **ACK is never an open order.**
- `core/ws_hub.py` one multiplexed socket: `l2OrderbookUpdates` (sequence-gap
  resync), `markets` / `oraclePrices`, and account `orders` / `userFills` /
  `positions` / `account`. Keepalive + stale reconnect.
- `core/fills.py` is the side-derivation contract (unit-tested).
- `core/watchdog.py`: 60s zero-send **and** zero-veto → crash + last 20
  vetoes; stale WS → reconnect; idle market 5 min → restart worker.
- `core/risk.py`: daily loss → halt + flatten; low margin headroom → pause
  least-liquid markets.
- `web/` — dashboard with pair pills, Net PnL / Cost-per-$1M / Balance
  cards, live table, fills, activity log, Start / Pause / Stop / Flatten-All,
  and live Fleet Settings.

## Layout

```
rh-arcus-fleet/
├── bot.py
├── scripts/prove_production.py
├── core/
│   ├── config.py             # production-only env, banned-token guard
│   ├── signing.py            # Ed25519 scheme 1/2 + EIP-712 createApiKey
│   ├── fills.py              # side from ask/bid ids — never a bare side
│   ├── rest.py               # bootstrap + 30s reconcile + kill-switch
│   ├── ws_hub.py             # market-data + account + order RPCs
│   ├── execution.py          # ALO / IOC / batch; ACK ≠ open
│   ├── market_registry.py    # full-universe auto-discovery
│   ├── market_worker.py      # per-market A-S MM loop
│   ├── fleet_orchestrator.py # scheduling, reconcile, controls
│   ├── risk.py
│   └── watchdog.py           # veto = liveness
├── web/                      # dashboard
├── configs/market_presets.yaml
└── tests/
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

## Notes

- Colocate in **Asia** — the matching engine is hosted there.
- Every market currently enforces a **$5** min notional (MARKET included).
- `goodTilTime` is ~40 days out on every order, including IOC.
- Restricted jurisdictions cannot trade; this bot does not bypass venue
  compliance. Not financial advice.
