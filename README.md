# ChainSignal — Options Flow Intelligence

Automated options trading system that monitors real-time institutional flow, runs multi-layer analysis, and executes paper trades via Interactive Brokers or Alpaca.

---

## Architecture

```
Unusual Whales WebSocket
        │
        ▼
   Flow Alert arrives
        │
        ├── DTE >= 180 → LEAP print tracked in Supabase
        │                  (accumulation scan every 30min → paper trade)
        │
        ├── Ticker in earnings watchlist? → Earnings print tracked in Supabase
        │   (premium >= $50K, 7-90 DTE, accumulation scan every 30min)
        │
        ├── DTE < 180 + sweep + vol>OI → SWING analysis
        │   └── 7-layer analysis + 9 entry gates → paper trade
        │
        └── Existing position held? → Flow contradiction check
            (closes on direction flip or conviction collapse)

Position Management (triple path):
   ├── Sync polling (every 30s IB / 15s Alpaca)
   ├── Real-time option quote streaming (tick-by-tick)
   └── UW event-driven checks (flow/dark pool/GEX for held tickers)

Daily:
   ├── 8 AM ET: Earnings watchlist refresh (3-14 days out)
   └── 4:05 PM ET: EOD P&L report via Telegram

Overnight:
   └── ES futures monitoring (alerts at -1.0% or worse)
```

**Data sources**: Unusual Whales (flow, dark pool, GEX, IV, technicals, catalyst, market tide), ApeWisdom (social)
**Execution**: Interactive Brokers (TWS/Gateway) or Alpaca paper trading API — selected via `BROKER` env var
**Persistence**: Supabase (signals, positions, LEAP flow, trade events)
**Alerts**: Telegram (trade entries, exits, daily P&L report)

---

## Broker Integration

### Broker Selection

Set `BROKER=ib` or `BROKER=alpaca` in `.env` (default: `alpaca`). Can also override via CLI: `--broker ib`.

Both brokers export an identical public API. `strategies.py` dynamically imports the correct module via `_get_broker_module()`, which is used throughout the LiveMonitor for trade placement, position management, EOD reports, and all streaming loops:

| Function | Description |
|---|---|
| `place_paper_trade(result)` | Place a swing order based on StrategyResult |
| `place_leap_trade(result, trade_plan)` | Place a LEAP order |
| `check_and_manage_positions(ticker=None)` | Poll-based exit management |
| `check_flow_contradiction(result, min_conviction)` | Close on thesis reversal |
| `start_trade_stream()` | Async real-time fill detection |
| `start_option_stream()` | Async real-time option quote streaming |
| `send_eod_report()` | Daily P&L report via Telegram |
| `get_open_positions()` | List active positions |
| `get_portfolio_summary()` | Win/loss stats |

### Interactive Brokers (`ib_trader.py`)

Uses `ib_insync` with three dedicated connections to TWS/IB Gateway:

| Client ID | Purpose | Mode |
|---|---|---|
| `IB_CLIENT_ID` | Order placement, position queries, price snapshots | Sync (via dedicated `ThreadPoolExecutor` thread) |
| `IB_CLIENT_ID + 1` | Real-time option quote streaming (`reqMktData`) | Async (event loop) |
| `IB_CLIENT_ID + 2` | Order status monitoring (`orderStatusEvent`) | Async (event loop) |

Key advantage: `reqMktData` gives tick-by-tick option quote callbacks, so exit logic fires on every price change — not on a polling interval.

**Threading model**:
- `_ib_executor` — single-worker `ThreadPoolExecutor(thread_name_prefix="ib_order")` that owns the order connection's event loop. All `ib_insync` sync methods (`qualifyContracts`, `placeOrder`, `reqMktData`, etc.) are dispatched via `_ib_run(fn)` to this thread. Python 3.14 removed implicit per-thread event loops, so calls from other threads would fail with "no current event loop" — pinning to one thread avoids this.
- `_tick_lock` protects all position mutations
- `_ib_lock` protects the order connection (lazy reconnect)
- Lock ordering: always `_tick_lock` → `_ib_lock`, never reversed
- Deadlock avoidance: only IB API calls are dispatched to `_ib_executor` — never functions that acquire `_tick_lock`, since exit monitoring holds `_tick_lock` while calling IB methods via the executor

**Two-phase close** (streaming path):
1. **Phase 1 — mark under lock**: `_process_tick_updates` holds `_tick_lock`, runs `_run_exit_logic(from_stream=True)` to check all exit conditions, calls `_mark_closed()` to set status/bookkeeping, then persists to Supabase — all inside the lock.
2. **Phase 2 — sell and notify outside lock**: After releasing the lock, submits the broker sell order via `_submit_ib_sell()` (blocking, up to ~10s). If the fill price differs from the trigger price, re-acquires the lock to update `close_price` and `pnl_pct`. Then calls `_notify_close()` to send Telegram with actual fill prices.

This eliminates the previous 5-10s lock hold during blocking IB sell calls, which could stall all other tick processing. Telegram notifications are deferred until after the IB fill, so exit alerts show actual fill prices — not the streaming trigger quote.

Three-function close split:
- `_mark_closed(pos, exit_price, reason)` — bookkeeping only: status, close_reason, close_price, pnl_pct (no Telegram, no broker calls)
- `_notify_close(pos)` — log event, persist to Supabase, send Telegram (called after IB fill)
- `_finalize_close(pos, exit_price, reason)` — convenience wrapper: `_mark_closed` + `_notify_close` (used by poll path where fill is already known)
- `_submit_ib_sell(pos)` — blocking IB market sell + fill poll (must NOT hold `_tick_lock`)
- `_close_position(pos, current_price, reason)` — poll path: `_finalize_close` under lock + `_submit_ib_sell`

**Cancel-race protection**: When cancelling a PENDING order (flow contradiction / conviction collapse), the bot checks IB positions after cancellation. If contracts filled before the cancel took effect, it immediately sells the residual position and closes with full two-phase close. This prevents orphaned positions with no risk management.

**Connection resilience**:
- All three `connectAsync` / `connect` calls use a 45-second timeout (vs ib_insync's 4s default). This gives TWS time to complete farm reconnections during the mandatory init handshake (`reqPositionsAsync`, `reqAccountUpdatesAsync`, `reqExecutionsAsync`) — ib_insync tears down the entire connection if any init request times out.
- `_get_ib()` creates the IB connection on the dedicated executor thread (via `_ib_run`), ensuring the connection's event loop lives on the same thread that will make all future sync calls.
- Streaming functions use `await ib.reqAllOpenOrdersAsync()` (not the sync `reqAllOpenOrders()`) since they run inside the main event loop — the sync version calls `loop.run_until_complete()` which raises `RuntimeError: This event loop is already running`.

**Reconnect handling**: When a streaming connection drops, `_subscribed_contracts` is cleared before re-subscribing all filled positions — prevents stale entries from blocking resubscription. Streaming functions raise `ConnectionError` on disconnect; the outer reconnect loop in `strategies.py` handles retry.

### Alpaca (`paper_trader.py`)

Uses Alpaca's Python SDK with three clients:
- `TradingClient` — order placement and position queries
- `OptionHistoricalDataClient` — option price snapshots
- `StockHistoricalDataClient` — SPY daily return, stock quotes for LEAP underlying checks

Real-time streaming uses:
- `TradingStream` — fill detection via `subscribe_trade_updates`
- `OptionDataStream` (custom `_SafeOptionStream` subclass with reconnect logic) — option quote streaming

**Two-phase close** (streaming path):
1. **Phase 1 — check and mark under lock**: `_handle_option_quote` runs `_check_and_mark()` (via `run_in_executor` for thread safety), which acquires `_pos_lock`, evaluates all exit conditions in a single locked block (eliminating TOCTOU races), calls `_finalize_close()` if closing, and persists to Supabase.
2. **Phase 2 — sell outside lock**: After releasing the lock, submits the broker sell order via `_submit_alpaca_sell()`. If the fill price differs from the trigger price, re-acquires the lock to update `close_price` and `pnl_pct`.

Three-function close split (same pattern as IB):
- `_submit_alpaca_sell(tc, pos)` — blocking Alpaca close + fill poll (must NOT hold `_pos_lock`)
- `_finalize_close(pos, exit_price, reason)` — bookkeeping, event logging, Telegram alert (no broker calls)
- `_close_position(tc, pos, current_price, reason)` — combines both for poll/contradiction paths

### Broker Isolation in Supabase

Both brokers share the same `paper_positions` table. Broker identity is determined by the `order_id` prefix:

| Broker | Order ID Format | Example |
|---|---|---|
| IB | `IB-{orderId}` | `IB-42` |
| Alpaca | `{uuid}` | `a1b2c3d4-e5f6-...` |

IB filters by `IB-` prefix when loading positions, generating EOD reports, and computing portfolio stats. IB ENTRY events also include `"broker": "ib"` in the `paper_trade_events` metadata field.

---

## Swing Strategy

### Entry Criteria

A swing trade must pass 9 gates before execution:

#### Gate 1 — Flow Trigger (WebSocket, real-time)

A flow alert from Unusual Whales must meet ALL of:

| Filter | Threshold | Rationale |
|---|---|---|
| Not an ETF | SPY, QQQ, IWM, etc. excluded | ETF flow is mostly hedging noise |
| Premium | >= $100,000 | Filters out retail-sized orders |
| Sweep | Must be a sweep order | Multi-exchange fill = urgency and conviction |
| Volume > OI | volume_oi_ratio > 1.0 | New positions opening, not closing |
| DTE < 180 | Triggering alert must be near-term | LEAP sweeps (180+ DTE) skip swing, handled by LEAP scanner |
| Cooldown | 5 min since last analysis on this ticker | Prevents redundant API calls |

#### Gate 2 — 7-Layer Analysis

If the flow qualifies, runs a full analysis across 7 layers:

| # | Layer | Weight | Type | What It Measures |
|---|---|---|---|---|
| 1 | **Flow** | 28% | Directional | Sweep count, premium size, bull/bear skew (DTE-weighted) |
| 2 | **Dark Pool** | 10% | Condition | Institutional block print activity level (non-directional) |
| 3 | **GEX** | 10% | Directional | Gamma walls, put/call walls, gamma flip level |
| 4 | **IV** | 12% | Condition | IV rank/percentile — cheap or expensive vol |
| 5 | **Technicals** | 17% | Directional | RSI, MACD, SMA trend, Bollinger Bands, VWAP, RVOL |
| 6 | **Catalyst** | 12% | Condition | Earnings proximity, FDA dates |
| 7 | **Social** | 11% | Directional | Reddit buzz (WSB, r/stocks, r/options) |

**Directional layers** (Flow, GEX, Technicals, Social) vote on BULLISH/BEARISH/NEUTRAL. The side with more votes sets the trade direction.

**Condition layers** (Dark Pool, IV, Catalyst) contribute to the composite score but don't vote on direction. Dark pool measures institutional interest. IV tells you if options are cheap or expensive. Catalyst flags upcoming events.

**TIDE adjustment**: In live mode, the market-wide option flow tide (net call vs put premium) adjusts the composite score by +3 (aligns with direction) or -3 (conflicts). Conviction is re-evaluated after the TIDE adjustment.

##### Layer 1 — Flow Scoring

Each flow alert is scored using DTE-weighted premium:

| DTE Range | Weight | Reasoning |
|---|---|---|
| 0-5 days | 0.5x | Gamma-dominated, noisy |
| 6-180 days | 1.0x | Sweet spot — near-term thesis |
| 181+ days | 0.25x | Likely hedges |

Additional filters applied per alert:
- OTM only (directional bets, not hedges)
- SPX/SPY 0DTE skipped entirely

Direction is determined by premium-weighted bull/bear ratio:
- \>65% bull premium → BULLISH
- <35% bull premium → BEARISH
- 35-65% → NEUTRAL

Flow score (0-100) = premium_score (40%) + sweep_count_score (30%) + directional_conviction_score (30%)

##### Layer 2 — Dark Pool Scoring

Measures institutional activity, NOT direction (dark pool midpoint data is structurally unreliable for direction inference).

| Activity Level | Criteria |
|---|---|
| VERY_HIGH | >= 8 large prints OR >= $50M notional |
| HIGH | >= 5 large prints OR >= $20M notional |
| MODERATE | >= 2 large prints OR >= $5M notional |
| LOW | Below thresholds |

Dark pool score (0-100) = large_print_score (60%) + notional_score (40%). Signal is always NEUTRAL.

#### Gate 3 — Conviction Score

The composite score and directional alignment determine conviction:

| Conviction | Composite Score | Directional Layers Aligned |
|---|---|---|
| **VERY_HIGH** | >= 75 | 4/4 |
| **HIGH** | >= 60 | >= 3/4 |
| **MEDIUM** | >= 45 | >= 2/4 |
| LOW | >= 30 | — |
| NONE | < 30 | — |

Only **MEDIUM or above** triggers a trade (configurable via `min_conviction`).

#### Gate 4 — Market Regime Filter

SPY's 20-day SMA determines the market regime:

| Regime | Condition | Action |
|---|---|---|
| BULLISH | SPY close > SMA20 × 1.01 | Trades allowed |
| BEARISH | SPY close < SMA20 × 0.99 | Trades allowed |
| NEUTRAL | Within 1% band | **No trades** — backtest showed PF 0.93 (losing) |

#### Gate 5 — Dedup

Before placing any order:
1. **Broker API check** (primary) — queries open positions + pending orders for the same ticker
   - IB: `ib.positions()` + `ib.openTrades()` for held tickers
   - Alpaca: `get_all_positions()` + `get_orders(status=OPEN)` for held tickers
2. **Supabase check** (secondary) — queries local position tracking for same ticker + strategy type
3. **Same-company check** — maps multi-class tickers (GOOG/GOOGL, BRK.A/BRK.B, FOX/FOXA, NWS/NWSA, etc.) to prevent double exposure on the same underlying

If the ticker (or a same-company ticker) already has an open swing position → skip.

#### Gate 6 — Position Cap

Total open positions (PENDING + FILLED, all strategies combined) are capped at 8:

| Open Positions | Minimum Conviction |
|---|---|
| 0-2 | MEDIUM+ |
| 3-8 | HIGH+ only |
| > 8 | **No new trades** |

This forces selectivity and prevents concentration risk from correlated bets.

#### Gate 6b — Position Sizing

Contract quantity is computed from `MAX_POSITION_COST` (default $3,000):

```
cost_per_contract = option_price × 100
quantity = max(1, floor(MAX_POSITION_COST / cost_per_contract))
```

If a single contract exceeds `MAX_POSITION_COST`, quantity stays at 1 with a warning logged. This prevents outsized risk on expensive contracts (e.g., a $50 option = $5,000/contract) and ensures consistent dollar exposure across positions.

Applies to both swing and LEAP trades in both brokers. LEAP allocation cap (`new_cost = mid_price × 100 × quantity`) accounts for multi-contract sizing.

#### Gate 7 — Technicals Filter (Direction-Aware)

Technicals must confirm the trade direction:
- **Bullish trades**: technicals score must be >= 50 (uptrend confirmation)
- **Bearish trades**: technicals score must be <= 50 (downtrend confirmation)

Backtest validation (282 trades): bearish trades with confirming technicals (score < 50) had 86% win rate vs 43% for contrarian bearish trades (score >= 50). Trades that fail this filter are still analyzed and alerted via Telegram but do not execute.

#### Gate 8 — Trade Plan Validation

Must have a valid suggested strike and expiry. Derived from qualifying flow prints when available:
- Correct option type (CALL for bull, PUT for bear)
- OTM only
- Within DTE guidance range
- Strike within target range
- Closest to ATM among qualifying prints (higher delta, more realistic targets)

**Synthetic contract fallback**: If no qualifying flow prints match the DTE/strike criteria, the system builds a synthetic contract at ~3% OTM using the median DTE from available prints. This prevents flow contradiction trades and edge cases from being skipped entirely when the triggering flow prints don't match the computed direction's option type.

### Exit Criteria (Swing)

Positions are checked via three paths: sync polling (every 30s for IB, 15s for Alpaca), real-time option quote streaming (tick-by-tick), and UW event-driven checks (flow alerts, dark pool prints, and GEX updates for held tickers trigger immediate position checks with 10s per-ticker debounce). Exit checks run in this order — first match wins:

#### 0. Flow Contradiction

```
If new sweep flow flips direction (BULLISH → BEARISH or vice versa)
AND new analysis conviction >= MEDIUM → CLOSE
If new analysis conviction drops to LOW/NONE → CLOSE (conviction collapsed)
```

Runs on every qualifying flow alert for tickers with open positions. Uses a 15-minute cooldown per ticker to avoid redundant API calls. Only triggers on sweep orders that pass volume/OI filters. Same-company tickers (GOOG/GOOGL) are checked. LEAP-DTE sweeps (180+) still trigger contradiction checks for existing positions.

Both brokers use two-phase close: exit conditions are evaluated and positions marked CLOSED under lock, then broker sell orders are submitted outside the lock. If the fill price differs from the trigger price, the lock is re-acquired to update `close_price` and `pnl_pct`. This prevents stale state while avoiding lock contention during blocking broker calls.

#### 1. Progressive Stop Ratchet

As a swing position's peak profit grows, the stop floor automatically tightens. The floor **linearly interpolates** between tier anchors so there are no flat zones where large profits can evaporate:

| Peak PnL (anchor) | Stop Floor | Max Giveback |
|---|---|---|
| +10% | 0% (breakeven) | 10% |
| +20% | +10% | 10% |
| +30% | +25.5% (15% trail) | 4.5% |
| +50% | +45% (10% trail) | 5% |
| +80% | +74.4% (7% trail) | 5.6% |
| +100%+ | 5% trail from peak | ~5% |

Between anchors the floor is interpolated — e.g. +27% peak → +21% floor (not flat +10%). This keeps max giveback under ~10% at all levels above breakeven.

The ratchet is one-way — the stop only moves up, never down. Enforced in both sync polling and real-time quote streaming, and re-applied on position load so existing positions get protected on restart. Does not apply to LEAPs.

#### 2. Hard Stop

```
If premium PnL <= -25% → CLOSE
```

Non-negotiable. Limits max loss on any single trade. Only applies if progressive stop has not raised the floor above -25%.

#### 3. Profit Target

```
If premium PnL >= premium_target_pct → CLOSE
```

Target is dynamically computed from IV expected move × option leverage × conviction/regime/catalyst multipliers:

```
expected_move = IV × sqrt(max_hold_days) × 100
target_pct = expected_move × conviction_mult × regime_mult × catalyst_mult
premium_target = target_pct × option_leverage
```

| Conviction | Multiplier |
|---|---|
| VERY_HIGH | 2.0x |
| HIGH | 1.5x |
| MEDIUM | 1.0x |
| LOW | 0.75x |

| Regime | Multiplier |
|---|---|
| Aligned (bull signal + bull regime) | 1.2x |
| Conflicting | 0.8x |
| Neutral | 1.0x |

| Catalyst | Multiplier |
|---|---|
| Earnings <= 7 days | 1.3x |
| Earnings <= 14 days | 1.1x |
| No catalyst | 1.0x |

Premium target is clamped to 20-200%.

#### 4. Legacy Trailing Stop (safety net)

```
If premium PnL >= trail_activate_pct → activate trailing
Once active, if drawdown from peak >= trail_stop_pct → CLOSE
```

- Trail activation = 60% of the premium target, capped at 40%
- Trail stop = 20% drawdown from the peak premium

This is a legacy backstop — the progressive stop ratchet (Exit 1) handles profit protection at all levels and will typically close positions before this fires. Kept as a safety net for edge cases.

#### 5. Theta Kill

```
If days_held >= theta_kill_days AND abs(premium PnL) < theta_kill_move_pct → CLOSE
```

Default: if after 5 days the premium hasn't moved 10% in either direction, the trade isn't working and theta is eating the position. Cut it.

Adjusted for earnings proximity:
- Earnings <= 7 days: theta_kill_days = min(2, max_hold - 1)
- Earnings <= 14 days: theta_kill_days = min(4, max_hold - 2)

#### 6. Max Hold

```
If days_held >= max_hold_days → CLOSE
```

Default 10 days. Shortened near earnings. Prevents dead positions from tying up capital.

### Swing Stop Loss (Underlying)

The underlying stop price is set from GEX levels when available:

- **Bull trades**: put wall support or gamma flip level (whichever is higher), if 2-10% below price
- **Bear trades**: call wall resistance, if 2-10% above price
- **Fallback**: 5% default stop if no GEX level qualifies

This stop is used for the trade plan display and Telegram alerts, but the actual position management uses **premium-based stops** (progressive ratchet or hard stop at -25%), not underlying price stops — except for LEAPs.

### DTE Guidance

Contract DTE is selected from flow print timing:

1. If qualifying flow prints exist: use median DTE of prints (minimum 21), +21 window
2. If earnings within 30 days: earnings DTE + 7 to earnings DTE + 21
3. Default: 21-45 DTE

### Contract Selection

From all flow prints matching the correct option type, OTM, within DTE range, and within target range — pick the strike closest to ATM (higher delta = more realistic target):

| OTM Distance | Estimated Delta | Option Leverage |
|---|---|---|
| <= 2% | ~0.50 | 2.0x |
| <= 5% | ~0.35 | 2.9x |
| <= 10% | ~0.25 | 4.0x |
| > 10% | ~0.15 | 6.7x |

**Minimum delta floor**: Contracts with estimated delta < 0.20 (>10% OTM) are rejected during filtering. This prevents lottery-ticket entries on high-priced stocks where even moderate OTM % produces very low probability-of-profit contracts. If no flow prints pass the delta floor, the synthetic fallback at ~3% OTM (delta ~0.35) kicks in.

### Stale Order Handling

Pending orders that haven't filled within 24 hours are automatically cancelled (IB cancels via `ib.cancelOrder()`, Alpaca via `cancel_order_by_id()`).

---

## LEAP Strategy

### Entry Criteria

LEAPs follow a completely different entry path — accumulation-based instead of single-alert-based.

#### Step 1 — Individual Print Tracking (real-time)

Every flow alert is checked for LEAP criteria BEFORE the swing filters:

| Filter | Threshold |
|---|---|
| Premium | >= $100,000 |
| DTE | >= 180 days |
| Not an ETF | Same blacklist as swing |

Each qualifying print is saved to Supabase with sentiment derived from order side:
- CALL bought at ask OR PUT sold at bid → **BULL**
- PUT bought at ask OR CALL sold at bid → **BEAR**
- Equal ask/bid premium → **NEUTRAL**

#### Step 2 — Accumulation Scan (every 30 minutes)

The LEAP scan loop queries Supabase for tickers with enough accumulated flow over the past 5 days:

| Filter | Threshold | Rationale |
|---|---|---|
| Minimum prints | >= 2 | Not a one-off |
| Total premium | >= $300,000 | Meaningful institutional size |
| Directional skew | >= 65% one direction | Clear directional edge |
| Dedup | No LEAP signal for this ticker in 48 hours | Prevents over-alerting |

#### Step 3 — Regime Filter

Same as swing — NEUTRAL regime skips entirely (no API calls wasted).

#### Step 4 — Directional Edge

Direction is set from the accumulated LEAP flow, NOT from the 7-layer analysis:
- If bull premium > bear premium → BULLISH
- If bear premium > bull premium → BEARISH
- If equal → no edge → skip

The 7-layer analysis still runs for the composite score and conviction, but the direction is overridden by the flow accumulation. The flow IS the thesis.

#### Step 5 — Trade Plan

Contract selection follows the highest-premium print from the accumulated flow (the biggest institutional bet):

| Moneyness | Estimated Delta |
|---|---|
| ITM >= 15% | ~0.90 |
| ITM >= 5% | ~0.70 |
| ITM < 5% | ~0.55 |
| OTM <= 5% | ~0.50 |
| OTM <= 15% | ~0.35 |
| OTM <= 30% | ~0.20 |
| OTM > 30% | ~0.10 |

#### Step 6 — Position Cap

Same 8-position cap as swing (shared across all strategies). Slots 0-2 allow MEDIUM+, slots 3-8 require HIGH+.

#### Step 7 — Allocation Cap

Total LEAP exposure cannot exceed 20% of account equity. If adding this trade would breach the cap, it's skipped.

- IB: equity from `ib.accountValues()` (NetLiquidation)
- Alpaca: equity from `tc.get_account().equity`

#### Step 8 — Dedup

Same as swing — broker API check + Supabase check for existing LEAP positions on same ticker.

### Exit Criteria (LEAP)

Positions are checked via sync polling (30s IB / 15s Alpaca), real-time streaming, and UW event-driven checks. IB's streaming path skips LEAP underlying checks (blocking IB calls) to avoid stalling the event loop — the 30s poll handles those. Exit checks run in this order:

#### 1. Hard Stop (Premium)

```
If premium PnL <= -50% → CLOSE
```

Wider than swing (-25%) because LEAPs need more room.

#### 2. Underlying Stop

```
Bull LEAP: if stock drops 25% from entry → CLOSE
Bear LEAP: if stock rises 25% from entry → CLOSE
```

This is unique to LEAPs. Uses the stock price at time of fill (updated on fill via IB `reqMktData` snapshot or Alpaca `StockLatestQuoteRequest`). Swing trades don't have an underlying stop in position management — only LEAPs do.

IB streaming path: underlying check is skipped (`from_stream=True`) because it requires blocking IB stock price calls. The 30s poll path handles it instead.

#### 3. No Fixed Profit Target

```
premium_target_pct = 99999% (effectively disabled)
```

LEAPs don't have a fixed TP. The thesis is long-term — you let it run.

#### 4. Trailing Stop (Primary Exit)

```
If premium PnL >= +100% (doubled) → activate trailing
Once active, if drawdown from peak >= 25% → CLOSE
```

The trail is the only way to take profit on a LEAP. Once the position doubles, it starts tracking the peak. A 25% pullback from the peak locks in gains.

#### 5. Theta Kill — Disabled

```
theta_kill_days = 999 (effectively disabled)
```

LEAPs have long DTE by design. Theta kill doesn't apply.

#### 6. Max Hold

```
If days_held >= 180 → CLOSE
```

Hard cap at 180 days. Prevents positions from sitting indefinitely.

### Stale Order Handling (LEAP)

Pending LEAP orders that haven't filled within 48 hours are automatically cancelled (vs 24 hours for swing).

---

## Pre-Earnings Flow Scanner

Detects institutional positioning ahead of earnings announcements. Mirrors the LEAP scanner pattern: track prints passively → accumulate in Supabase → scan periodically → analyze → alert.

### Data Flow

1. **Daily watchlist refresh** (8 AM ET + startup): Queries UW earnings API for ~130 tickers, builds `_earnings_watchlist` of tickers with earnings 3-14 days out.
2. **Print tracking** (every flow alert): If ticker is in watchlist, premium >= $50K, and 7 <= DTE <= 90, the print is saved to `earnings_flow` table in Supabase.
3. **Accumulation scan** (every 30 min): Checks accumulated earnings flow over the past 5 days against thresholds.

### Accumulation Thresholds

| Parameter | Threshold |
|---|---|
| Minimum prints | >= 3 |
| Total premium | >= $200,000 |
| Directional skew | >= 65% one direction |
| Sweep percentage | >= 40% |
| Dedup window | 24 hours |

### Threshold Comparison (vs LEAP)

| Parameter | LEAP | Pre-Earnings | Why |
|---|---|---|---|
| Min premium/print | $100K | $50K | Pre-earnings flow can be smaller |
| Min prints | 2 | 3 | Need more confirmation |
| Total accumulation | $300K | $200K | Lower bar, more prints required |
| Sweep requirement | none | 40% | Sweeps signal urgency |
| Dedup window | 48h | 24h | Earnings timing is more critical |
| DTE filter | >= 180 | 7-90 | Near-term positioning |

### Exit Strategy

Always close BEFORE earnings. The thesis is pre-earnings IV expansion + smart money positioning, NOT earnings prediction. `max_hold_days = earnings_date - 1` enforces this.

Currently alert-only (`_earnings_auto_trade = False`).

---

## ES Overnight Monitoring

Monitors E-mini S&P 500 futures (/ES) during overnight sessions for portfolio risk alerts.

- Subscribes to ES continuous contract via IB `reqMktData`
- Calculates percentage move from prior close
- Alerts via Telegram at **-1.0% or worse** (one alert per session, resets at market open)
- Flags open positions with `es_overnight_pct` at close for post-analysis

---

## Swing vs LEAP Comparison

| Parameter | Swing | LEAP |
|---|---|---|
| **Trigger** | Single $100K+ sweep (DTE < 180) | $300K+ accumulated over 5 days (DTE 180+) |
| **DTE** | 21-45 DTE | 180+ DTE |
| **Direction source** | 7-layer analysis | LEAP flow accumulation |
| **Profit target** | Dynamic (20-200%) | None — trail decides |
| **Trail activation** | 60% of target (max 40%) | +100% (doubled) |
| **Trail stop** | 20% from peak (legacy, superseded by progressive) | 25% from peak |
| **Progressive stop** | Interpolated ratchet: +10%→0%, +20%→+10%, +30%→+25.5%, +50%→+45%, +80%→+74.4%, +100%→5%trail (smooth between anchors) | None |
| **Hard stop** | -25% premium | -50% premium |
| **Underlying stop** | None (premium-based only) | -25% stock move |
| **Theta kill** | Day 5, <10% move | Disabled |
| **Max hold** | 10 days | 180 days |
| **Stale order cancel** | 24 hours | 48 hours |
| **Position sizing** | Dollar-based: `MAX_POSITION_COST / (price × 100)`, min 1 | Same |
| **Position cap** | Shared 8-slot cap (0-2 MEDIUM+, 3-8 HIGH+) | Shared 8-slot cap + 20% equity cap |
| **Technicals filter** | Direction-aware (bull ≥50, bear ≤50) | Score >= 50 |
| **Flow contradiction** | Closes on direction flip | Closes on direction flip |
| **Scan frequency** | Real-time (WebSocket) | Every 30 minutes |

---

## Alerts

### Trade Alerts (real-time)
- **Entry**: Telegram alert on order placement with contract details, conviction, and stop/trail parameters
- **Fill**: Telegram alert on order fill with filled price
- **Exit**: Telegram alert with P&L percentage and dollar amounts (based on actual fill price, not trigger quote), cost basis, exit value, peak premium info, and exit reason
- **Analysis**: Full strategy alert with 7-layer breakdown, trade plan, and top flow prints (Telegram alerts disabled for strategy signals by default)

### EOD Daily P&L Report (4:05 PM ET)
- Realized P&L: all positions closed today with individual P&L and exit reasons
- Unrealized P&L: all open positions with current mark-to-market
- Net daily P&L (realized + unrealized)
- Account equity (IB: NetLiquidation, Alpaca: account equity)
- All-time win/loss stats and win rate

---

## Persistence

All data is stored in Supabase:

| Table | Purpose |
|---|---|
| `signals` | Every analysis result (live, scan, manual, leap, earnings) |
| `paper_positions` | Active position tracking — shared by both brokers, distinguished by `order_id` prefix |
| `paper_trade_events` | State change audit log (ENTRY, FILL, TP_HIT, HARD_STOP, TRAIL_STOP, PROGRESSIVE_STOP, BREAKEVEN_STOP, UNDERLYING_STOP, THETA_KILL, MAX_HOLD, FLOW_CONTRADICTION, ORDER_CANCELLED, ORDER_REJECTED, ORDER_EXPIRED, TRAIL_ACTIVATED) |
| `leap_flow` | Individual LEAP flow prints for accumulation tracking |
| `earnings_flow` | Pre-earnings flow prints for accumulation tracking |
| `backtest_runs` | Backtest run metadata and aggregate stats |
| `backtest_trades` | Individual backtest trade results |

Position management loads only active positions (PENDING/FILLED) from Supabase. Closed positions are persisted but not reloaded on restart.

### Broker Isolation

Both IB and Alpaca write to the same `paper_positions` table. Broker is identified by `order_id` format:

- **IB**: `order_id` starts with `IB-` → IB filters by `IB-` prefix on load, EOD, and stats
- **Alpaca**: `order_id` is a UUID → Alpaca filters by `broker="alpaca"` column on load, EOD, and stats

This means if you switch brokers, the new broker won't see the old broker's positions — each broker only loads its own records.

### Position Tracking Fields

Each position tracks:
- `peak_premium` — highest option price seen (for progressive stop ratchet)
- `trough_premium` — lowest option price seen (for drawdown analysis)
- `trail_active` — whether legacy trailing stop has been activated
- `market_return_pct` — SPY daily return at time of entry (for red-day vs green-day analysis)
- `strategy_type` — `SWING` or `LEAP` (determines which exit rules apply)

On load, swing positions have:
- `trail_activate_pct` capped at 40% to prevent unreachable thresholds
- `premium_stop_pct` ratcheted up via `_progressive_stop()` based on peak P/L (e.g., +103% peak → +97.8% stop floor)

---

## Files

| File | Lines | Purpose |
|---|---|---|
| `strategies.py` | ~4210 | 7-layer analysis engine (incl. Bollinger Bands + RSI confluence), composite scoring, trade plan computation (with synthetic fallback + delta floor), LiveMonitor (WebSocket + UW event-driven position monitoring + earnings watchlist + ES overnight), backtest engine, CLI, Telegram alerts, broker module selection |
| `ib_trader.py` | ~1750 | IB paper trading: order placement, dollar-based position sizing, position management, tick-by-tick streaming (reqMktData) with two-phase close (mark/notify/finalize split), fill detection (orderStatusEvent), cancel-race protection, EOD report, three-clientId architecture, dedicated `ThreadPoolExecutor` order thread, 45s connection timeout, actual fill price on exit |
| `paper_trader.py` | ~1620 | Alpaca paper trading: order placement, dollar-based position sizing, position management, option quote streaming (OptionDataStream) with two-phase close, fill detection (TradingStream), EOD report, actual fill price on exit, shared utilities (PaperPosition, progressive stop, OCC builder, Telegram helper) |
| `db.py` | ~430 | Supabase persistence layer: signals, positions, trade events, LEAP flow, earnings flow, backtest results |
| `migrations/` | — | SQL schema migrations (initial schema, broker column, flow contradicts, trade events, LEAP tracking, positions, updated_at trigger, ES overnight, earnings flow) |
| `.env` | — | API keys (UW, Telegram, Supabase, Alpaca, IB), broker selection |

### Shared Utilities (exported from `paper_trader.py`)

`ib_trader.py` imports these from `paper_trader.py` to avoid duplication:
- `PaperPosition` — dataclass with all position tracking fields
- `SAME_COMPANY_TICKERS` — multi-class ticker mapping (GOOG/GOOGL, etc.)
- `_has_same_company_position()` — dedup helper
- `_build_occ_symbol()` — OCC option symbol builder (e.g., `AAPL260515C00200000`)
- `_progressive_stop()` — peak-based stop floor calculator
- `_send_paper_telegram()` — Telegram message sender
- `_reason_to_event_type()` — close reason → event type mapper

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `UW_API_KEY` | Yes | Unusual Whales API key |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token for alerts |
| `TELEGRAM_CHAT_ID` | Yes | Telegram chat ID for alerts |
| `NEXT_PUBLIC_SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes | Supabase service role key |
| `BROKER` | No | `ib` or `alpaca` (default: `alpaca`) |
| `IB_HOST` | No | TWS/Gateway host (default: `127.0.0.1`) |
| `IB_PORT` | No | TWS/Gateway port (default: `7497` for paper) |
| `IB_CLIENT_ID` | No | Base client ID for IB connections (default: `1`) |
| `ALPACA_API_KEY` | No | Alpaca paper trading API key |
| `ALPACA_SECRET_KEY` | No | Alpaca paper trading secret key |
| `MAX_POSITION_COST` | No | Max dollar cost per position (default: `3000`). Quantity = floor(cost / (price × 100)), minimum 1. |

---

## CLI Usage

```bash
# Single ticker analysis
python strategies.py --ticker NVDA

# Watchlist
python strategies.py --watchlist NVDA,AAPL,TSLA

# Flow scan (top candidates)
python strategies.py --scan

# Live monitor with paper trading (IB)
python strategies.py --live --paper --broker ib

# Live monitor with paper trading (Alpaca)
python strategies.py --live --paper --broker alpaca

# Live monitor (console only, no trades)
python strategies.py --live

# Backtest
python strategies.py --backtest --start-date 2026-01-01 --end-date 2026-03-01 \
    --top-n 5 --backtest-delay 4.5 --backtest-min-conviction MEDIUM

# JSON output
python strategies.py --ticker NVDA --json

# Telegram alerts
python strategies.py --scan --telegram --telegram-min-conviction HIGH
```

### CLI Arguments

| Argument | Default | Description |
|---|---|---|
| `--ticker, -t` | — | Analyze a single ticker |
| `--watchlist, -w` | — | Comma-separated tickers |
| `--scan, -s` | — | Scan flow for top candidates |
| `--live` | — | Live WebSocket monitor |
| `--paper` | off | Enable paper trading in live mode |
| `--broker` | env `BROKER` | `ib` or `alpaca` |
| `--min-premium` | 100000 | Min premium for flow scan |
| `--cooldown` | 5 | Minutes between re-analyzing same ticker |
| `--telegram` | off | Send Telegram alerts |
| `--telegram-min-conviction` | MEDIUM | Min conviction for Telegram alerts |
| `--backtest` | — | Run historical backtest |
| `--start-date` | — | Backtest start (YYYY-MM-DD) |
| `--end-date` | — | Backtest end (YYYY-MM-DD) |
| `--top-n` | 5 | Tickers per day in backtest |
| `--backtest-delay` | 1.0 | Seconds between API calls |
| `--backtest-min-conviction` | LOW | Min conviction for backtest trades |
| `--json` | off | Output as JSON |

---

## Backtest Results

60-day backtest with regime filter + stop-loss management:

- **Profit Factor**: 2.02
- **Total trades**: 121
- **Regime filter**: NEUTRAL regime trades excluded (PF 0.93 without filter)

Out-of-sample validation still pending.

---

## Dependencies

```
httpx>=0.28
python-dotenv>=1.0
websockets>=15.0
supabase>=2.0
ib_insync>=0.9.86     # IB broker
alpaca-py>=0.43        # Alpaca broker
```
