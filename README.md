# ChainSignal — Options Flow Intelligence

Automated options trading system that monitors real-time institutional flow, runs multi-layer analysis, and executes paper trades on Alpaca.

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
        ├── DTE < 180 + sweep + vol>OI → SWING analysis
        │   └── 7-layer analysis + 9 entry gates → paper trade
        │
        └── Existing position held? → Flow contradiction check
            (closes on direction flip or conviction collapse)

Position Management (dual path):
   ├── Sync polling (every 120s)
   └── Real-time option quote streaming (sub-second)

Daily:
   └── EOD P&L report via Telegram (4:05 PM ET)
```

**Data sources**: Unusual Whales (flow, dark pool, GEX, IV, technicals, catalyst, market tide), ApeWisdom (social)
**Execution**: Alpaca paper trading API
**Persistence**: Supabase (signals, positions, LEAP flow, trade events)
**Alerts**: Telegram (trade entries, exits, daily P&L report)

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
| 5 | **Technicals** | 17% | Directional | RSI, MACD, SMA trend, VWAP, RVOL |
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
1. **Alpaca API check** (primary) — queries open positions + pending orders for the same ticker
2. **Supabase check** (secondary) — queries local position tracking for same ticker + strategy type
3. **Same-company check** — maps multi-class tickers (GOOG/GOOGL, BRK.A/BRK.B, etc.) to prevent double exposure on the same underlying

If the ticker (or a same-company ticker) already has an open swing position → skip.

#### Gate 6 — Position Cap

Total open positions (PENDING + FILLED, all strategies combined) are capped at 8:

| Open Positions | Minimum Conviction |
|---|---|
| 1-3 | MEDIUM+ |
| 4-8 | HIGH+ only |
| > 8 | **No new trades** |

This forces selectivity and prevents concentration risk from correlated bets.

#### Gate 7 — Technicals Filter

Technicals score must be >= 50 to place a paper trade. Trades with weak technicals (fighting momentum) are still analyzed and alerted via Telegram but do not execute.

#### Gate 8 — Trade Plan Validation

Must have a valid suggested strike and expiry derived from the qualifying flow prints:
- Correct option type (CALL for bull, PUT for bear)
- OTM only
- Within DTE guidance range
- Strike within target range
- Closest to ATM among qualifying prints (higher delta, more realistic targets)

### Exit Criteria (Swing)

Positions are checked every 120 seconds via sync polling AND real-time option quote streaming. Exit checks run in this order — first match wins:

#### 0. Flow Contradiction

```
If new sweep flow flips direction (BULLISH → BEARISH or vice versa)
AND new analysis conviction >= MEDIUM → CLOSE
If new analysis conviction drops to LOW/NONE → CLOSE (conviction collapsed)
```

Runs on every qualifying flow alert for tickers with open positions. Uses a 15-minute cooldown per ticker to avoid redundant API calls. Only triggers on sweep orders that pass volume/OI filters. Same-company tickers (GOOG/GOOGL) are checked. LEAP-DTE sweeps (180+) still trigger contradiction checks for existing positions.

#### 1. Breakeven Stop

```
If peak premium PnL ever reached >= +10% → stop moves from -40% to 0%
If premium PnL then drops to <= 0% → CLOSE
```

Once a swing position has been +10% green, the hard stop tightens to breakeven. This prevents positions that were profitable from becoming -40% losers. The stop ratchets one way — once activated, it never reverts. Also enforced on position load so existing positions get protected on restart. Does not apply to LEAPs.

#### 2. Hard Stop

```
If premium PnL <= -40% → CLOSE
```

Non-negotiable. Limits max loss on any single trade. Only applies if breakeven stop has not been activated.

#### 3. Profit Target

```
If premium PnL >= premium_target_pct → CLOSE
```

Target is dynamically computed from IV expected move × option leverage × conviction/regime/catalyst multipliers:

```
expected_move = IV × √(max_hold_days) × 100
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

#### 4. Trailing Stop

```
If premium PnL >= trail_activate_pct → activate trailing
Once active, if drawdown from peak >= trail_stop_pct → CLOSE
```

- Trail activation = 60% of the premium target, capped at 40%
- Trail stop = 20% drawdown from the peak premium

This is the key mechanism for capturing outsized gains — once the trail activates, it lets winners run while locking in profit on pullback.

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

This stop is used for the trade plan display and Telegram alerts, but the actual position management uses **premium-based stops** (breakeven at 0% or hard stop at -40%), not underlying price stops — except for LEAPs.

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

### Stale Order Handling

Pending orders that haven't filled within 24 hours are automatically cancelled.

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

Same 8-position cap as swing (shared across all strategies). Slots 1-3 allow MEDIUM+, slots 4-8 require HIGH+.

#### Step 7 — Allocation Cap

Total LEAP exposure cannot exceed 20% of account equity. If adding this trade would breach the cap, it's skipped.

#### Step 8 — Dedup

Same as swing — Alpaca API check + Supabase check for existing LEAP positions on same ticker.

### Exit Criteria (LEAP)

Positions are checked every 120 seconds. Exit checks run in this order:

#### 1. Hard Stop (Premium)

```
If premium PnL <= -50% → CLOSE
```

Wider than swing (-40%) because LEAPs need more room. No breakeven stop for LEAPs.

#### 2. Underlying Stop

```
Bull LEAP: if stock drops 25% from entry → CLOSE
Bear LEAP: if stock rises 25% from entry → CLOSE
```

This is unique to LEAPs. Uses the stock price at time of fill (updated with zero-quote protection). Swing trades don't have an underlying stop in the position management — only LEAPs do.

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

## Swing vs LEAP Comparison

| Parameter | Swing | LEAP |
|---|---|---|
| **Trigger** | Single $100K+ sweep (DTE < 180) | $300K+ accumulated over 5 days (DTE 180+) |
| **DTE** | 21-45 DTE | 180+ DTE |
| **Direction source** | 7-layer analysis | LEAP flow accumulation |
| **Profit target** | Dynamic (20-200%) | None — trail decides |
| **Trail activation** | 60% of target (max 40%) | +100% (doubled) |
| **Trail stop** | 20% from peak | 25% from peak |
| **Breakeven stop** | +10% peak → stop moves to 0% | None |
| **Hard stop** | -40% premium | -50% premium |
| **Underlying stop** | None (premium-based only) | -25% stock move |
| **Theta kill** | Day 5, <10% move | Disabled |
| **Max hold** | 10 days | 180 days |
| **Stale order cancel** | 24 hours | 48 hours |
| **Position cap** | Shared 8-slot cap (3 MEDIUM, 4-8 HIGH+) | Shared 8-slot cap + 20% equity cap |
| **Technicals filter** | Score >= 50 | Score >= 50 |
| **Flow contradiction** | Closes on direction flip | Closes on direction flip |
| **Scan frequency** | Real-time (WebSocket) | Every 30 minutes |

---

## Alerts

### Trade Alerts (real-time)
- **Entry**: Telegram alert on order fill with contract details
- **Exit**: Telegram alert with P&L, exit reason, and entry/exit prices
- **Analysis**: Full strategy alert with 7-layer breakdown, trade plan, and top flow prints

### EOD Daily P&L Report (4:05 PM ET)
- Realized P&L: all positions closed today with individual P&L and exit reasons
- Unrealized P&L: all open positions with current mark-to-market
- Net daily P&L (realized + unrealized)
- Account equity
- All-time win/loss stats and win rate

---

## Persistence

All data is stored in Supabase:

| Table | Purpose |
|---|---|
| `signals` | Every analysis result (live, scan, manual, leap) |
| `paper_positions` | Active position tracking (replaces local JSON) |
| `paper_trade_events` | State change audit log (ENTRY, FILL, TP_HIT, HARD_STOP, etc.) |
| `leap_flow` | Individual LEAP flow prints for accumulation tracking |
| `backtest_runs` | Backtest run metadata and aggregate stats |
| `backtest_trades` | Individual backtest trade results |

Position management loads only active positions (PENDING/FILLED) from Supabase. Closed positions are persisted but not reloaded on restart.

### Position Tracking Fields

Each position tracks:
- `peak_premium` — highest option price seen (for trailing stop and breakeven stop)
- `trough_premium` — lowest option price seen (for drawdown analysis)
- `trail_active` — whether trailing stop has been activated

On load, swing positions have:
- `trail_activate_pct` capped at 40% to prevent unreachable thresholds
- `premium_stop_pct` tightened to 0% if `peak_premium` already reached +10% above entry (breakeven enforcement)

---

## Backtest Results

60-day backtest with regime filter + stop-loss management:

- **Profit Factor**: 2.02
- **Total trades**: 121
- **Regime filter**: NEUTRAL regime trades excluded (PF 0.93 without filter)

Out-of-sample validation still pending.
