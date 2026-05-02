import os
import sys
import json
import argparse
import logging
import asyncio
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum
from zoneinfo import ZoneInfo
import httpx
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Definitions:
# OTM = Out of the Money — a call with a strike price above the current stock price, or a put with a strike below. These are cheaper (no intrinsic value, pure premium), which is why they're leveraged directional bets. If someone is buying OTM options aggressively, they're betting on a big move.
# DTE = Days to Expiration — how many days until the options contract expires worthless or gets exercised. In the strategy, we weight 6-180 DTE highest because that sweet spot means the trader has a near-term thesis, not just a long-dated hedge.
# IV (Implied Volatility) — how expensive the option is relative to its history. High IV = pricey premium = you need a bigger move to profit.
# GEX (Gamma Exposure) — measures how much market makers need to hedge. High positive gamma = price stabilizes (pins to a strike). Negative gamma = moves accelerate.
# Sweep — a large order split across multiple exchanges simultaneously. Signals urgency — the buyer doesn't care about getting the best price, they want to get filled now.
# Volume > OI — when today's trading volume exceeds the total open interest (existing contracts). Means new positions are being opened, not old ones being closed.
# 
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

UW_BASE = "https://api.unusualwhales.com"
UW_TOKEN = os.getenv("UW_API_KEY", "")

HEADERS = {
    "Authorization": f"Bearer {UW_TOKEN}",
    "Accept": "application/json",
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

ETF_BLACKLIST = {
    "SPY", "QQQ", "IWM", "DIA", "SPX", "XSP", "VIX",
    "UVXY", "SQQQ", "TQQQ", "XLF", "XLE", "XLK", "GLD",
    "SLV", "TLT", "HYG", "EEM", "ARKK", "KWEB",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("strategy")

# -------------------------STRATEGIES--------------------------------------
# 1. Flow — Unusual sweep/block activity (UW API)
# 2. Dark Pool — Large institutional block prints (UW API)
# 3. GEX — Gamma walls as support/resistance (UW API)
# 4. IV Rank — Is volatility cheap or expensive? (UW API)
# 5. Technicals — RSI, MACD, SMA trend (UW API)
# 6. Catalyst — Earnings, FDA dates (UW API)
# 7. Social — Reddit WSB/r/stocks/r/options buzz (ApeWisdom — free, no key needed)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class Signal(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

@dataclass
class FlowScore:
    """Layer 1: Unusual options flow signals."""
    total_alerts: int = 0                          # how many unusual flow alerts were found
    bullish_sweeps: int = 0                        # how many were bullish (calls bought at ask, puts sold at bid)
    bearish_sweeps: int = 0                        # how many were bearish (puts bought at ask, calls sold at bid)
    max_premium: float = 0.0                       # biggest single order size in dollars
    total_premium: float = 0.0                     # raw total money spent across all alerts
    total_premium_weighted: float = 0.0            # DTE-weighted total premium (used for scoring)
    volume_gt_oi_count: int = 0                    # how many had volume > open interest (new positions being opened, not closing)
    signal: Signal = Signal.NEUTRAL                # the final verdict — BULLISH, BEARISH, or NEUTRAL
    score: float = 0.0                             # numeric confidence score (0-100)
    details: list = field(default_factory=list)     # raw list of the top individual alerts (>$100K)


@dataclass
class DarkpoolScore:
    """Layer 2: Dark pool activity — institutional interest level."""
    total_prints: int = 0                          # total number of dark pool trades found
    large_prints: int = 0                          # how many were over $1M notional (the ones worth paying attention to)
    total_notional: float = 0.0                    # total dollar value of all the dark pool prints combined
    activity_level: str = "LOW"                    # LOW / MODERATE / HIGH / VERY_HIGH based on print count + notional
    signal: Signal = Signal.NEUTRAL                # always NEUTRAL — dark pool shows activity, not direction
    score: float = 0.0

@dataclass
class GEXScore:
    """Layer 3: Gamma exposure — key price levels where market makers must hedge."""
    max_gamma_strike: Optional[float] = None       # the strike with the most gamma — price gets "magnetized" here
    put_wall_strike: Optional[float] = None        # strike with heaviest put gamma — acts as a support level
    call_wall_strike: Optional[float] = None       # strike with heaviest call gamma — acts as a resistance level
    gamma_flip: Optional[float] = None             # price below this = negative gamma territory = moves accelerate violently
    signal: Signal = Signal.NEUTRAL                # BULLISH if price is above max gamma (positive gamma), BEARISH if below
    score: float = 0.0                             # numeric confidence score (0-100)

@dataclass
class IVScore:
    """Layer 4: Implied volatility — how expensive are the options right now?"""
    iv_current: Optional[float] = None             # the current implied volatility as a decimal (e.g. 0.45 = 45%)
    iv_percentile: Optional[float] = None          # where IV sits vs the last year (0-100). Low = cheap options, High = expensive
    iv_rank: Optional[float] = None                # similar to percentile but weighted differently
    signal: Signal = Signal.NEUTRAL                # CONDITION layer — "BULLISH" = cheap options (good for buying calls OR puts), not a stock direction call
    score: float = 0.0                             # numeric confidence score (0-100)


@dataclass
class TechnicalScore:
    """Layer 5: Chart-based technical indicators — is the price action cooperating?"""
    rsi_14: Optional[float] = None                 # Relative Strength Index (14-period). <30 = oversold, >70 = overbought, 40-65 = sweet spot
    macd_histogram: Optional[float] = None         # MACD minus Signal line. Positive = upward momentum, Negative = downward
    sma_20: Optional[float] = None                 # 20-day Simple Moving Average — short-term trend
    sma_50: Optional[float] = None                 # 50-day Simple Moving Average — medium-term trend
    vwap: Optional[float] = None                   # Volume-Weighted Average Price — institutional fair value benchmark
    relative_volume: Optional[float] = None        # today's volume / avg volume — >1.5 = unusual activity, >2.0 = very high
    current_price: Optional[float] = None          # latest stock price
    trend_aligned: bool = False                    # True when SMA20 > SMA50 (uptrend structure)
    signal: Signal = Signal.NEUTRAL                # BULLISH if score >= 65, BEARISH if <= 35
    score: float = 0.0                             # numeric confidence score (0-100)


@dataclass
class CatalystScore:
    """Layer 6: Upcoming events that could move the stock — earnings, FDA, macro."""
    next_earnings_date: Optional[str] = None       # date string of the next earnings report (YYYY-MM-DD)
    days_to_earnings: Optional[int] = None         # how many days until earnings — <7 = imminent, use defined-risk only
    has_upcoming_catalyst: bool = False            # True if any catalyst (earnings, FDA, etc.) is on the horizon
    catalyst_type: Optional[str] = None            # what kind of catalyst — "EARNINGS", "FDA", etc.
    signal: Signal = Signal.NEUTRAL                # always NEUTRAL — catalyst measures event magnitude, not direction
    score: float = 0.0                             # numeric confidence score (0-100)


@dataclass
class SocialScore:
    """Layer 7: Social sentiment — what's buzzing on Reddit WSB, r/stocks, r/options, Twitter/X."""
    mentions_24h: int = 0                          # total mentions across all tracked subreddits in last 24 hours
    mentions_change_pct: float = 0.0               # % change in mentions vs prior 24 hours (200%+ = exploding)
    upvotes: int = 0                               # total upvotes on posts mentioning this ticker (quality signal)
    wsb_rank: Optional[int] = None                 # rank on WSB trending list (top 5 = massive retail attention)
    is_trending: bool = False                      # True if mention spike >100% or WSB rank <= 15
    signal: Signal = Signal.NEUTRAL                # BULLISH if score >= 50 (strong buzz), otherwise NEUTRAL
    score: float = 0.0                             # numeric confidence score (0-100)


@dataclass
class StrategyResult:
    """Composite result across all 7 layers — the final output."""
    ticker: str = ""                                                   # stock ticker symbol (e.g. "NVDA")
    timestamp: str = ""                                                # when this analysis was run (ISO format)
    flow: FlowScore = field(default_factory=FlowScore)                 # Layer 1 results
    darkpool: DarkpoolScore = field(default_factory=DarkpoolScore)     # Layer 2 results
    gex: GEXScore = field(default_factory=GEXScore)                    # Layer 3 results
    iv: IVScore = field(default_factory=IVScore)                       # Layer 4 results
    technicals: TechnicalScore = field(default_factory=TechnicalScore) # Layer 5 results
    catalyst: CatalystScore = field(default_factory=CatalystScore)     # Layer 6 results
    social: SocialScore = field(default_factory=SocialScore)           # Layer 7 results
    composite_score: float = 0.0                                       # weighted average of all 7 layer scores (0-100)
    direction: Signal = Signal.NEUTRAL                                 # overall direction — whichever side has more layers agreeing
    conviction: str = "NONE"                                           # human-readable confidence: NONE / LOW / MEDIUM / HIGH / VERY_HIGH
    layers_aligned: int = 0                                            # how many of the 4 directional layers agree (Flow, GEX, Technicals, Social)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def uw_get(path: str, params: dict = None) -> dict:
    """Make authenticated GET to Unusual Whales API."""
    url = f"{UW_BASE}{path}"
    try:
        resp = httpx.get(url, params=params or {}, timeout=30, headers=HEADERS)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        log.error(f"UW API error {e.response.status_code}: {path}")
        return {}
    except Exception as e:
        log.error(f"UW API request failed: {e}")
        return {}

def _float(val, default=0.0):
    """Safe float — handles None, empty strings, and non-numeric values from API."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def _int(val, default=0):
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def _str(val, default=""):
    if val is None:
        return default
    return str(val)

# ---------------------------------------------------------------------------
# Strategy/Layer 1: The "Smart Money Sweep" Scanner
# This is the bread and butter. You're looking for aggressive sweeps (multi-exchange fills = urgency) on individual stocks, not ETFs. The filters that separate signal from noise:
# # Premium > $100K (filters out retail noise)
# # Sweep orders only (urgency = conviction)
# #  Filled at the ask (buyer is aggressive, not passive)
# #  Volume > Open Interest (new positions being opened, not closing)
# #  OTM contracts (leveraged directional bet, not a hedge)
# #  DTE between 6–180 days (not too short to be gamma noise, not too long to be a LEAP hedge)
# #  Exclude ETFs/indices (SPY, QQQ flow is mostly hedging noise)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 1: Flow Analysis
# ---------------------------------------------------------------------------

def analyze_flow(ticker: str) -> FlowScore: # analyze_flow function name, takes one argument type string. and returns FlowScore dataclass defined above.
    """
    Fetch unusual flow alerts and score conviction.
    Key filters:
        - min_premium $100K (filters retail noise)
        - size_greater_oi = True (new positions, not closing)
        - is_otm = True (directional bets, not hedges)
    DTE weighting:
        - 0-5 DTE: 0.5x (gamma noise)
        - 6-180 DTE: 1.0x (sweet spot, per UW recommended range)
        - 181+ DTE: 0.25x (LEAPS, likely hedges)
        - SPX/SPY/XSP 0DTE: skipped entirely (verified noise)
    """
    fs = FlowScore()
    log.info(f"[FLOW] {ticker}: Fetching flow alerts (min_premium=$100K, OTM only, vol>OI)...")

    # Get flow alerts for ticker
    data = uw_get("/api/option-trades/flow-alerts", {
        "ticker_symbol": ticker,
        "min_premium": 100_000,
        "size_greater_oi": True,
        "is_otm": True,
        "limit": 50,
    })
    alerts = data.get("data", [])
    if not alerts:
        log.info(f"[FLOW] {ticker}: ❌ No significant flow alerts found — skipping layer")
        return fs

    today = datetime.now().date()

    # DTE buckets: 0-5 = short-term noise, 6-180 = sweet spot (UW recommended), 181+ = LEAPS/hedges
    DTE_WEIGHTS = {
        "short": 0.5,   # 0-5 DTE — gamma-dominated, noisy
        "sweet": 1.0,   # 6-180 DTE — peak signal range
        "leap":  0.25,  # 181+ DTE — almost certainly hedges
    }

    skipped_0dte_index = 0
    log.info(f"[FLOW] {ticker}: Found {len(alerts)} raw alerts to process")

    for i, alert in enumerate(alerts):
        try:
            premium = _float(alert.get("total_premium"))
            option_type = _str(alert.get("type")).upper()      # CALL or PUT
            side = _str(alert.get("side")).upper()              # ASK, BID, MID
            trade_type = _str(alert.get("trade_type")).upper() # SWEEP, BLOCK, etc.

            # Calculate DTE from expiry field
            expiry_str = alert.get("expires", "")
            strike = alert.get("strike", "?")
            dte = None
            dte_bucket = "sweet"
            if expiry_str:
                try:
                    expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
                    dte = (expiry_date - today).days
                    if dte < 0:
                        dte = 0
                    if dte <= 5:
                        dte_bucket = "short"
                    elif dte <= 180:
                        dte_bucket = "sweet"
                    else:
                        dte_bucket = "leap"
                except ValueError:
                    pass

            # Skip SPX/SPY 0DTE entirely — verified noise
            if ticker.upper() in ("SPX", "SPY", "XSP") and dte is not None and dte == 0:
                skipped_0dte_index += 1
                log.debug(f"[FLOW] {ticker}: Skipping 0DTE index flow #{i+1}")
                continue

            dte_weight = DTE_WEIGHTS[dte_bucket]
            weighted_premium = premium * dte_weight

            fs.total_premium += premium
            fs.total_premium_weighted += weighted_premium
            fs.max_premium = max(fs.max_premium, premium)

            if alert.get("size_greater_oi"):
                fs.volume_gt_oi_count += 1

            # Determine if bullish or bearish
            # Call at ask = bullish, Put at ask = bearish
            # Call at bid = bearish (selling calls), Put at bid = bullish (selling puts)
            is_bullish = (
                (option_type == "CALL" and side in ("ASK", "A")) or
                (option_type == "PUT" and side in ("BID", "B"))
            )
            is_bearish = (
                (option_type == "PUT" and side in ("ASK", "A")) or
                (option_type == "CALL" and side in ("BID", "B"))
            )

            if trade_type in ("SWEEP", "BLOCK"):
                if is_bullish:
                    fs.bullish_sweeps += 1
                elif is_bearish:
                    fs.bearish_sweeps += 1

            # Log every significant trade individually
            sentiment = "BULL" if is_bullish else "BEAR" if is_bearish else "NEUTRAL"
            expiry = expiry_str or "?"
            vol = alert.get("volume", "?")
            oi = alert.get("open_interest", "?")
            dte_label = f"{dte}d" if dte is not None else "?"

            if premium >= 100_000:
                log.info(
                    f"[FLOW] {ticker}: 🔔 BIG PRINT #{i+1} — {trade_type} {option_type} "
                    f"${premium:,.0f} (wt ${weighted_premium:,.0f}) | Strike={strike} Exp={expiry} DTE={dte_label} [{dte_bucket}] | "
                    f"Side={side} → {sentiment} | Vol={vol} OI={oi}"
                )
                fs.details.append({
                    "type": option_type,
                    "trade_type": trade_type,
                    "premium": premium,
                    "weighted_premium": weighted_premium,
                    "strike": strike,
                    "expiry": expiry,
                    "dte": dte,
                    "dte_bucket": dte_bucket,
                    "side": side,
                    "sentiment": sentiment,
                })
            elif premium >= 50_000:
                log.debug(
                    f"[FLOW] {ticker}: Print #{i+1} — {trade_type} {option_type} "
                    f"${premium:,.0f} (wt ${weighted_premium:,.0f}) | Strike={strike} DTE={dte_label} [{dte_bucket}] | {sentiment}"
                )
        except Exception as e:
            log.warning(f"[FLOW] {ticker}: Skipping bad alert #{i+1}: {e}")
            continue

    if skipped_0dte_index > 0:
        log.info(f"[FLOW] {ticker}: Skipped {skipped_0dte_index} 0DTE index prints (noise)")

    fs.total_alerts = len(alerts) - skipped_0dte_index

    # Score: weighted by premium size, sweep count, and directional bias
    total_directional = fs.bullish_sweeps + fs.bearish_sweeps
    log.info(
        f"[FLOW] {ticker}: Directional sweeps/blocks: "
        f"Bull={fs.bullish_sweeps} vs Bear={fs.bearish_sweeps} "
        f"(total={total_directional})"
    )
    log.info(
        f"[FLOW] {ticker}: Vol>OI count={fs.volume_gt_oi_count} "
        f"(new positions being opened)"
    )

    if total_directional > 0:
        bull_ratio = fs.bullish_sweeps / total_directional
        log.info(f"[FLOW] {ticker}: Bull ratio = {bull_ratio:.1%}")
        if bull_ratio > 0.65:
            fs.signal = Signal.BULLISH
            log.info(f"[FLOW] {ticker}: → BULLISH (bull ratio {bull_ratio:.1%} > 65% threshold)")
        elif bull_ratio < 0.35:
            fs.signal = Signal.BEARISH
            log.info(f"[FLOW] {ticker}: → BEARISH (bull ratio {bull_ratio:.1%} < 35% threshold)")
        else:
            fs.signal = Signal.NEUTRAL
            log.info(f"[FLOW] {ticker}: → NEUTRAL (bull ratio {bull_ratio:.1%} between 35-65%)")
    else:
        bull_ratio = 0.5

    # Score components (0-100) — uses DTE-weighted premium for scoring
    premium_score = min(fs.total_premium_weighted / 5_000_000 * 100, 100)  # $5M+ = max
    sweep_score = min(total_directional / 10 * 100, 100)           # 10+ sweeps = max
    conviction_score = abs(bull_ratio - 0.5) * 200 if total_directional > 0 else 0
    fs.score = (premium_score * 0.4 + sweep_score * 0.3 + conviction_score * 0.3)

    log.info(
        f"[FLOW] {ticker}: Score breakdown — "
        f"premium_score={premium_score:.1f} (40% weight) | "
        f"sweep_score={sweep_score:.1f} (30% weight) | "
        f"conviction_score={conviction_score:.1f} (30% weight)"
    )
    log.info(
        f"[FLOW] {ticker}: ✅ FINAL — {fs.total_alerts} alerts | "
        f"Bull={fs.bullish_sweeps} Bear={fs.bearish_sweeps} | "
        f"MaxPrem=${fs.max_premium:,.0f} RawPrem=${fs.total_premium:,.0f} WtPrem=${fs.total_premium_weighted:,.0f} | "
        f"Score={fs.score:.1f}/100 | Signal={fs.signal.value}"
    )
    return fs

# ---------------------------------------------------------------------------
# Strategy/Layer 2: Dark Pool Activity Scanner
# Institutional players (hedge funds, pension funds, banks) use dark pools to
# execute massive trades without moving the public market price. The trades get
# reported after the fact.
# IMPORTANT: Dark pool prints do NOT reliably indicate direction. Most trades
# execute at or near the NBBO midpoint by design, so buy/sell inference is weak.
# Instead, we use dark pool as a VOLUME/ACTIVITY indicator:
# # Heavy dark pool activity = institutions are positioning (something is happening)
# # Light activity = no institutional interest
# How it works:
# # Fetch all dark pool prints for the ticker
# # Flag "large prints" — trades with notional value > $1M
# # Score based on activity level (print count + notional size)
# # Signal is always NEUTRAL — dark pool confirms institutional interest,
# #   not direction. Direction comes from flow (Layer 1) and technicals (Layer 5).
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 2: Dark Pool Analysis
# ---------------------------------------------------------------------------

def analyze_darkpool(ticker: str) -> DarkpoolScore:
    """
    Measure institutional dark pool activity level.
    Dark pool direction is structurally unreliable (most prints execute at midpoint),
    so we use this as an activity/interest indicator, not a directional signal.
    High activity = institutions are positioning. Direction comes from other layers.
    """
    ds = DarkpoolScore()
    log.info(f"[DARK] {ticker}: Fetching dark pool block prints...")

    data = uw_get(f"/api/darkpool/{ticker}")
    prints = data.get("data", [])
    if not prints:
        log.info(f"[DARK] {ticker}: ❌ No dark pool data returned — skipping layer")
        return ds

    ds.total_prints = len(prints)
    log.info(f"[DARK] {ticker}: Found {ds.total_prints} dark pool prints to analyze")

    for i, p in enumerate(prints):
        try:
            volume = _float(p.get("volume"))
            price = _float(p.get("price"))
            notional = volume * price
            ds.total_notional += notional
            trade_date = p.get("date", p.get("tracking_timestamp", "?"))

            if notional >= 1_000_000:
                ds.large_prints += 1
                log.info(
                    f"[DARK] {ticker}: 🐋 LARGE PRINT #{ds.large_prints} — "
                    f"${notional:,.0f} notional | {volume:,.0f} shares @ ${price:.2f} | "
                    f"Date={trade_date}"
                )
            elif notional >= 500_000:
                log.debug(
                    f"[DARK] {ticker}: Medium print — "
                    f"${notional:,.0f} notional | {volume:,.0f} shares @ ${price:.2f}"
                )
        except Exception as e:
            log.warning(f"[DARK] {ticker}: Skipping bad print #{i+1}: {e}")
            continue

    # Score based on activity level (print count + notional size)
    log.info(
        f"[DARK] {ticker}: Summary — {ds.large_prints} large prints (>$1M) "
        f"out of {ds.total_prints} total | Total notional=${ds.total_notional:,.0f}"
    )

    # Activity scoring: combines print count and notional size
    print_score = min(ds.large_prints / 10 * 100, 100)
    notional_score = min(ds.total_notional / 50_000_000 * 100, 100)
    ds.score = print_score * 0.6 + notional_score * 0.4

    if ds.large_prints >= 8 or ds.total_notional >= 50_000_000:
        ds.activity_level = "VERY_HIGH"
    elif ds.large_prints >= 5 or ds.total_notional >= 20_000_000:
        ds.activity_level = "HIGH"
    elif ds.large_prints >= 2 or ds.total_notional >= 5_000_000:
        ds.activity_level = "MODERATE"
    else:
        ds.activity_level = "LOW"

    # Dark pool is always NEUTRAL — it measures institutional interest, not direction
    ds.signal = Signal.NEUTRAL
    log.info(
        f"[DARK] {ticker}: → Activity={ds.activity_level} "
        f"(direction not inferred — dark pool midpoint data is structurally unreliable)"
    )

    log.info(
        f"[DARK] {ticker}: ✅ FINAL — {ds.total_prints} prints | "
        f"Large(>$1M)={ds.large_prints} | Activity={ds.activity_level} | "
        f"Notional=${ds.total_notional:,.0f} | Score={ds.score:.1f}/100"
    )
    return ds

# ---------------------------------------------------------------------------
# Strategy/Layer 3: Gamma Exposure (GEX) — Market Maker Positioning
# Market makers who sell options must hedge by buying/selling shares. Gamma
# measures how aggressively they need to hedge as price moves. This creates
# invisible "walls" in the market:
# Key levels:
# # Max Gamma Strike — the strike with the most combined gamma. Price gets
# #   "magnetized" here because MM hedging dampens moves in both directions.
# # Call Wall — strike with highest call gamma. Acts as resistance (hard ceiling).
# # Put Wall — strike with highest put gamma. Acts as support (hard floor).
# # Gamma Flip — below this level, MMs switch from dampening moves to
# #   accelerating them (negative gamma = violent selloffs).
# Signal logic:
# # Price ABOVE max gamma strike = BULLISH (positive gamma, stable, supportive)
# # Price BELOW max gamma strike = BEARISH (could accelerate down)
# # No price context available = NEUTRAL
# Scoring:
# # Dynamic — scales with distance from max gamma strike. Closer = stronger signal.
# # Bonus if price is between put wall (support) and call wall (resistance).
# # Score range: 30-100 depending on proximity to key levels.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 3: Gamma Exposure (GEX)
# ---------------------------------------------------------------------------

def analyze_gex(ticker: str, current_price: float = None) -> GEXScore:
    """
    Identify key gamma levels:
        - Max gamma strike = price magnet (stocks pin here)
        - Put wall = support level
        - Call wall = resistance level
        - Gamma flip = below this, moves accelerate (negative gamma)
    """
    gs = GEXScore()
    log.info(f"[GEX] {ticker}: Fetching gamma exposure by strike...")
    if current_price:
        log.info(f"[GEX] {ticker}: Current price context: ${current_price:.2f}")

    data = uw_get(f"/api/stock/{ticker}/spot-exposures/strike")
    levels = data.get("data", [])
    if not levels:
        log.info(f"[GEX] {ticker}: ❌ No GEX data returned — skipping layer")
        return gs

    log.info(f"[GEX] {ticker}: Found {len(levels)} strike levels with gamma data")

    max_call_gex = 0
    max_put_gex = 0
    max_total_gex = 0
    strike_net_gex = []

    for level in levels:
        try:
            strike = _float(level.get("strike"))
            raw_call_gex = _float(level.get("call_gamma_oi"))
            raw_put_gex = _float(level.get("put_gamma_oi"))
            call_gex_abs = abs(raw_call_gex)
            put_gex_abs = abs(raw_put_gex)
            net_gex = raw_call_gex - raw_put_gex
            total_gex = call_gex_abs + put_gex_abs

            strike_net_gex.append((strike, net_gex))

            # Max gamma strike (biggest combined exposure)
            if total_gex > max_total_gex:
                max_total_gex = total_gex
                gs.max_gamma_strike = strike
                log.debug(f"[GEX] {ticker}: New max gamma strike=${strike} (total_gex={total_gex:.0f})")

            # Call wall = strike with highest call gamma
            if call_gex_abs > max_call_gex:
                max_call_gex = call_gex_abs
                gs.call_wall_strike = strike

            # Put wall = strike with highest put gamma
            if put_gex_abs > max_put_gex:
                max_put_gex = put_gex_abs
                gs.put_wall_strike = strike
        except Exception as e:
            log.warning(f"[GEX] {ticker}: Skipping bad strike level: {e}")
            continue

    # Gamma flip — the strike where net GEX crosses from positive to negative
    # Below this level, dealers are short gamma and their hedging amplifies moves
    strike_net_gex.sort(key=lambda x: x[0])
    for j in range(len(strike_net_gex) - 1):
        s_low, gex_low = strike_net_gex[j]
        s_high, gex_high = strike_net_gex[j + 1]
        if gex_low > 0 and gex_high <= 0:
            if gex_low != gex_high:
                gs.gamma_flip = s_low + (s_high - s_low) * (gex_low / (gex_low - gex_high))
            else:
                gs.gamma_flip = s_low
            log.info(f"[GEX] {ticker}: Gamma flip level = ${gs.gamma_flip:.2f} (net GEX crosses zero)")
            break

    if gs.gamma_flip is None:
        log.info(f"[GEX] {ticker}: No gamma flip detected (net GEX does not cross zero in available strikes)")

    log.info(f"[GEX] {ticker}: Key levels identified:")
    log.info(f"[GEX] {ticker}:   Max Gamma Strike = ${gs.max_gamma_strike} (price magnet — stock pins here)")
    log.info(f"[GEX] {ticker}:   Call Wall        = ${gs.call_wall_strike} (resistance — hard to break above)")
    log.info(f"[GEX] {ticker}:   Put Wall         = ${gs.put_wall_strike} (support — hard to break below)")
    log.info(f"[GEX] {ticker}:   Gamma Flip       = ${gs.gamma_flip} (below = negative gamma, moves accelerate)")

    # Signal based on price position relative to gamma levels
    if current_price and gs.max_gamma_strike:
        distance_pct = ((current_price - gs.max_gamma_strike) / gs.max_gamma_strike) * 100

        # Check if price is between put wall (support) and call wall (resistance)
        between_walls = (
            gs.put_wall_strike and gs.call_wall_strike and
            gs.put_wall_strike <= current_price <= gs.call_wall_strike
        )

        if current_price > gs.max_gamma_strike:
            gs.signal = Signal.BULLISH
            gs.score = max(30, 80 - abs(distance_pct) * 3)
            log.info(
                f"[GEX] {ticker}: → BULLISH — price ${current_price:.2f} is ABOVE max gamma "
                f"${gs.max_gamma_strike} ({distance_pct:+.1f}%) — positive gamma territory, "
                f"moves dampened, supportive"
            )
        elif current_price < gs.max_gamma_strike:
            gs.signal = Signal.BEARISH
            gs.score = max(30, 80 - abs(distance_pct) * 3)
            log.info(
                f"[GEX] {ticker}: → BEARISH — price ${current_price:.2f} is BELOW max gamma "
                f"${gs.max_gamma_strike} ({distance_pct:+.1f}%) — could accelerate down"
            )
        else:
            gs.signal = Signal.NEUTRAL
            gs.score = 75
            log.info(
                f"[GEX] {ticker}: → NEUTRAL — price ${current_price:.2f} PINNED at max gamma "
                f"${gs.max_gamma_strike} — gamma magnet confirmed, range-bound"
            )

        if gs.gamma_flip and current_price < gs.gamma_flip:
            gs.score = min(gs.score + 15, 100)
            log.info(
                f"[GEX] {ticker}: Price below gamma flip ${gs.gamma_flip:.2f} — "
                f"negative gamma territory, moves accelerate. +15 → {gs.score:.1f}"
            )

        if between_walls and gs.signal != Signal.BEARISH:
            gs.score = min(gs.score + 15, 100)
            log.info(
                f"[GEX] {ticker}: Price is between put wall ${gs.put_wall_strike} "
                f"and call wall ${gs.call_wall_strike} — range-bound, +15 bonus"
            )
        elif between_walls and gs.signal == Signal.BEARISH:
            log.info(
                f"[GEX] {ticker}: Price is between walls but BEARISH — "
                f"put wall ${gs.put_wall_strike} provides support, no bonus"
            )

        gs.score = max(0, min(100, gs.score))
    else:
        log.info(f"[GEX] {ticker}: → NEUTRAL — no price context to determine position relative to gamma levels")

    log.info(
        f"[GEX] {ticker}: ✅ FINAL — MaxGamma=${gs.max_gamma_strike} | "
        f"CallWall=${gs.call_wall_strike} | PutWall=${gs.put_wall_strike} | "
        f"GammaFlip=${gs.gamma_flip} | "
        f"Score={gs.score:.1f}/100 | Signal={gs.signal.value}"
    )
    return gs

# ---------------------------------------------------------------------------
# Strategy/Layer 4: Implied Volatility (IV) — Are Options Cheap or Expensive?
# IV tells you how much "fear" or "uncertainty" is priced into the options.
# High IV = expensive premiums = you need a bigger move to profit.
# Low IV = cheap premiums = options are on sale.
# IMPORTANT: IV is a CONDITION layer, not directional. "BULLISH" here means
# "cheap options, good for buying calls OR puts" — it does NOT signal stock direction.
# How it works:
# # Fetch IV data and look at the IV Percentile (where current IV sits vs last 52 weeks)
# # IV Percentile <= 30 = CHEAP (score 80) — great time to buy options outright
# # IV Percentile <= 50 = REASONABLE (score 60) — options fairly priced
# # IV Percentile <= 70 = ELEVATED (score 40) — use spreads to offset IV crush
# # IV Percentile > 70 = EXPENSIVE (score 20) — high crush risk, sell premium
# #   or use debit spreads. Naked calls/puts will lose money even if direction is right.
# Why it matters:
# # You can be RIGHT on direction and still LOSE money if IV was too high when you
# #   entered. IV crush after earnings/events destroys option value.
# # This layer tells you HOW to trade, not just WHETHER to trade.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 4: IV Rank / Percentile
# ---------------------------------------------------------------------------

def analyze_iv(ticker: str) -> IVScore:
    """
    Check implied volatility percentile.
    IV < 30th percentile = cheap options (good for buying)
    IV > 70th percentile = expensive (better to sell premium or use spreads)
    """
    ivs = IVScore()
    log.info(f"[IV] {ticker}: Fetching implied volatility data...")

    data = uw_get(f"/api/stock/{ticker}/interpolated-iv")
    iv_data = data.get("data", [])
    if not iv_data:
        log.info(f"[IV] {ticker}: ❌ No IV data returned — skipping layer")
        return ivs

    log.info(f"[IV] {ticker}: Received {len(iv_data)} IV data points")

    # Get the most recent IV data point
    latest = iv_data[-1] if iv_data else {}
    ivs.iv_current = _float(latest.get("implied_volatility"))
    ivs.iv_percentile = _float(latest.get("iv_percentile"), 50)
    ivs.iv_rank = _float(latest.get("iv_rank"), 50)

    log.info(f"[IV] {ticker}: Current IV = {ivs.iv_current:.1%}")
    log.info(f"[IV] {ticker}: IV Percentile = {ivs.iv_percentile:.0f}/100 (where IV sits vs last 52 weeks)")
    log.info(f"[IV] {ticker}: IV Rank = {ivs.iv_rank:.0f}/100")

    # Score using BOTH IV percentile and IV rank for robustness
    # IV percentile handles outlier spikes better, IV rank is more responsive
    avg_iv = (ivs.iv_percentile + ivs.iv_rank) / 2
    log.info(f"[IV] {ticker}: Combined IV metric = {avg_iv:.0f} (avg of percentile {ivs.iv_percentile:.0f} + rank {ivs.iv_rank:.0f})")

    if avg_iv <= 30:
        ivs.signal = Signal.BULLISH
        ivs.score = 80
        log.info(
            f"[IV] {ticker}: → BULLISH — Combined IV {avg_iv:.0f} is CHEAP (≤30). "
            f"Options are underpriced. Good time to buy calls/puts outright."
        )
    elif avg_iv <= 50:
        ivs.signal = Signal.BULLISH
        ivs.score = 60
        log.info(
            f"[IV] {ticker}: → BULLISH (moderate) — Combined IV {avg_iv:.0f} is below average (≤50). "
            f"Options are reasonably priced."
        )
    elif avg_iv <= 70:
        ivs.signal = Signal.NEUTRAL
        ivs.score = 40
        log.info(
            f"[IV] {ticker}: → NEUTRAL — Combined IV {avg_iv:.0f} is elevated (50-70). "
            f"Consider spreads instead of naked options to offset IV crush."
        )
    else:
        ivs.signal = Signal.BEARISH
        ivs.score = 20
        log.info(
            f"[IV] {ticker}: → BEARISH — Combined IV {avg_iv:.0f} is EXPENSIVE (>70). "
            f"⚠️ High IV crush risk! Use debit spreads or sell premium. "
            f"Naked calls/puts will likely lose money even if direction is right."
        )

    log.info(
        f"[IV] {ticker}: ✅ FINAL — IV={ivs.iv_current:.1%} | "
        f"Percentile={ivs.iv_percentile:.0f} | Rank={ivs.iv_rank:.0f} | "
        f"Score={ivs.score:.1f}/100 | Signal={ivs.signal.value}"
    )
    return ivs


# ---------------------------------------------------------------------------
# Strategy/Layer 5: Technical Indicators — Is the Chart Cooperating?
# Pure price action analysis. Even if smart money is buying, you want the chart
# to confirm. Fighting the trend = fighting gravity.
# Indicators used:
# # RSI(14) — Relative Strength Index. Measures momentum on a 0-100 scale.
# #   <30 = oversold (potential bounce, +10 pts)
# #   40-65 = sweet spot (room to run, +15 pts)
# #   >70 = overbought danger zone (-20 pts)
# # MACD Histogram — momentum direction. Positive = upward momentum (+15 pts)
# # SMA(20) vs SMA(50) — trend confirmation.
# #   SMA20 > SMA50 = confirmed uptrend (+15 pts)
# #   SMA20 < SMA50 = downtrend (no bonus)
# # VWAP — Volume-Weighted Average Price. Institutional fair value benchmark.
# #   Price above VWAP = buyers in control (+10 pts)
# #   Price below VWAP = sellers in control (-5 pts)
# # Relative Volume (RVOL) — today's volume vs 20-day average.
# #   >= 2.0x = very high institutional participation (+15 pts)
# #   >= 1.5x = elevated, above average (+10 pts)
# Scoring:
# # Starts at base 50 (neutral), adds/subtracts based on indicators above
# # Final score clamped to 0-100
# # Score >= 65 = BULLISH, <= 35 = BEARISH, between = NEUTRAL
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 5: Technical Indicators
# ---------------------------------------------------------------------------

def analyze_technicals(ticker: str, current_price: float = None) -> TechnicalScore:
    """
    RSI(14) + SMA(20) vs SMA(50) trend alignment.
    
    Bullish setup:
        - RSI between 40-65 (not overbought, has room to run)
        - Price > SMA20 > SMA50 (uptrend confirmed)
        - MACD histogram positive (momentum)
    """
    ts = TechnicalScore()
    ts.current_price = current_price
    log.info(f"[TECH] {ticker}: Fetching technical indicators (RSI, MACD, SMA20, SMA50)...")

    # RSI
    log.info(f"[TECH] {ticker}: Requesting RSI(14)...")
    rsi_data = uw_get(f"/api/stock/{ticker}/technical-indicator/RSI", {
        "interval": "daily",
        "time_period": 14,
        "series_type": "close",
    })
    rsi_points = rsi_data.get("data", [])
    if rsi_points:
        ts.rsi_14 = _float(rsi_points[-1].get("value"), 50)
        if ts.rsi_14 < 30:
            log.info(f"[TECH] {ticker}: RSI(14) = {ts.rsi_14:.1f} — OVERSOLD (below 30, potential bounce)")
        elif ts.rsi_14 > 70:
            log.info(f"[TECH] {ticker}: RSI(14) = {ts.rsi_14:.1f} — OVERBOUGHT (above 70, risky to enter)")
        elif 40 <= ts.rsi_14 <= 65:
            log.info(f"[TECH] {ticker}: RSI(14) = {ts.rsi_14:.1f} — SWEET SPOT (40-65, room to run)")
        else:
            log.info(f"[TECH] {ticker}: RSI(14) = {ts.rsi_14:.1f}")
    else:
        log.warning(f"[TECH] {ticker}: ⚠️ RSI data unavailable")

    # MACD
    log.info(f"[TECH] {ticker}: Requesting MACD...")
    macd_data = uw_get(f"/api/stock/{ticker}/technical-indicator/MACD", {
        "interval": "daily",
        "series_type": "close",
    })
    macd_points = macd_data.get("data", [])
    if macd_points:
        latest_macd = macd_points[-1]
        macd_val = _float(latest_macd.get("macd"))
        signal_val = _float(latest_macd.get("signal"))
        ts.macd_histogram = macd_val - signal_val
        if ts.macd_histogram > 0:
            log.info(
                f"[TECH] {ticker}: MACD histogram = {ts.macd_histogram:+.4f} — POSITIVE MOMENTUM "
                f"(MACD={macd_val:.4f} > Signal={signal_val:.4f})"
            )
        else:
            log.info(
                f"[TECH] {ticker}: MACD histogram = {ts.macd_histogram:+.4f} — NEGATIVE MOMENTUM "
                f"(MACD={macd_val:.4f} < Signal={signal_val:.4f})"
            )
    else:
        log.warning(f"[TECH] {ticker}: ⚠️ MACD data unavailable")

    # SMA 20
    log.info(f"[TECH] {ticker}: Requesting SMA(20)...")
    sma20_data = uw_get(f"/api/stock/{ticker}/technical-indicator/SMA", {
        "interval": "daily",
        "time_period": 20,
        "series_type": "close",
    })
    sma20_points = sma20_data.get("data", [])
    if sma20_points:
        ts.sma_20 = _float(sma20_points[-1].get("value"))
        log.info(f"[TECH] {ticker}: SMA(20) = ${ts.sma_20:.2f}")
    else:
        log.warning(f"[TECH] {ticker}: ⚠️ SMA(20) data unavailable")

    # SMA 50
    log.info(f"[TECH] {ticker}: Requesting SMA(50)...")
    sma50_data = uw_get(f"/api/stock/{ticker}/technical-indicator/SMA", {
        "interval": "daily",
        "time_period": 50,
        "series_type": "close",
    })
    sma50_points = sma50_data.get("data", [])
    if sma50_points:
        ts.sma_50 = _float(sma50_points[-1].get("value"))
        log.info(f"[TECH] {ticker}: SMA(50) = ${ts.sma_50:.2f}")
    else:
        log.warning(f"[TECH] {ticker}: ⚠️ SMA(50) data unavailable")

    # VWAP — institutional fair value. Price above VWAP = buyers in control.
    log.info(f"[TECH] {ticker}: Requesting VWAP...")
    vwap_data = uw_get(f"/api/stock/{ticker}/technical-indicator/VWAP", {
        "interval": "daily",
    })
    vwap_points = vwap_data.get("data", [])
    if vwap_points:
        ts.vwap = _float(vwap_points[-1].get("value"))
        log.info(f"[TECH] {ticker}: VWAP = ${ts.vwap:.2f}")
    else:
        log.warning(f"[TECH] {ticker}: ⚠️ VWAP data unavailable")

    # Relative Volume — confirms institutional participation behind moves
    log.info(f"[TECH] {ticker}: Requesting volume data...")
    vol_data = uw_get(f"/api/stock/{ticker}/volume")
    vol_points = vol_data.get("data", [])
    if vol_points and len(vol_points) >= 21:
        today_vol = _float(vol_points[-1].get("volume"))
        avg_vol = sum(_float(v.get("volume")) for v in vol_points[-21:-1]) / 20
        if avg_vol > 0:
            ts.relative_volume = today_vol / avg_vol
            if ts.relative_volume >= 2.0:
                log.info(f"[TECH] {ticker}: Relative Volume = {ts.relative_volume:.2f}x — VERY HIGH (institutional participation)")
            elif ts.relative_volume >= 1.5:
                log.info(f"[TECH] {ticker}: Relative Volume = {ts.relative_volume:.2f}x — ELEVATED (above average)")
            else:
                log.info(f"[TECH] {ticker}: Relative Volume = {ts.relative_volume:.2f}x — NORMAL")
    else:
        log.warning(f"[TECH] {ticker}: ⚠️ Volume data unavailable or insufficient")

    # Trend alignment
    if ts.sma_20 and ts.sma_50:
        ts.trend_aligned = ts.sma_20 > ts.sma_50
        if ts.trend_aligned:
            spread = ((ts.sma_20 - ts.sma_50) / ts.sma_50) * 100
            log.info(
                f"[TECH] {ticker}: Trend = UPTREND ✅ — SMA20 ${ts.sma_20:.2f} > SMA50 ${ts.sma_50:.2f} "
                f"(spread={spread:+.2f}%)"
            )
        else:
            spread = ((ts.sma_20 - ts.sma_50) / ts.sma_50) * 100
            log.info(
                f"[TECH] {ticker}: Trend = DOWNTREND ❌ — SMA20 ${ts.sma_20:.2f} < SMA50 ${ts.sma_50:.2f} "
                f"(spread={spread:+.2f}%)"
            )
    else:
        log.info(f"[TECH] {ticker}: Cannot determine trend — missing SMA data")

    # Scoring
    score = 50  # neutral base
    log.info(f"[TECH] {ticker}: Scoring — starting at base=50")

    if ts.rsi_14 is not None:
        if 40 <= ts.rsi_14 <= 65:
            score += 15
            log.info(f"[TECH] {ticker}: Scoring — RSI in sweet spot (40-65): +15 → {score}")
        elif ts.rsi_14 < 30:
            score += 10
            log.info(f"[TECH] {ticker}: Scoring — RSI oversold bounce potential: +10 → {score}")
        elif ts.rsi_14 > 70:
            score -= 20
            log.info(f"[TECH] {ticker}: Scoring — RSI overbought danger zone: -20 → {score}")
        else:
            log.info(f"[TECH] {ticker}: Scoring — RSI neutral range: +0 → {score}")

    if ts.macd_histogram is not None and ts.macd_histogram > 0:
        score += 15
        log.info(f"[TECH] {ticker}: Scoring — MACD positive momentum: +15 → {score}")
    elif ts.macd_histogram is not None and ts.macd_histogram < 0:
        score -= 10
        log.info(f"[TECH] {ticker}: Scoring — MACD negative momentum: -10 → {score}")

    if ts.sma_20 and ts.sma_50:
        if ts.trend_aligned:
            score += 15
            log.info(f"[TECH] {ticker}: Scoring — Uptrend confirmed (SMA20>SMA50): +15 → {score}")
        else:
            score -= 10
            log.info(f"[TECH] {ticker}: Scoring — Downtrend (SMA20<SMA50): -10 → {score}")

    # VWAP — price above VWAP = buyers in control
    if ts.vwap and ts.current_price:
        if ts.current_price > ts.vwap:
            score += 10
            log.info(f"[TECH] {ticker}: Scoring — Price above VWAP (buyers in control): +10 → {score}")
        else:
            score -= 5
            log.info(f"[TECH] {ticker}: Scoring — Price below VWAP (sellers in control): -5 → {score}")
    elif ts.vwap:
        log.info(f"[TECH] {ticker}: Scoring — VWAP available but no price context: +0 → {score}")

    # Relative volume — confirms institutional participation
    if ts.relative_volume:
        if ts.relative_volume >= 2.0:
            score += 15
            log.info(f"[TECH] {ticker}: Scoring — Very high relative volume ({ts.relative_volume:.1f}x): +15 → {score}")
        elif ts.relative_volume >= 1.5:
            score += 10
            log.info(f"[TECH] {ticker}: Scoring — Elevated relative volume ({ts.relative_volume:.1f}x): +10 → {score}")
        else:
            log.info(f"[TECH] {ticker}: Scoring — Normal relative volume ({ts.relative_volume:.1f}x): +0 → {score}")

    ts.score = max(0, min(100, score))

    # Signal
    if ts.score >= 65:
        ts.signal = Signal.BULLISH
        log.info(f"[TECH] {ticker}: → BULLISH (score {ts.score:.0f} ≥ 65)")
    elif ts.score <= 35:
        ts.signal = Signal.BEARISH
        log.info(f"[TECH] {ticker}: → BEARISH (score {ts.score:.0f} ≤ 35)")
    else:
        ts.signal = Signal.NEUTRAL
        log.info(f"[TECH] {ticker}: → NEUTRAL (score {ts.score:.0f} between 35-65)")

    log.info(
        f"[TECH] {ticker}: ✅ FINAL — RSI={ts.rsi_14} | "
        f"MACD_hist={ts.macd_histogram} | "
        f"SMA20=${ts.sma_20} SMA50=${ts.sma_50} | "
        f"VWAP=${ts.vwap} | RVOL={ts.relative_volume} | "
        f"Trend={'UP' if ts.trend_aligned else 'DOWN'} | "
        f"Score={ts.score:.1f}/100 | Signal={ts.signal.value}"
    )
    return ts

# ---------------------------------------------------------------------------
# Strategy/Layer 6: Catalyst Detection — What Could Move the Stock?
# Options are time-decaying assets. You need something to MOVE the stock before
# your contract expires. Catalysts are those somethings.
# IMPORTANT: Catalyst is direction-AGNOSTIC — it measures event magnitude (how
# likely is a big move), NOT direction. Direction comes from flow (Layer 1).
# Sources: OptionStrat, SpotGamma, Schwab all treat catalysts as vol events.
# What it checks:
# # Earnings calendar — when is the next earnings report?
# #   <= 7 days + directional flow = IMMINENT (score 90) — high magnitude
# #   <= 7 days + mixed flow = IMMINENT (score 50) — likely hedging
# #   <= 14 days = APPROACHING (score 50-70) — pre-earnings positioning window
# #   <= 30 days = ON RADAR (score 50) — monitor, not actionable yet
# #   > 30 days = TOO FAR (score 20) — not a near-term catalyst
# # FDA calendar — biotech PDUFA dates, drug approvals, advisory committees
# #   Found = high magnitude (score 80) — binary event
# Signal is always NEUTRAL — direction inherited from other layers.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 6: Catalyst Detection
# ---------------------------------------------------------------------------

def analyze_catalyst(ticker: str, flow: FlowScore = None) -> CatalystScore:
    """
    Check for upcoming earnings or other catalysts.
    Direction-agnostic: scores event MAGNITUDE (how likely is a big move), not direction.
    Direction comes from flow (Layer 1). Uses flow context to distinguish positioning from hedging.
    """
    cs = CatalystScore()
    log.info(f"[CAT] {ticker}: Checking for upcoming catalysts (earnings, FDA)...")

    # Check earnings
    log.info(f"[CAT] {ticker}: Fetching earnings calendar...")
    earnings_data = uw_get(f"/api/earnings/{ticker}")
    earnings = earnings_data.get("data", [])

    if earnings:
        log.info(f"[CAT] {ticker}: Found {len(earnings)} earnings records")
        today = datetime.now().date()

        # Find nearest future earnings date
        nearest_date = None
        for e in earnings:
            date_str = e.get("date", "")
            if not date_str:
                continue
            try:
                earn_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
                if earn_date >= today:
                    if nearest_date is None or earn_date < nearest_date:
                        nearest_date = earn_date
            except ValueError:
                continue
        if nearest_date:
            cs.next_earnings_date = nearest_date.isoformat()
            cs.days_to_earnings = (nearest_date - today).days
            log.info(
                f"[CAT] {ticker}: Next earnings = {cs.next_earnings_date} "
                f"({cs.days_to_earnings} days away)"
            )

        if cs.next_earnings_date is None:
            log.info(f"[CAT] {ticker}: No upcoming earnings dates found (all are in the past)")
    else:
        log.info(f"[CAT] {ticker}: No earnings data returned from API")

    # Also check FDA calendar for biotech
    log.info(f"[CAT] {ticker}: Checking FDA calendar for biotech catalysts...")
    fda_data = uw_get("/api/market/fda-calendar")
    fda_events = fda_data.get("data", [])
    fda_found = False
    for event in fda_events:
        if _str(event.get("ticker")).upper() == ticker.upper():
            cs.has_upcoming_catalyst = True
            cs.catalyst_type = "FDA"
            fda_found = True
            fda_date = event.get("date", "unknown")
            fda_drug = event.get("drug_name", event.get("description", "unknown"))
            log.info(
                f"[CAT] {ticker}: 🧬 FDA CATALYST FOUND — "
                f"Date={fda_date} | Drug/Event={fda_drug}"
            )
            break
    if not fda_found:
        log.info(f"[CAT] {ticker}: No FDA catalysts found")

    # Score based on catalyst proximity + flow context
    # Catalyst is direction-AGNOSTIC — it measures magnitude (how likely is a big move),
    # not direction. Direction comes from flow (Layer 1). Sources: OptionStrat, SpotGamma, Schwab.
    flow_is_directional = (
        flow and flow.signal != Signal.NEUTRAL and
        (flow.bullish_sweeps + flow.bearish_sweeps) >= 3
    )
    flow_is_mixed = flow and flow.signal == Signal.NEUTRAL and flow.total_alerts > 0

    if flow_is_directional:
        log.info(f"[CAT] {ticker}: Flow context: DIRECTIONAL ({flow.signal.value}) — likely positioning, not hedging")
    elif flow_is_mixed:
        log.info(f"[CAT] {ticker}: Flow context: MIXED/NEUTRAL — could be hedging or vol bets (straddles/strangles)")
    else:
        log.info(f"[CAT] {ticker}: Flow context: NO FLOW DATA — cannot distinguish hedge from directional")

    # Signal is always NEUTRAL — catalyst measures event magnitude, not direction
    cs.signal = Signal.NEUTRAL

    if cs.days_to_earnings is not None:
        if cs.days_to_earnings <= 7:
            cs.has_upcoming_catalyst = True
            cs.catalyst_type = cs.catalyst_type or "EARNINGS"
            if flow_is_directional:
                cs.score = 90
                log.info(
                    f"[CAT] {ticker}: Earnings in {cs.days_to_earnings}d + DIRECTIONAL flow ({flow.signal.value})! "
                    f"Score=90 — high magnitude event. ⚠️ Use defined-risk only (spreads)!"
                )
            elif flow_is_mixed:
                cs.score = 50
                log.info(
                    f"[CAT] {ticker}: Earnings in {cs.days_to_earnings}d but flow is MIXED. "
                    f"Score=50 — likely hedging or vol bets, not directional conviction."
                )
            else:
                cs.score = 60
                log.info(
                    f"[CAT] {ticker}: Earnings in {cs.days_to_earnings}d, no flow context. "
                    f"Score=60 — ⚠️ cannot confirm if flow is directional vs hedging."
                )
        elif cs.days_to_earnings <= 14:
            cs.has_upcoming_catalyst = True
            cs.catalyst_type = cs.catalyst_type or "EARNINGS"
            cs.score = 70 if flow_is_directional else 50
            log.info(
                f"[CAT] {ticker}: Earnings in {cs.days_to_earnings}d. "
                f"Score={cs.score} — "
                f"{'directional flow confirms pre-earnings positioning.' if flow_is_directional else 'monitoring — need directional flow to confirm.'}"
            )
        elif cs.days_to_earnings <= 30:
            cs.has_upcoming_catalyst = True
            cs.catalyst_type = cs.catalyst_type or "EARNINGS"
            cs.score = 50
            log.info(
                f"[CAT] {ticker}: Earnings in {cs.days_to_earnings}d. "
                f"Score=50 — on the radar but not imminent."
            )
        else:
            cs.score = 20
            log.info(
                f"[CAT] {ticker}: Earnings in {cs.days_to_earnings}d. "
                f"Score=20 — too far out to be a near-term catalyst."
            )
    elif cs.has_upcoming_catalyst:
        cs.score = 80
        log.info(f"[CAT] {ticker}: Non-earnings catalyst ({cs.catalyst_type}) — Score=80")
    else:
        cs.score = 10
        log.info(f"[CAT] {ticker}: No upcoming catalysts identified — Score=10")

    log.info(
        f"[CAT] {ticker}: ✅ FINAL — Earnings={cs.next_earnings_date} "
        f"({cs.days_to_earnings}d away) | "
        f"Catalyst={'YES — ' + cs.catalyst_type if cs.has_upcoming_catalyst else 'NONE'} | "
        f"Score={cs.score:.1f}/100 | Signal={cs.signal.value}"
    )
    return cs


# ---------------------------------------------------------------------------
# Strategy/Layer 7: Social Sentiment — Reddit WSB, r/stocks, r/options
# Social buzz is a CONFIRMATION layer, not a primary signal. High buzz + no
# institutional flow = retail pump (be cautious). High buzz + strong flow +
# dark pool = institutional + retail aligned = high conviction.
# Data source: ApeWisdom (free, no API key, tracks Reddit mentions)
# What it checks:
# # WSB trending rank — top 5 = massive retail eyeballs (+25 pts)
# # Mention volume — 50+ mentions in 24h = high buzz (+30 pts)
# # Mention momentum — % change vs prior 24h. 200%+ = exploding (+30 pts)
# # Upvote quality — 1000+ upvotes = quality discussion, not spam (+15 pts)
# # Also checks r/stocks and r/options for broader coverage
# Trending flag: mention spike >= 100% OR WSB rank <= 15
# Scoring:
# # Sum of components above, capped at 100
# # Score >= 50 = BULLISH (strong social buzz)
# # Score 25-49 = NEUTRAL (some buzz, not enough to confirm)
# # Score < 25 = NEUTRAL (minimal social presence)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Layer 7: Social Sentiment (Reddit WSB + r/stocks, Twitter/X)
# ---------------------------------------------------------------------------

# ApeWisdom — free, no API key, tracks Reddit mentions
APEWISDOM_BASE = "https://apewisdom.io/api/v1.0"


def analyze_social(ticker: str) -> SocialScore:
    """
    Check social buzz on Reddit (WSB, r/stocks, r/options) via ApeWisdom.
    
    What matters:
        - Mention spike (rising mentions vs 24h ago = building momentum)
        - WSB rank (top 10 = heavy retail attention)
        - Upvotes (high upvotes = post quality, not just spam)
    
    IMPORTANT: Social buzz is a CONFIRMATION layer, not a primary signal.
    High buzz + no institutional flow = retail pump, be cautious.
    High buzz + strong flow + dark pool = institutional + retail aligned = high conviction.
    """
    ss = SocialScore()
    ticker = ticker.upper()
    log.info(f"[SOCIAL] {ticker}: Fetching social sentiment from Reddit (WSB, r/stocks, r/options)...")

    try:
        # Pull WSB trending list
        log.info(f"[SOCIAL] {ticker}: Querying ApeWisdom — r/wallstreetbets trending...")
        resp = httpx.get(f"{APEWISDOM_BASE}/filter/wallstreetbets", timeout=10)
        resp.raise_for_status()
        wsb_data = resp.json()

        results = wsb_data.get("results", [])
        total_tickers_on_wsb = len(results)
        log.info(f"[SOCIAL] {ticker}: WSB has {total_tickers_on_wsb} tickers being discussed")

        found_on_wsb = False
        for item in results:
            if _str(item.get("ticker")).upper() == ticker:
                found_on_wsb = True
                ss.wsb_rank = item.get("rank")
                ss.mentions_24h = _int(item.get("mentions"))
                ss.upvotes = _int(item.get("upvotes"))

                # Calculate mention change
                mentions_prev = _int(item.get("mentions_24h_ago"))
                if mentions_prev > 0:
                    ss.mentions_change_pct = (
                        (ss.mentions_24h - mentions_prev) / mentions_prev * 100
                    )
                elif ss.mentions_24h > 0:
                    ss.mentions_change_pct = 500.0

                log.info(
                    f"[SOCIAL] {ticker}: 📊 Found on WSB — "
                    f"Rank #{ss.wsb_rank} | "
                    f"Mentions={ss.mentions_24h} (was {mentions_prev} yesterday, {ss.mentions_change_pct:+.0f}% change) | "
                    f"Upvotes={ss.upvotes}"
                )
                break

        if not found_on_wsb:
            log.info(f"[SOCIAL] {ticker}: Not found in WSB trending list (not being discussed)")

        # Also check r/stocks and r/options for broader coverage
        for sub in ("stocks", "options"):
            try:
                log.info(f"[SOCIAL] {ticker}: Querying ApeWisdom — r/{sub}...")
                resp2 = httpx.get(f"{APEWISDOM_BASE}/filter/{sub}", timeout=10)
                resp2.raise_for_status()
                sub_data = resp2.json()
                found_in_sub = False
                for item in sub_data.get("results", []):
                    if _str(item.get("ticker")).upper() == ticker:
                        sub_mentions = _int(item.get("mentions"))
                        sub_upvotes = _int(item.get("upvotes"))
                        sub_rank = item.get("rank", "?")
                        ss.mentions_24h += sub_mentions
                        ss.upvotes += sub_upvotes
                        found_in_sub = True
                        log.info(
                            f"[SOCIAL] {ticker}: Found on r/{sub} — "
                            f"Rank #{sub_rank} | +{sub_mentions} mentions | +{sub_upvotes} upvotes"
                        )
                        break
                if not found_in_sub:
                    log.info(f"[SOCIAL] {ticker}: Not found on r/{sub}")
            except Exception as e:
                log.warning(f"[SOCIAL] {ticker}: ⚠️ Failed to fetch r/{sub}: {e}")

    except Exception as e:
        log.warning(f"[SOCIAL] {ticker}: ⚠️ ApeWisdom request failed: {e}")

    # Determine if trending (spike in mentions)
    if ss.mentions_change_pct >= 100 or (ss.wsb_rank and ss.wsb_rank <= 15):
        ss.is_trending = True
        if ss.mentions_change_pct >= 100:
            log.info(f"[SOCIAL] {ticker}: 📈 TRENDING — mention spike of {ss.mentions_change_pct:+.0f}% (≥100%)")
        if ss.wsb_rank and ss.wsb_rank <= 15:
            log.info(f"[SOCIAL] {ticker}: 📈 TRENDING — WSB rank #{ss.wsb_rank} (top 15)")

    # Scoring
    score = 0
    log.info(f"[SOCIAL] {ticker}: Scoring social sentiment...")

    # Mention volume
    if ss.mentions_24h >= 50:
        score += 30
        log.info(f"[SOCIAL] {ticker}: Scoring — High mention volume ({ss.mentions_24h} ≥ 50): +30 → {score}")
    elif ss.mentions_24h >= 20:
        score += 20
        log.info(f"[SOCIAL] {ticker}: Scoring — Moderate mention volume ({ss.mentions_24h} ≥ 20): +20 → {score}")
    elif ss.mentions_24h >= 5:
        score += 10
        log.info(f"[SOCIAL] {ticker}: Scoring — Some mentions ({ss.mentions_24h} ≥ 5): +10 → {score}")
    else:
        log.info(f"[SOCIAL] {ticker}: Scoring — Low/no mentions ({ss.mentions_24h}): +0 → {score}")

    # Mention momentum
    if ss.mentions_change_pct >= 200:
        score += 30
        log.info(f"[SOCIAL] {ticker}: Scoring — EXPLODING momentum ({ss.mentions_change_pct:+.0f}% ≥ 200%): +30 → {score}")
    elif ss.mentions_change_pct >= 100:
        score += 20
        log.info(f"[SOCIAL] {ticker}: Scoring — Strong spike ({ss.mentions_change_pct:+.0f}% ≥ 100%): +20 → {score}")
    elif ss.mentions_change_pct >= 50:
        score += 10
        log.info(f"[SOCIAL] {ticker}: Scoring — Moderate increase ({ss.mentions_change_pct:+.0f}% ≥ 50%): +10 → {score}")
    else:
        log.info(f"[SOCIAL] {ticker}: Scoring — Flat/declining momentum ({ss.mentions_change_pct:+.0f}%): +0 → {score}")

    # WSB rank
    if ss.wsb_rank and ss.wsb_rank <= 5:
        score += 25
        log.info(f"[SOCIAL] {ticker}: Scoring — WSB TOP 5 (rank #{ss.wsb_rank}): +25 → {score}")
    elif ss.wsb_rank and ss.wsb_rank <= 15:
        score += 15
        log.info(f"[SOCIAL] {ticker}: Scoring — WSB top 15 (rank #{ss.wsb_rank}): +15 → {score}")
    elif ss.wsb_rank and ss.wsb_rank <= 30:
        score += 5
        log.info(f"[SOCIAL] {ticker}: Scoring — WSB top 30 (rank #{ss.wsb_rank}): +5 → {score}")
    else:
        log.info(f"[SOCIAL] {ticker}: Scoring — Not ranked on WSB: +0 → {score}")

    # Upvote quality
    if ss.upvotes >= 1000:
        score += 15
        log.info(f"[SOCIAL] {ticker}: Scoring — High quality discussion ({ss.upvotes} upvotes ≥ 1000): +15 → {score}")
    elif ss.upvotes >= 200:
        score += 10
        log.info(f"[SOCIAL] {ticker}: Scoring — Decent engagement ({ss.upvotes} upvotes ≥ 200): +10 → {score}")
    else:
        log.info(f"[SOCIAL] {ticker}: Scoring — Low engagement ({ss.upvotes} upvotes): +0 → {score}")

    ss.score = min(100, score)

    # Signal
    if ss.score >= 50:
        ss.signal = Signal.BULLISH
        log.info(f"[SOCIAL] {ticker}: → BULLISH (score {ss.score:.0f} ≥ 50 — strong social buzz)")
    elif ss.score >= 25:
        ss.signal = Signal.NEUTRAL
        log.info(f"[SOCIAL] {ticker}: → NEUTRAL (score {ss.score:.0f} — some buzz, not enough to confirm)")
    else:
        ss.signal = Signal.NEUTRAL
        log.info(f"[SOCIAL] {ticker}: → NEUTRAL (score {ss.score:.0f} — minimal social presence)")

    log.info(
        f"[SOCIAL] {ticker}: ✅ FINAL — Mentions={ss.mentions_24h} "
        f"({ss.mentions_change_pct:+.0f}%) | "
        f"WSB_Rank={ss.wsb_rank or 'N/A'} | "
        f"Upvotes={ss.upvotes} | "
        f"Trending={'YES 📈' if ss.is_trending else 'NO'} | "
        f"Score={ss.score:.1f}/100 | Signal={ss.signal.value}"
    )
    return ss

# ---------------------------------------------------------------------------
# Composite Scoring
# ---------------------------------------------------------------------------

# Weights for each layer (must sum to 1.0)
WEIGHTS = {
    "flow": 0.28,       # flow is primary directional signal
    "darkpool": 0.10,   # institutional activity level (non-directional)
    "gex": 0.10,        # gamma levels — now dynamic scoring
    "iv": 0.12,         # vol regime matters for entry
    "technicals": 0.17, # chart confirmation — now includes VWAP + RVOL
    "catalyst": 0.12,   # catalyst proximity boosts conviction
    "social": 0.11,     # social buzz as momentum confirmation
}

def compute_composite(result: StrategyResult) -> StrategyResult:
    """Compute weighted composite score and conviction level."""
    ticker = result.ticker

    log.info(f"\n[COMPOSITE] {ticker}: {'─'*50}")
    log.info(f"[COMPOSITE] {ticker}: Computing weighted composite score...")
    log.info(f"[COMPOSITE] {ticker}: Layer weights: {json.dumps(WEIGHTS, indent=2)}")

    # Calculate weighted contributions
    flow_contrib = result.flow.score * WEIGHTS["flow"]
    dark_contrib = result.darkpool.score * WEIGHTS["darkpool"]
    gex_contrib = result.gex.score * WEIGHTS["gex"]
    iv_contrib = result.iv.score * WEIGHTS["iv"]
    tech_contrib = result.technicals.score * WEIGHTS["technicals"]
    cat_contrib = result.catalyst.score * WEIGHTS["catalyst"]
    social_contrib = result.social.score * WEIGHTS["social"]

    result.composite_score = (
        flow_contrib + dark_contrib + gex_contrib +
        iv_contrib + tech_contrib + cat_contrib + social_contrib
    )

    log.info(f"[COMPOSITE] {ticker}: Weighted contributions:")
    log.info(f"[COMPOSITE] {ticker}:   1. Flow       {result.flow.score:6.1f} × {WEIGHTS['flow']:.2f} = {flow_contrib:6.2f}  [{result.flow.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   2. Darkpool    {result.darkpool.score:6.1f} × {WEIGHTS['darkpool']:.2f} = {dark_contrib:6.2f}  [{result.darkpool.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   3. GEX         {result.gex.score:6.1f} × {WEIGHTS['gex']:.2f} = {gex_contrib:6.2f}  [{result.gex.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   4. IV          {result.iv.score:6.1f} × {WEIGHTS['iv']:.2f} = {iv_contrib:6.2f}  [{result.iv.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   5. Technicals  {result.technicals.score:6.1f} × {WEIGHTS['technicals']:.2f} = {tech_contrib:6.2f}  [{result.technicals.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   6. Catalyst    {result.catalyst.score:6.1f} × {WEIGHTS['catalyst']:.2f} = {cat_contrib:6.2f}  [{result.catalyst.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   7. Social      {result.social.score:6.1f} × {WEIGHTS['social']:.2f} = {social_contrib:6.2f}  [{result.social.signal.value}]")
    log.info(f"[COMPOSITE] {ticker}:   {'─'*45}")
    log.info(f"[COMPOSITE] {ticker}:   COMPOSITE SCORE = {result.composite_score:.1f}/100")

    # Count aligned layers — only DIRECTIONAL layers count for alignment
    # Directional (4): Flow, GEX, Technicals, Social — can signal BULLISH/BEARISH
    # Condition (3): Darkpool (activity), IV (option pricing), Catalyst (event magnitude)
    # Condition layers contribute to composite score but not directional alignment.
    # IV "BULLISH" means cheap options (good for buying calls OR puts), not a directional call.
    directional_layers = [
        ("Flow", result.flow.signal),
        ("GEX", result.gex.signal),
        ("Technicals", result.technicals.signal),
        ("Social", result.social.signal),
    ]
    condition_layers = [
        ("Darkpool", result.darkpool.signal),
        ("IV", result.iv.signal),
        ("Catalyst", result.catalyst.signal),
    ]
    all_layers = directional_layers + condition_layers

    bullish_count = sum(1 for _, s in directional_layers if s == Signal.BULLISH)
    bearish_count = sum(1 for _, s in directional_layers if s == Signal.BEARISH)

    result.layers_aligned = max(bullish_count, bearish_count)

    log.info(f"[COMPOSITE] {ticker}: Directional alignment (of 4): 🟢 Bullish={bullish_count} | 🔴 Bearish={bearish_count}")
    bullish_layers = [name for name, s in all_layers if s == Signal.BULLISH]
    bearish_layers = [name for name, s in all_layers if s == Signal.BEARISH]
    neutral_layers = [name for name, s in all_layers if s == Signal.NEUTRAL]
    if bullish_layers:
        log.info(f"[COMPOSITE] {ticker}:   🟢 Bullish: {', '.join(bullish_layers)}")
    if bearish_layers:
        log.info(f"[COMPOSITE] {ticker}:   🔴 Bearish: {', '.join(bearish_layers)}")
    if neutral_layers:
        log.info(f"[COMPOSITE] {ticker}:   ⚪ Neutral/Condition: {', '.join(neutral_layers)}")

    if bullish_count > bearish_count:
        result.direction = Signal.BULLISH
        log.info(f"[COMPOSITE] {ticker}: Overall direction → 🟢 BULLISH ({bullish_count} vs {bearish_count})")
    elif bearish_count > bullish_count:
        result.direction = Signal.BEARISH
        log.info(f"[COMPOSITE] {ticker}: Overall direction → 🔴 BEARISH ({bearish_count} vs {bullish_count})")
    else:
        result.direction = Signal.NEUTRAL
        log.info(f"[COMPOSITE] {ticker}: Overall direction → ⚪ NEUTRAL (tied {bullish_count} vs {bearish_count})")

    # Conviction — thresholds based on 4 directional layers
    if result.composite_score >= 75 and result.layers_aligned >= 4:
        result.conviction = "VERY_HIGH"
        log.info(f"[COMPOSITE] {ticker}: 🔥🔥🔥 VERY HIGH CONVICTION — score {result.composite_score:.1f} ≥ 75 AND {result.layers_aligned}/4 directional layers aligned")
    elif result.composite_score >= 60 and result.layers_aligned >= 3:
        result.conviction = "HIGH"
        log.info(f"[COMPOSITE] {ticker}: 🔥🔥 HIGH CONVICTION — score {result.composite_score:.1f} ≥ 60 AND {result.layers_aligned}/4 directional layers aligned ≥ 3")
    elif result.composite_score >= 45 and result.layers_aligned >= 2:
        result.conviction = "MEDIUM"
        log.info(f"[COMPOSITE] {ticker}: 🔥 MEDIUM CONVICTION — score {result.composite_score:.1f} ≥ 45 AND {result.layers_aligned}/4 directional layers aligned ≥ 2")
    elif result.composite_score >= 30:
        result.conviction = "LOW"
        log.info(f"[COMPOSITE] {ticker}: 💤 LOW CONVICTION — score {result.composite_score:.1f} ≥ 30 but insufficient directional alignment")
    else:
        result.conviction = "NONE"
        log.info(f"[COMPOSITE] {ticker}: ❌ NO CONVICTION — score {result.composite_score:.1f} < 30, skip this trade")

    log.info(f"[COMPOSITE] {ticker}: {'─'*50}\n")

    return result

# ---------------------------------------------------------------------------
# Main analysis pipeline
# ---------------------------------------------------------------------------

def analyze_ticker(ticker: str) -> StrategyResult:
    """Run all 7 layers of analysis on a single ticker."""
    ticker = ticker.upper()
    log.info(f"\n{'='*60}")
    log.info(f"  🔍 ANALYZING: {ticker}")
    log.info(f"  📅 Timestamp: {datetime.now().isoformat()}")
    log.info(f"{'='*60}")

    result = StrategyResult(
        ticker=ticker,
        timestamp=datetime.now().isoformat(),
    )

    # Layer 1: Flow
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 1/7: OPTIONS FLOW ──")
    result.flow = analyze_flow(ticker)

    # Layer 2: Dark Pool
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 2/7: DARK POOL ──")
    result.darkpool = analyze_darkpool(ticker)

    # Layer 3: GEX (needs current price from technicals)
    # We'll get price from SMA data later and re-score if needed

    # Layer 4: IV
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 4/7: IMPLIED VOLATILITY ──")
    result.iv = analyze_iv(ticker)

    # Fetch current stock price (needed by technicals and GEX)
    current_price = None
    price_data = uw_get(f"/api/stock/{ticker}/quote")
    quote = price_data.get("data", {})
    if isinstance(quote, list):
        quote = quote[-1] if quote else {}
    for key in ("price", "last", "close"):
        if quote.get(key):
            current_price = _float(quote[key])
            if current_price > 0:
                log.info(f"[PIPELINE] {ticker}: Current price = ${current_price:.2f}")
                break
            current_price = None
    if not current_price:
        log.warning(f"[PIPELINE] {ticker}: ⚠️ Could not fetch price — VWAP scoring and GEX will be limited")

    # Layer 5: Technicals (needs current_price for VWAP comparison)
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 5/7: TECHNICALS ──")
    result.technicals = analyze_technicals(ticker, current_price=current_price)

    # Layer 3 revisited with price context
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 3/7: GAMMA EXPOSURE (GEX) ──")
    result.gex = analyze_gex(ticker, current_price)

    # Layer 6: Catalyst
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 6/7: CATALYST DETECTION ──")
    result.catalyst = analyze_catalyst(ticker, flow=result.flow)

    # Layer 7: Social Sentiment (Reddit WSB, r/stocks, Twitter/X)
    log.info(f"\n[PIPELINE] {ticker}: ── Layer 7/7: SOCIAL SENTIMENT ──")
    result.social = analyze_social(ticker)

    # Composite
    log.info(f"\n[PIPELINE] {ticker}: ── COMPUTING COMPOSITE SCORE ──")
    result = compute_composite(result)

    log.info(f"[PIPELINE] {ticker}: ✅ Analysis complete — {result.conviction} conviction {result.direction.value}")

    return result

def scan_flow_for_candidates(min_premium: int = 100_000, limit: int = 50) -> list[str]:
    """
    Scan the market-wide flow alerts to find tickers with unusual activity.
    Returns a deduplicated list of tickers worth analyzing.
    """
    log.info("Scanning market-wide flow for candidates...")

    data = uw_get("/api/option-trades/flow-alerts", {
        "min_premium": min_premium,
        "size_greater_oi": True,
        "is_otm": True,
        "limit": limit,
    })
    alerts = data.get("data", [])

    # Count alerts per ticker, weighted by premium
    ticker_scores = {}
    for alert in alerts:
        try:
            ticker = alert.get("ticker_symbol", alert.get("ticker", ""))
            premium = _float(alert.get("total_premium"))
            if ticker:
                if ticker not in ticker_scores:
                    ticker_scores[ticker] = {"count": 0, "premium": 0}
                ticker_scores[ticker]["count"] += 1
                ticker_scores[ticker]["premium"] += premium
        except Exception as e:
            log.warning(f"[SCAN] Skipping bad alert in scan: {e}")
            continue

    # Sort by count * premium (conviction)
    ranked = sorted(
        ticker_scores.items(),
        key=lambda x: x[1]["count"] * x[1]["premium"],
        reverse=True,
    )

    # Filter out ETFs/indices
    candidates = [t for t, _ in ranked if t not in ETF_BLACKLIST]

    log.info(f"Found {len(candidates)} candidate tickers: {candidates[:15]}")
    return candidates[:15]  # top 15

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_report(result: StrategyResult):
    """Print a formatted analysis report."""
    direction_emoji = {
        Signal.BULLISH: "🟢",
        Signal.BEARISH: "🔴",
        Signal.NEUTRAL: "⚪",
    }

    conviction_emoji = {
        "VERY_HIGH": "🔥🔥🔥",
        "HIGH": "🔥🔥",
        "MEDIUM": "🔥",
        "LOW": "💤",
        "NONE": "❌",
    }

    print(f"\n{'='*60}")
    print(f"  {result.ticker} STRATEGY REPORT")
    print(f"  {result.timestamp}")
    print(f"{'='*60}")
    print(f"\n  COMPOSITE SCORE:  {result.composite_score:.1f}/100")
    print(f"  DIRECTION:        {direction_emoji.get(result.direction, '')} {result.direction.value}")
    print(f"  CONVICTION:       {conviction_emoji.get(result.conviction, '')} {result.conviction}")
    print(f"  LAYERS ALIGNED:   {result.layers_aligned}/4")

    print(f"\n  {'─'*56}")
    print(f"  LAYER BREAKDOWN:")
    print(f"  {'─'*56}")
    print(f"  1. Flow      {result.flow.score:5.1f}  {direction_emoji.get(result.flow.signal, '')} "
          f"Bull={result.flow.bullish_sweeps} Bear={result.flow.bearish_sweeps} "
          f"RawPrem=${result.flow.total_premium:,.0f} WtPrem=${result.flow.total_premium_weighted:,.0f}")
    print(f"  2. Darkpool   {result.darkpool.score:5.1f}  {direction_emoji.get(result.darkpool.signal, '')} "
          f"Large prints={result.darkpool.large_prints} "
          f"Activity={result.darkpool.activity_level} "
          f"Notional=${result.darkpool.total_notional:,.0f}")
    print(f"  3. GEX        {result.gex.score:5.1f}  {direction_emoji.get(result.gex.signal, '')} "
          f"MaxGamma=${result.gex.max_gamma_strike} "
          f"PutWall=${result.gex.put_wall_strike} "
          f"CallWall=${result.gex.call_wall_strike}")
    print(f"  4. IV         {result.iv.score:5.1f}  {direction_emoji.get(result.iv.signal, '')} "
          f"Pctl={result.iv.iv_percentile} "
          f"Current={result.iv.iv_current}")
    print(f"  5. Technicals {result.technicals.score:5.1f}  {direction_emoji.get(result.technicals.signal, '')} "
          f"RSI={result.technicals.rsi_14} "
          f"MACD_h={result.technicals.macd_histogram} "
          f"RVOL={result.technicals.relative_volume or 'N/A'} "
          f"Trend={'UP' if result.technicals.trend_aligned else 'DOWN'}")
    print(f"  6. Catalyst   {result.catalyst.score:5.1f}  {direction_emoji.get(result.catalyst.signal, '')} "
          f"Earnings={result.catalyst.next_earnings_date} "
          f"({result.catalyst.days_to_earnings}d)")
    print(f"  7. Social     {result.social.score:5.1f}  {direction_emoji.get(result.social.signal, '')} "
          f"Mentions={result.social.mentions_24h} "
          f"({result.social.mentions_change_pct:+.0f}%) "
          f"WSB_Rank={result.social.wsb_rank or 'N/A'} "
          f"{'📈 TRENDING' if result.social.is_trending else ''}")
    print(f"  {'─'*56}")

    # Trade suggestion
    if result.conviction in ("HIGH", "VERY_HIGH"):
        if result.direction == Signal.BULLISH:
            if result.iv.iv_percentile and result.iv.iv_percentile > 60:
                print(f"\n  💡 SUGGESTION: Bull call SPREAD (IV is elevated, use spreads to reduce IV crush)")
            else:
                print(f"\n  💡 SUGGESTION: Buy OTM CALLS (IV is cheap, naked calls viable)")
            if result.catalyst.days_to_earnings and result.catalyst.days_to_earnings <= 7:
                print(f"  ⚠️  Earnings in {result.catalyst.days_to_earnings}d — use defined-risk only!")
        elif result.direction == Signal.BEARISH:
            if result.iv.iv_percentile and result.iv.iv_percentile > 60:
                print(f"\n  💡 SUGGESTION: Bear put SPREAD (IV elevated)")
            else:
                print(f"\n  💡 SUGGESTION: Buy OTM PUTS")
            if result.catalyst.days_to_earnings and result.catalyst.days_to_earnings <= 7:
                print(f"  ⚠️  Earnings in {result.catalyst.days_to_earnings}d — use defined-risk only!")
    elif result.conviction == "MEDIUM":
        print(f"\n  💡 SUGGESTION: Watch for confirmation. Paper trade or small size only.")
    else:
        print(f"\n  💡 SUGGESTION: No trade. Insufficient conviction.")

    # Top flow details
    if result.flow.details:
        print(f"\n  {'─'*56}")
        print(f"  TOP FLOW PRINTS (>$100K):")
        print(f"  {'─'*56}")
        for d in sorted(result.flow.details, key=lambda x: x["premium"], reverse=True)[:5]:
            dte_str = f"{d['dte']}d" if d.get('dte') is not None else "?"
            bucket = d.get('dte_bucket', '?')
            print(f"    {d['sentiment']:4s} | {d['type']:4s} | {d['trade_type']:6s} | "
                  f"${d['premium']:>10,.0f} | Strike={d['strike']} | Exp={d['expiry']} | DTE={dte_str} [{bucket}]")

    print(f"\n{'='*60}\n")

# ---------------------------------------------------------------------------
# Telegram Alerts
# ---------------------------------------------------------------------------

def format_telegram_message(result: StrategyResult) -> str:
    direction_emoji = {Signal.BULLISH: "🟢", Signal.BEARISH: "🔴", Signal.NEUTRAL: "⚪"}
    conviction_emoji = {"VERY_HIGH": "🔥🔥🔥", "HIGH": "🔥🔥", "MEDIUM": "🔥", "LOW": "💤", "NONE": "❌"}

    de = direction_emoji.get
    lines = [
        f"{'='*30}",
        f"📊 <b>{result.ticker}</b> — STRATEGY ALERT",
        f"{'='*30}",
        f"",
        f"<b>Score:</b>  {result.composite_score:.1f}/100",
        f"<b>Direction:</b>  {de(result.direction, '')} {result.direction.value}",
        f"<b>Conviction:</b>  {conviction_emoji.get(result.conviction, '')} {result.conviction}",
        f"<b>Layers Aligned:</b>  {result.layers_aligned}/4",
        f"",
        f"{'─'*30}",
        f"<b>LAYER BREAKDOWN</b>",
        f"{'─'*30}",
        f"1. Flow       {result.flow.score:5.1f} {de(result.flow.signal, '')}  Bull={result.flow.bullish_sweeps} Bear={result.flow.bearish_sweeps}  RawPrem=${result.flow.total_premium:,.0f} WtPrem=${result.flow.total_premium_weighted:,.0f}",
        f"2. Darkpool   {result.darkpool.score:5.1f} {de(result.darkpool.signal, '')}  Lg prints={result.darkpool.large_prints}  Activity={result.darkpool.activity_level}",
        f"3. GEX        {result.gex.score:5.1f} {de(result.gex.signal, '')}  MaxGamma=${result.gex.max_gamma_strike}  Flip={result.gex.gamma_flip}",
        f"4. IV         {result.iv.score:5.1f} {de(result.iv.signal, '')}  Pctl={result.iv.iv_percentile}  Current={result.iv.iv_current}",
        f"5. Technicals {result.technicals.score:5.1f} {de(result.technicals.signal, '')}  RSI={result.technicals.rsi_14}  MACD_h={result.technicals.macd_histogram}  RVOL={result.technicals.relative_volume or 'N/A'}",
        f"6. Catalyst   {result.catalyst.score:5.1f} {de(result.catalyst.signal, '')}  Earnings={result.catalyst.next_earnings_date} ({result.catalyst.days_to_earnings}d)",
        f"7. Social     {result.social.score:5.1f} {de(result.social.signal, '')}  Mentions={result.social.mentions_24h} ({result.social.mentions_change_pct:+.0f}%)  WSB#{result.social.wsb_rank or 'N/A'}",
    ]

    # Trade suggestion
    if result.conviction in ("HIGH", "VERY_HIGH"):
        if result.direction == Signal.BULLISH:
            if result.iv.iv_percentile and result.iv.iv_percentile > 60:
                lines.append(f"\n💡 Bull call SPREAD (IV elevated, use spreads)")
            else:
                lines.append(f"\n💡 Buy OTM CALLS (IV cheap, naked calls viable)")
            if result.catalyst.days_to_earnings and result.catalyst.days_to_earnings <= 7:
                lines.append(f"⚠️ Earnings in {result.catalyst.days_to_earnings}d — defined-risk only!")
        elif result.direction == Signal.BEARISH:
            if result.iv.iv_percentile and result.iv.iv_percentile > 60:
                lines.append(f"\n💡 Bear put SPREAD (IV elevated)")
            else:
                lines.append(f"\n💡 Buy OTM PUTS")
            if result.catalyst.days_to_earnings and result.catalyst.days_to_earnings <= 7:
                lines.append(f"⚠️ Earnings in {result.catalyst.days_to_earnings}d — defined-risk only!")
    elif result.conviction == "MEDIUM":
        lines.append(f"\n💡 Watch for confirmation — paper trade or small size only")
    else:
        lines.append(f"\n💡 No trade — insufficient conviction")

    # Top flow prints
    if result.flow.details:
        top_flows = sorted(result.flow.details, key=lambda x: x["premium"], reverse=True)[:3]
        lines.append(f"\n{'─'*30}")
        lines.append(f"<b>TOP FLOW</b>")
        for d in top_flows:
            dte_str = f"{d['dte']}d" if d.get('dte') is not None else "?"
            lines.append(f"  {d['sentiment']} | {d['type']} | ${d['premium']:,.0f} | {d['strike']} | {d['expiry']} | DTE={dte_str} [{d.get('dte_bucket', '?')}]")

    lines.append(f"\n⏰ {result.timestamp}")
    return "\n".join(lines)


def send_telegram_alert(result: StrategyResult) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set — skipping alert")
        return False

    message = format_telegram_message(result)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
            if resp.status_code == 200:
                log.info(f"[TELEGRAM] ✅ Alert sent for {result.ticker}")
                return True
            else:
                log.error(f"[TELEGRAM] ❌ Failed ({resp.status_code}): {resp.text}")
                return False
    except Exception as e:
        log.error(f"[TELEGRAM] ❌ Error sending alert: {e}")
        return False


# ---------------------------------------------------------------------------
# Live WebSocket Monitor
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")

class LiveMonitor:
    """Persistent websocket monitor — triggers 7-layer analysis on qualifying flow."""

    def __init__(self, min_premium=100_000, min_conviction="MEDIUM",
                 cooldown_minutes=5, send_telegram=True):
        self.min_premium = min_premium
        self.min_conviction = min_conviction
        self.cooldown_minutes = cooldown_minutes
        self.send_telegram = send_telegram
        self._last_analysis: dict[str, datetime] = {}
        self._running = True
        self._conviction_order = ["NONE", "LOW", "MEDIUM", "HIGH", "VERY_HIGH"]

    def _is_market_hours(self) -> bool:
        now_et = datetime.now(ET)
        if now_et.weekday() >= 5:
            return False
        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_open <= now_et <= market_close

    def _should_analyze(self, ticker: str, premium: float) -> bool:
        if ticker in ETF_BLACKLIST:
            return False
        if premium < self.min_premium:
            return False
        last = self._last_analysis.get(ticker)
        if last and (datetime.now() - last).total_seconds() < self.cooldown_minutes * 60:
            log.debug(f"[LIVE] {ticker}: Cooldown active, skipping")
            return False
        return True

    def _run_analysis(self, ticker: str):
        log.info(f"[LIVE] {'='*50}")
        log.info(f"[LIVE] 🔍 Running full 7-layer analysis for {ticker}")
        log.info(f"[LIVE] {'='*50}")

        try:
            result = analyze_ticker(ticker)
            print_report(result)

            r_level = self._conviction_order.index(result.conviction)
            min_level = self._conviction_order.index(self.min_conviction)

            if self.send_telegram and r_level >= min_level:
                send_telegram_alert(result)
                log.info(f"[LIVE] 📱 Telegram alert sent for {ticker} ({result.conviction})")
            elif self.send_telegram:
                log.info(f"[LIVE] {ticker}: Conviction {result.conviction} below threshold {self.min_conviction} — no Telegram")
        except Exception as e:
            log.error(f"[LIVE] Analysis failed for {ticker}: {e}")
            del self._last_analysis[ticker]

    async def _handle_flow_alert(self, payload: dict):
        ticker = payload.get("ticker_symbol", payload.get("ticker", ""))
        premium = _float(payload.get("total_premium"))

        if not self._is_market_hours():
            if ticker and premium >= self.min_premium and ticker not in ETF_BLACKLIST:
                side = str(payload.get("side", "")).upper()
                option_type = str(payload.get("type", "")).upper()
                log.info(
                    f"[LIVE] 🌙 After-hours flow: {ticker} | {option_type} {side} | "
                    f"${premium:,.0f} — not triggering analysis (market closed)"
                )
            return

        if not ticker or not self._should_analyze(ticker, premium):
            return

        is_otm = payload.get("is_otm", False)
        trade_type = str(payload.get("trade_type", "")).upper()
        vol_gt_oi = payload.get("size_greater_oi", False)

        if not (is_otm and trade_type in ("SWEEP", "BLOCK") and vol_gt_oi):
            return

        option_type = str(payload.get("type", "")).upper()
        side = str(payload.get("side", "")).upper()
        expiry = payload.get("expires", "?")
        log.info(
            f"[LIVE] 🚨 Qualifying flow: {ticker} | {option_type} {side} | "
            f"${premium:,.0f} | Exp={expiry} | Triggering analysis..."
        )

        self._last_analysis[ticker] = datetime.now()
        asyncio.create_task(asyncio.to_thread(self._run_analysis, ticker))

    def _handle_darkpool_print(self, payload: dict):
        volume = _float(payload.get("volume", payload.get("size")))
        price = _float(payload.get("price"))
        notional = volume * price
        ticker = payload.get("ticker_symbol", payload.get("ticker", ""))
        if ticker and notional >= 1_000_000 and ticker not in ETF_BLACKLIST:
            log.info(f"[LIVE] 🏦 Dark pool: {ticker} | ${notional:,.0f} | {volume:,.0f} shares @ ${price:.2f}")

    async def run(self):
        try:
            import websockets
        except ImportError:
            print("ERROR: websockets library required for live mode")
            print("  pip install websockets")
            sys.exit(1)

        ws_url = f"wss://api.unusualwhales.com/socket?token={UW_TOKEN}"
        reconnect_delay = 1
        max_reconnect_delay = 60

        log.info(f"[LIVE] {'='*50}")
        log.info(f"[LIVE] 🟢 ChainSignal LIVE MONITOR starting")
        log.info(f"[LIVE] Filters: min_premium=${self.min_premium:,} | min_conviction={self.min_conviction} | cooldown={self.cooldown_minutes}min")
        log.info(f"[LIVE] Telegram: {'ON' if self.send_telegram else 'OFF'}")
        log.info(f"[LIVE] ETF filter: {len(ETF_BLACKLIST)} tickers excluded")
        log.info(f"[LIVE] {'='*50}")

        while self._running:
            try:
                async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10) as ws:
                    log.info("[LIVE] ✅ WebSocket connected")

                    await ws.send(json.dumps({"channel": "flow-alerts", "msg_type": "join"}))
                    await ws.send(json.dumps({"channel": "off_lit_trades", "msg_type": "join"}))
                    log.info("[LIVE] Subscribed to: flow-alerts, off_lit_trades")

                    if not self._is_market_hours():
                        now_et = datetime.now(ET)
                        log.info(f"[LIVE] ⏸️  Market closed ({now_et.strftime('%A %H:%M ET')}) — listening but not triggering analysis")

                    reconnect_delay = 1

                    async for raw_msg in ws:
                        if not self._running:
                            break

                        try:
                            msg = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue

                        if not isinstance(msg, list) or len(msg) < 2:
                            continue

                        channel, payload = msg[0], msg[1]

                        try:
                            if channel == "flow-alerts":
                                await self._handle_flow_alert(payload)
                            elif channel == "off_lit_trades":
                                self._handle_darkpool_print(payload)
                        except Exception as e:
                            log.warning(f"[LIVE] Bad payload on {channel}: {e}")
                            continue

            except KeyboardInterrupt:
                log.info("[LIVE] Shutting down...")
                self._running = False
                break
            except Exception as e:
                log.error(f"[LIVE] WebSocket error: {e}")
                if not self._running:
                    break
                log.info(f"[LIVE] Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

        log.info("[LIVE] 🔴 Monitor stopped")

    def stop(self):
        self._running = False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if not UW_TOKEN:
        print("ERROR: Set UW_API_KEY in your .env file")
        print("  UW_API_KEY='your_unusual_whales_api_key'")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="ChainSignal Options Strategy Engine")
    parser.add_argument("--ticker", "-t", type=str, help="Analyze a single ticker")
    parser.add_argument("--watchlist", "-w", type=str, help="Comma-separated list of tickers")
    parser.add_argument("--scan", "-s", action="store_true", help="Scan flow for top candidates")
    parser.add_argument("--live", action="store_true",
                        help="Live mode — connect to UW websocket, trigger analysis on qualifying flow")
    parser.add_argument("--min-premium", type=int, default=100_000, help="Min premium for flow scan")
    parser.add_argument("--cooldown", type=int, default=5,
                        help="Minutes between re-analyzing the same ticker in live mode (default: 5)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--telegram", action="store_true", help="Send alerts to Telegram")
    parser.add_argument("--telegram-min-conviction", type=str, default="MEDIUM",
                        choices=["NONE", "LOW", "MEDIUM", "HIGH", "VERY_HIGH"],
                        help="Minimum conviction to trigger Telegram alert (default: MEDIUM)")
    args = parser.parse_args()

    if args.telegram and (not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID):
        print("ERROR: Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
        sys.exit(1)

    # Live mode — persistent websocket monitor
    if args.live:
        live_telegram = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
        if not live_telegram:
            log.warning("[LIVE] Telegram credentials not set — running console-only mode")
        monitor = LiveMonitor(
            min_premium=args.min_premium,
            min_conviction=args.telegram_min_conviction,
            cooldown_minutes=args.cooldown,
            send_telegram=live_telegram,
        )
        try:
            asyncio.run(monitor.run())
        except KeyboardInterrupt:
            monitor.stop()
            log.info("[LIVE] Shutdown complete")
        return

    results = []

    if args.scan:
        candidates = scan_flow_for_candidates(min_premium=args.min_premium)
        for ticker in candidates[:5]:  # analyze top 5
            result = analyze_ticker(ticker)
            results.append(result)
    elif args.watchlist:
        tickers = [t.strip().upper() for t in args.watchlist.split(",")]
        for ticker in tickers:
            result = analyze_ticker(ticker)
            results.append(result)
    elif args.ticker:
        result = analyze_ticker(args.ticker)
        results.append(result)
    else:
        parser.print_help()
        sys.exit(0)

    # Sort by composite score
    results.sort(key=lambda r: r.composite_score, reverse=True)

    if args.json:
        output = []
        for r in results:
            d = asdict(r)
            # Convert enums to strings for JSON
            d["direction"] = r.direction.value
            d["flow"]["signal"] = r.flow.signal.value
            d["darkpool"]["signal"] = r.darkpool.signal.value
            d["gex"]["signal"] = r.gex.signal.value
            d["iv"]["signal"] = r.iv.signal.value
            d["technicals"]["signal"] = r.technicals.signal.value
            d["catalyst"]["signal"] = r.catalyst.signal.value
            d["social"]["signal"] = r.social.signal.value
            output.append(d)
        print(json.dumps(output, indent=2, default=str))
    else:
        for result in results:
            print_report(result)

        # Summary table if multiple
        if len(results) > 1:
            print(f"\n{'='*70}")
            print(f"  SUMMARY RANKINGS")
            print(f"{'='*70}")
            print(f"  {'Ticker':<8} {'Score':>6} {'Dir':>8} {'Conviction':>12} {'Aligned':>8} {'Social':>10}")
            print(f"  {'─'*60}")
            for r in results:
                social_tag = f"📈 WSB#{r.social.wsb_rank}" if r.social.wsb_rank else "—"
                print(f"  {r.ticker:<8} {r.composite_score:>6.1f} {r.direction.value:>8} "
                      f"{r.conviction:>12} {r.layers_aligned:>5}/4 {social_tag:>10}")

    # Telegram alerts
    if args.telegram:
        conviction_order = ["NONE", "LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
        min_level = conviction_order.index(args.telegram_min_conviction)
        sent = 0
        for r in results:
            r_level = conviction_order.index(r.conviction)
            if r_level >= min_level:
                if send_telegram_alert(r):
                    sent += 1
            else:
                log.info(f"[TELEGRAM] Skipping {r.ticker} — conviction {r.conviction} below threshold {args.telegram_min_conviction}")
        print(f"\n📱 Telegram: {sent}/{len(results)} alerts sent (min conviction: {args.telegram_min_conviction})")

if __name__ == "__main__":
    main()