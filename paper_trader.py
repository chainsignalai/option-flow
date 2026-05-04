"""Alpaca paper trading integration for ChainSignal.

Places option orders on Alpaca's paper environment based on StrategyResult
trade plans. Tracks positions and manages exits (profit target, stop loss,
trailing stop, theta kill).
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

from dotenv import load_dotenv
from db import log_paper_event

import httpx

load_dotenv()

log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

_trading_client = None
_data_client = None


def _get_trading_client():
    global _trading_client
    if _trading_client is not None:
        return _trading_client
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        log.warning("[PAPER] Alpaca credentials not set — paper trading disabled")
        return None
    from alpaca.trading.client import TradingClient
    _trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
    log.info("[PAPER] Alpaca paper trading client initialized")
    return _trading_client


def _get_data_client():
    global _data_client
    if _data_client is not None:
        return _data_client
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return None
    from alpaca.data.historical.option import OptionHistoricalDataClient
    _data_client = OptionHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    return _data_client


def _send_paper_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        httpx.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
    except Exception as e:
        log.error("[PAPER] Telegram send failed: %s", e)


def _build_occ_symbol(ticker: str, expiry: str, option_type: str, strike: float) -> str:
    """Build OCC option symbol: AAPL260515C00200000"""
    root = ticker.upper()
    exp = datetime.strptime(expiry[:10], "%Y-%m-%d").strftime("%y%m%d")
    cp = "C" if option_type == "CALL" else "P"
    strike_int = int(round(strike * 1000))
    return f"{root}{exp}{cp}{strike_int:08d}"


@dataclass
class PaperPosition:
    """Tracks a paper-traded option position."""
    ticker: str
    direction: str
    option_type: str
    strike: float
    expiry: str
    quantity: int
    limit_price: float
    occ_symbol: str
    order_id: str = ""
    status: str = "PENDING"
    filled_price: Optional[float] = None
    filled_at: Optional[str] = None
    premium_target_pct: float = 50.0
    premium_stop_pct: float = -40.0
    trail_activate_pct: float = 30.0
    trail_stop_pct: float = 20.0
    max_hold_days: int = 10
    theta_kill_days: int = 5
    theta_kill_move_pct: float = 10.0
    underlying_entry: float = 0.0
    peak_premium: Optional[float] = None
    trail_active: bool = False
    opened_at: str = ""
    closed_at: str = ""
    close_reason: str = ""
    close_price: Optional[float] = None
    pnl_pct: Optional[float] = None


POSITIONS_FILE = os.path.join(os.path.dirname(__file__), "paper_positions.json")
_positions: list[PaperPosition] = []


def _load_positions():
    global _positions
    try:
        with open(POSITIONS_FILE, "r") as f:
            data = json.load(f)
            _positions = [PaperPosition(**p) for p in data]
            log.info("[PAPER] Loaded %d positions from disk", len(_positions))
    except (FileNotFoundError, json.JSONDecodeError):
        _positions = []


def _save_positions():
    with open(POSITIONS_FILE, "w") as f:
        json.dump([asdict(p) for p in _positions], f, indent=2)


def get_open_positions() -> list[PaperPosition]:
    if not _positions:
        _load_positions()
    return [p for p in _positions if p.status in ("PENDING", "FILLED")]


def place_paper_trade(result) -> Optional[PaperPosition]:
    """Place a paper option order via Alpaca based on a StrategyResult."""
    tc = _get_trading_client()
    if not tc:
        log.warning("[PAPER] No trading client — skipping")
        return None

    tp = result.trade_plan
    if not tp or not tp.entry_price or not tp.suggested_strike:
        log.info("[PAPER] %s: No trade plan or no suggested contract — skipping", result.ticker)
        return None

    from strategies import Signal
    if result.direction == Signal.NEUTRAL:
        return None

    if not _positions:
        _load_positions()
    open_tickers = {p.ticker for p in _positions if p.status in ("PENDING", "FILLED")}
    if result.ticker in open_tickers:
        log.info("[PAPER] %s: Already has open position — skipping duplicate", result.ticker)
        return None

    is_bull = result.direction == Signal.BULLISH
    option_type = "CALL" if is_bull else "PUT"

    strike = tp.suggested_strike
    expiry = tp.suggested_expiry
    if not expiry:
        log.info("[PAPER] %s: No expiry in trade plan — skipping", result.ticker)
        return None

    occ_symbol = _build_occ_symbol(result.ticker, expiry, option_type, strike)

    estimated_premium = tp.entry_price * (tp.target_pct / 100) / tp.option_leverage
    limit_price = round(max(estimated_premium, 0.50), 2)

    pos = PaperPosition(
        ticker=result.ticker,
        direction=result.direction.value,
        option_type=option_type,
        strike=strike,
        expiry=expiry[:10],
        quantity=1,
        limit_price=limit_price,
        occ_symbol=occ_symbol,
        premium_target_pct=tp.premium_target_pct or 50.0,
        premium_stop_pct=tp.premium_stop_pct,
        trail_activate_pct=tp.trail_activate_pct or 30.0,
        trail_stop_pct=tp.trail_stop_pct,
        max_hold_days=tp.max_hold_days,
        theta_kill_days=tp.theta_kill_days,
        theta_kill_move_pct=tp.theta_kill_move_pct,
        underlying_entry=tp.entry_price,
        opened_at=datetime.now().isoformat(),
    )

    try:
        from alpaca.trading.requests import LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce, PositionIntent

        order_req = LimitOrderRequest(
            symbol=occ_symbol,
            qty=1,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC,
            limit_price=limit_price,
            position_intent=PositionIntent.BUY_TO_OPEN,
        )
        order = tc.submit_order(order_data=order_req)
        pos.order_id = str(order.id)
        pos.status = "PENDING"
        log.info(
            "[PAPER] %s: Order placed — %s $%.0f exp %s @ $%.2f limit | %s | order_id=%s",
            result.ticker, option_type, strike, expiry[:10], limit_price,
            occ_symbol, pos.order_id,
        )
        log_paper_event(
            pos.order_id, result.ticker, "ENTRY",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10], price=limit_price,
            metadata={
                "occ_symbol": occ_symbol,
                "conviction": result.conviction,
                "composite_score": result.composite_score,
                "premium_target_pct": pos.premium_target_pct,
                "premium_stop_pct": pos.premium_stop_pct,
                "trail_activate_pct": pos.trail_activate_pct,
                "trail_stop_pct": pos.trail_stop_pct,
                "theta_kill_days": pos.theta_kill_days,
                "max_hold_days": pos.max_hold_days,
                "underlying_entry": pos.underlying_entry,
            },
        )
        _send_paper_telegram(
            f"📄 <b>PAPER TRADE OPENED</b>\n"
            f"{result.ticker} {option_type} ${strike:.0f} exp {expiry[:10]}\n"
            f"Limit: ${limit_price:.2f} | {result.conviction}\n"
            f"TP: {pos.premium_target_pct:+.0f}% | Stop: {pos.premium_stop_pct:+.0f}% | "
            f"Trail: {pos.trail_activate_pct:.0f}%/{pos.trail_stop_pct:.0f}%"
        )
    except Exception as e:
        log.error("[PAPER] %s: Order failed — %s", result.ticker, e)
        pos.status = "REJECTED"
        pos.close_reason = str(e)
        log_paper_event(
            occ_symbol, result.ticker, "ORDER_REJECTED",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10],
            close_reason=str(e),
        )

    if not _positions:
        _load_positions()
    _positions.append(pos)
    _save_positions()
    return pos if pos.status != "REJECTED" else None


def check_and_manage_positions():
    """Check all open positions, update fills, and manage exits."""
    tc = _get_trading_client()
    if not tc:
        return

    if not _positions:
        _load_positions()

    open_positions = [p for p in _positions if p.status in ("PENDING", "FILLED")]
    if not open_positions:
        return

    now = datetime.now()

    for pos in open_positions:
        try:
            if pos.status == "PENDING":
                _check_pending_order(tc, pos, now)
                continue

            if pos.status != "FILLED" or not pos.filled_price:
                continue

            current_price = _get_current_option_price(pos)
            if current_price is None:
                continue

            entry = pos.filled_price
            pnl_pct = (current_price - entry) / entry * 100

            if pos.peak_premium is None or current_price > pos.peak_premium:
                pos.peak_premium = current_price

            if pnl_pct <= pos.premium_stop_pct:
                _close_position(tc, pos, current_price, f"hard stop ({pnl_pct:+.1f}%)")
                continue

            if pnl_pct >= pos.premium_target_pct:
                _close_position(tc, pos, current_price, f"profit target ({pnl_pct:+.1f}%)")
                continue

            if pnl_pct >= pos.trail_activate_pct and not pos.trail_active:
                pos.trail_active = True
                log_paper_event(
                    pos.order_id, pos.ticker, "TRAIL_ACTIVATED",
                    direction=pos.direction, option_type=pos.option_type,
                    strike=pos.strike, expiry=pos.expiry,
                    price=current_price, filled_price=pos.filled_price,
                    pnl_pct=round(pnl_pct, 2), peak_premium=pos.peak_premium,
                    trail_active=True,
                )
            if pos.trail_active and pos.peak_premium:
                drawdown_from_peak = (pos.peak_premium - current_price) / pos.peak_premium * 100
                if drawdown_from_peak >= pos.trail_stop_pct:
                    _close_position(tc, pos, current_price,
                                    f"trailing stop (peak=${pos.peak_premium:.2f}, "
                                    f"drawdown={drawdown_from_peak:.1f}%)")
                    continue

            days_held = (now - datetime.fromisoformat(pos.opened_at)).days
            if days_held >= pos.theta_kill_days:
                underlying_move = abs(pnl_pct)
                if underlying_move < pos.theta_kill_move_pct:
                    _close_position(tc, pos, current_price,
                                    f"theta kill (day {days_held}, move={pnl_pct:+.1f}%)")
                    continue

            if days_held >= pos.max_hold_days:
                _close_position(tc, pos, current_price, f"max hold ({days_held}d)")
                continue

        except Exception as e:
            log.error("[PAPER] Error managing %s: %s", pos.ticker, e)

    _save_positions()


def _check_pending_order(tc, pos: PaperPosition, now: datetime):
    """Check if a pending order has been filled, cancelled, etc."""
    if not pos.order_id:
        return
    try:
        order = tc.get_order_by_id(pos.order_id)
        status = str(order.status).lower()

        if status == "filled":
            avg_price = float(order.filled_avg_price) if order.filled_avg_price else pos.limit_price
            pos.status = "FILLED"
            pos.filled_price = avg_price
            pos.filled_at = now.isoformat()
            pos.peak_premium = avg_price
            log.info("[PAPER] %s: FILLED @ $%.2f", pos.ticker, avg_price)
            log_paper_event(
                pos.order_id, pos.ticker, "FILL",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                filled_price=avg_price, price=avg_price,
            )
            _send_paper_telegram(
                f"✅ <b>PAPER FILL</b>\n"
                f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
                f"Filled @ ${avg_price:.2f}"
            )
        elif status in ("canceled", "cancelled", "expired", "rejected"):
            pos.status = "CLOSED"
            pos.close_reason = status
            pos.closed_at = now.isoformat()
            log.info("[PAPER] %s: Order %s", pos.ticker, status.upper())
            log_paper_event(
                pos.order_id, pos.ticker, f"ORDER_{status.upper()}",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                close_reason=status,
            )
    except Exception as e:
        log.error("[PAPER] Failed to check order for %s: %s", pos.ticker, e)


def _get_current_option_price(pos: PaperPosition) -> Optional[float]:
    """Get the current mid price of the option from Alpaca market data."""
    dc = _get_data_client()
    if not dc:
        return None
    try:
        from alpaca.data.requests import OptionLatestQuoteRequest
        req = OptionLatestQuoteRequest(symbol_or_symbols=[pos.occ_symbol])
        quotes = dc.get_option_latest_quote(req)
        quote = quotes.get(pos.occ_symbol)
        if quote and quote.bid_price and quote.ask_price:
            return round((float(quote.bid_price) + float(quote.ask_price)) / 2, 2)
        if quote and quote.bid_price:
            return float(quote.bid_price)
        if quote and quote.ask_price:
            return float(quote.ask_price)
    except Exception as e:
        log.error("[PAPER] Failed to get price for %s (%s): %s", pos.ticker, pos.occ_symbol, e)
    return None


def _reason_to_event_type(reason: str) -> str:
    r = reason.lower()
    if "profit target" in r:
        return "TP_HIT"
    if "hard stop" in r:
        return "HARD_STOP"
    if "trailing stop" in r:
        return "TRAIL_STOP"
    if "theta kill" in r:
        return "THETA_KILL"
    if "max hold" in r:
        return "MAX_HOLD"
    return "CLOSED"


def _close_position(tc, pos: PaperPosition, current_price: float, reason: str):
    """Close a position via Alpaca."""
    try:
        tc.close_position(symbol_or_asset_id=pos.occ_symbol)
    except Exception as e:
        log.error("[PAPER] Failed to close %s via Alpaca: %s", pos.ticker, e)

    pos.status = "CLOSED"
    pos.close_reason = reason
    pos.close_price = current_price
    pos.closed_at = datetime.now().isoformat()
    if pos.filled_price:
        pos.pnl_pct = round((current_price - pos.filled_price) / pos.filled_price * 100, 2)

    log.info(
        "[PAPER] %s: CLOSED — %s | entry=$%.2f exit=$%.2f pnl=%s%%",
        pos.ticker, reason, pos.filled_price or 0, current_price,
        f"{pos.pnl_pct:+.1f}" if pos.pnl_pct is not None else "?"
    )

    event_type = _reason_to_event_type(reason)
    log_paper_event(
        pos.order_id, pos.ticker, event_type,
        direction=pos.direction, option_type=pos.option_type,
        strike=pos.strike, expiry=pos.expiry,
        price=current_price, filled_price=pos.filled_price,
        pnl_pct=pos.pnl_pct, peak_premium=pos.peak_premium,
        trail_active=pos.trail_active, close_reason=reason,
    )
    pnl_emoji = "🟢" if pos.pnl_pct and pos.pnl_pct > 0 else "🔴"
    _send_paper_telegram(
        f"{pnl_emoji} <b>PAPER TRADE CLOSED</b>\n"
        f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
        f"Entry: ${pos.filled_price:.2f} → Exit: ${current_price:.2f}\n"
        f"PnL: {pos.pnl_pct:+.1f}% | Reason: {reason}"
    )


def get_portfolio_summary() -> dict:
    """Return summary stats for all paper positions."""
    if not _positions:
        _load_positions()

    open_pos = [p for p in _positions if p.status == "FILLED"]
    closed = [p for p in _positions if p.status == "CLOSED" and p.pnl_pct is not None]
    wins = [p for p in closed if p.pnl_pct > 0]
    losses = [p for p in closed if p.pnl_pct <= 0]

    return {
        "open_positions": len(open_pos),
        "total_closed": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(closed) * 100 if closed else 0,
        "avg_win": sum(p.pnl_pct for p in wins) / len(wins) if wins else 0,
        "avg_loss": sum(p.pnl_pct for p in losses) / len(losses) if losses else 0,
        "total_pnl": sum(p.pnl_pct for p in closed),
    }
