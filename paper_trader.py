"""Alpaca paper trading integration for ChainSignal.

Places option orders on Alpaca's paper environment based on StrategyResult
trade plans. Tracks positions and manages exits (profit target, stop loss,
trailing stop, theta kill).
"""
from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

from dotenv import load_dotenv
from db import log_paper_event, load_paper_positions, save_paper_positions, load_closed_paper_positions

import httpx

load_dotenv()

log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

_trading_client = None
_data_client = None
_stock_data_client = None


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


def _get_stock_data_client():
    global _stock_data_client
    if _stock_data_client is not None:
        return _stock_data_client
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return None
    from alpaca.data.historical.stock import StockHistoricalDataClient
    _stock_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    return _stock_data_client


def _get_alpaca_tickers(tc) -> set[str]:
    """Get tickers with open positions or pending orders on Alpaca (source of truth for dedup)."""
    tickers = set()
    try:
        positions = tc.get_all_positions()
        for p in positions:
            sym = p.symbol
            for i, ch in enumerate(sym):
                if ch.isdigit():
                    tickers.add(sym[:i])
                    break
    except Exception as e:
        log.error("[PAPER] Failed to get Alpaca positions for dedup: %s", e)
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        orders = tc.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN))
        for o in orders:
            sym = o.symbol
            for i, ch in enumerate(sym):
                if ch.isdigit():
                    tickers.add(sym[:i])
                    break
    except Exception as e:
        log.error("[PAPER] Failed to get Alpaca open orders for dedup: %s", e)
    return tickers


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
    strategy_type: str = "SWING"


_positions: list[PaperPosition] = []
_positions_loaded: bool = False


def _load_positions():
    global _positions, _positions_loaded
    data = load_paper_positions()
    if data is None:
        log.warning("[PAPER] Supabase load failed — will retry next cycle")
        return
    known_fields = set(PaperPosition.__dataclass_fields__)
    _positions = [PaperPosition(**{k: v for k, v in row.items() if k in known_fields}) for row in data]
    _positions_loaded = True


def _save_positions():
    rows = [asdict(p) for p in _positions if p.order_id]
    save_paper_positions(rows)


def get_open_positions() -> list[PaperPosition]:
    if not _positions_loaded:
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

    alpaca_tickers = _get_alpaca_tickers(tc)
    if result.ticker in alpaca_tickers:
        log.info("[PAPER] %s: Already has open position on Alpaca — skipping duplicate", result.ticker)
        return None

    if not _positions_loaded:
        _load_positions()
    open_swing_tickers = {p.ticker for p in _positions
                          if p.status in ("PENDING", "FILLED") and p.strategy_type == "SWING"}
    if result.ticker in open_swing_tickers:
        log.info("[PAPER] %s: Already has open swing position in local tracking — skipping duplicate", result.ticker)
        return None

    is_bull = result.direction == Signal.BULLISH
    option_type = "CALL" if is_bull else "PUT"

    strike = tp.suggested_strike
    expiry = tp.suggested_expiry
    if not expiry:
        log.info("[PAPER] %s: No expiry in trade plan — skipping", result.ticker)
        return None

    occ_symbol = _build_occ_symbol(result.ticker, expiry, option_type, strike)

    mid_price = _get_option_mid_price(occ_symbol)
    if mid_price and mid_price > 0:
        limit_price = mid_price
    else:
        limit_price = round(max(tp.entry_price * (tp.target_pct / 100) / tp.option_leverage, 0.50), 2)
        log.warning("[PAPER] %s: Could not get market price for %s — using estimate $%.2f",
                    result.ticker, occ_symbol, limit_price)

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

    if not _positions_loaded:
        _load_positions()
    _positions.append(pos)
    _save_positions()
    return pos if pos.status != "REJECTED" else None


def check_and_manage_positions():
    """Check all open positions, update fills, and manage exits."""
    tc = _get_trading_client()
    if not tc:
        return

    if not _positions_loaded:
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

            if pos.strategy_type == "LEAP" and pos.underlying_entry > 0:
                try:
                    from alpaca.data.requests import StockLatestQuoteRequest
                    sdc = _get_stock_data_client()
                    if not sdc:
                        raise RuntimeError("Stock data client unavailable")
                    sq = sdc.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[pos.ticker]))
                    stock_quote = sq.get(pos.ticker)
                    if stock_quote:
                        bid = float(stock_quote.bid_price or 0)
                        ask = float(stock_quote.ask_price or 0)
                        if bid <= 0 and ask <= 0:
                            log.debug("[PAPER] %s: No valid stock quote — skipping underlying check", pos.ticker)
                        else:
                            stock_price = (bid + ask) / 2 if (bid > 0 and ask > 0) else max(bid, ask)
                            stock_move_pct = (stock_price - pos.underlying_entry) / pos.underlying_entry * 100
                            is_bull = pos.direction == "BULLISH"
                            if (is_bull and stock_move_pct <= -25) or (not is_bull and stock_move_pct >= 25):
                                _close_position(tc, pos, current_price,
                                                f"underlying stop ({pos.ticker} {stock_move_pct:+.1f}% "
                                                f"from ${pos.underlying_entry:.2f})")
                                continue
                except Exception as e:
                    log.error("[PAPER] LEAP underlying check failed for %s: %s", pos.ticker, e)

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

            hold_start = pos.filled_at or pos.opened_at
            days_held = (now - datetime.fromisoformat(hold_start)).days
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
    """Check if a pending order has been filled, cancelled, or gone stale."""
    if not pos.order_id:
        return

    try:
        order = tc.get_order_by_id(pos.order_id)
        status = getattr(order.status, "value", str(order.status)).lower()

        if status == "filled":
            avg_price = float(order.filled_avg_price) if order.filled_avg_price else pos.limit_price
            pos.status = "FILLED"
            pos.filled_price = avg_price
            pos.filled_at = now.isoformat()
            pos.peak_premium = avg_price
            if pos.strategy_type == "LEAP":
                try:
                    from alpaca.data.requests import StockLatestQuoteRequest
                    sdc = _get_stock_data_client()
                    if sdc:
                        sq = sdc.get_stock_latest_quote(
                            StockLatestQuoteRequest(symbol_or_symbols=[pos.ticker]))
                        stock_quote = sq.get(pos.ticker)
                        if stock_quote:
                            bid = float(stock_quote.bid_price or 0)
                            ask = float(stock_quote.ask_price or 0)
                            if bid > 0 and ask > 0:
                                pos.underlying_entry = (bid + ask) / 2
                            elif bid > 0 or ask > 0:
                                pos.underlying_entry = max(bid, ask)
                            else:
                                log.warning("[PAPER] %s: Zero stock quotes on fill — keeping original underlying_entry $%.2f",
                                            pos.ticker, pos.underlying_entry)
                                bid = None
                            if bid is not None:
                                log.info("[PAPER] %s: Updated LEAP underlying_entry to $%.2f on fill",
                                         pos.ticker, pos.underlying_entry)
                except Exception as e:
                    log.error("[PAPER] %s: Failed to update underlying on fill: %s", pos.ticker, e)
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
        elif status in ("new", "accepted", "partially_filled", "pending_new"):
            max_pending_hours = 48 if pos.strategy_type == "LEAP" else 24
            if pos.opened_at:
                age_hours = (now - datetime.fromisoformat(pos.opened_at)).total_seconds() / 3600
                if age_hours > max_pending_hours:
                    try:
                        tc.cancel_order_by_id(pos.order_id)
                        log.info("[PAPER] %s: Cancelled stale %s order (pending %.0fh > %dh limit)",
                                 pos.ticker, pos.strategy_type, age_hours, max_pending_hours)
                        pos.status = "CLOSED"
                        pos.close_reason = f"order expired ({age_hours:.0f}h pending)"
                        pos.closed_at = now.isoformat()
                        log_paper_event(
                            pos.order_id, pos.ticker, "ORDER_EXPIRED",
                            direction=pos.direction, option_type=pos.option_type,
                            strike=pos.strike, expiry=pos.expiry,
                            close_reason=pos.close_reason,
                        )
                    except Exception as e:
                        log.error("[PAPER] %s: Failed to cancel stale order, will retry: %s",
                                  pos.ticker, e)
    except Exception as e:
        log.error("[PAPER] Failed to check order for %s: %s", pos.ticker, e)


def _get_option_mid_price(occ_symbol: str) -> Optional[float]:
    """Get the current mid price of an option from Alpaca market data."""
    dc = _get_data_client()
    if not dc:
        return None
    try:
        from alpaca.data.requests import OptionLatestQuoteRequest
        req = OptionLatestQuoteRequest(symbol_or_symbols=[occ_symbol])
        quotes = dc.get_option_latest_quote(req)
        quote = quotes.get(occ_symbol)
        if quote and quote.bid_price and quote.ask_price:
            return round((float(quote.bid_price) + float(quote.ask_price)) / 2, 2)
        if quote and quote.bid_price:
            return float(quote.bid_price)
        if quote and quote.ask_price:
            return float(quote.ask_price)
    except Exception as e:
        log.error("[PAPER] Failed to get mid price for %s: %s", occ_symbol, e)
    return None


def _get_current_option_price(pos: PaperPosition) -> Optional[float]:
    """Get the current mid price of a position's option."""
    return _get_option_mid_price(pos.occ_symbol)


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
    if "underlying stop" in r:
        return "UNDERLYING_STOP"
    return "CLOSED"


def _close_position(tc, pos: PaperPosition, current_price: float, reason: str):
    """Close a position via Alpaca."""
    try:
        tc.close_position(symbol_or_asset_id=pos.occ_symbol)
    except Exception as e:
        err_str = str(e).lower()
        if "not found" in err_str or "404" in err_str or "no position" in err_str:
            log.warning("[PAPER] %s: Position already gone on Alpaca — marking closed locally", pos.ticker)
        else:
            log.error("[PAPER] Failed to close %s via Alpaca: %s", pos.ticker, e)
            return

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


def place_leap_trade(result, trade_plan) -> Optional[PaperPosition]:
    """Place a paper LEAP option order via Alpaca."""
    tc = _get_trading_client()
    if not tc:
        log.warning("[LEAP] No trading client — skipping")
        return None

    if not trade_plan or not trade_plan.suggested_strike or not trade_plan.suggested_expiry:
        log.info("[LEAP] %s: No strike/expiry in trade plan — skipping", result.ticker)
        return None

    from strategies import Signal
    is_bull = result.direction == Signal.BULLISH
    option_type = "CALL" if is_bull else "PUT"

    strike = trade_plan.suggested_strike
    expiry = trade_plan.suggested_expiry
    occ_symbol = _build_occ_symbol(result.ticker, expiry, option_type, strike)

    mid_price = _get_option_mid_price(occ_symbol)
    if not mid_price or mid_price <= 0:
        log.warning("[LEAP] %s: Could not get option price for %s — skipping", result.ticker, occ_symbol)
        return None

    alpaca_tickers = _get_alpaca_tickers(tc)
    if result.ticker in alpaca_tickers:
        log.info("[LEAP] %s: Already has open position on Alpaca — skipping duplicate", result.ticker)
        return None

    if not _positions_loaded:
        _load_positions()
    open_leap_tickers = {p.ticker for p in _positions
                         if p.status in ("PENDING", "FILLED") and p.strategy_type == "LEAP"}
    if result.ticker in open_leap_tickers:
        log.info("[LEAP] %s: Already has open LEAP position in local tracking — skipping duplicate", result.ticker)
        return None

    MAX_LEAP_ALLOCATION = 0.20
    try:
        acct = tc.get_account()
        equity = float(acct.equity)
        leap_exposure = sum(
            (p.filled_price or p.limit_price) * 100 * p.quantity
            for p in _positions
            if p.status in ("PENDING", "FILLED") and p.strategy_type == "LEAP"
        )
        new_cost = mid_price * 100
        if (leap_exposure + new_cost) / equity > MAX_LEAP_ALLOCATION:
            log.info(
                "[LEAP] %s: Would exceed 20%% LEAP allocation "
                "(current=$%,.0f + new=$%,.0f vs limit=$%,.0f) — skipping",
                result.ticker, leap_exposure, new_cost, equity * MAX_LEAP_ALLOCATION,
            )
            return None
    except Exception as e:
        log.error("[LEAP] %s: Could not check allocation — skipping: %s", result.ticker, e)
        return None

    pos = PaperPosition(
        ticker=result.ticker,
        direction=result.direction.value,
        option_type=option_type,
        strike=strike,
        expiry=expiry[:10],
        quantity=1,
        limit_price=mid_price,
        occ_symbol=occ_symbol,
        strategy_type="LEAP",
        premium_target_pct=trade_plan.premium_target_pct or 99999.0,
        premium_stop_pct=trade_plan.premium_stop_pct,
        trail_activate_pct=trade_plan.trail_activate_pct or 100.0,
        trail_stop_pct=trade_plan.trail_stop_pct,
        max_hold_days=trade_plan.max_hold_days,
        theta_kill_days=trade_plan.theta_kill_days,
        theta_kill_move_pct=trade_plan.theta_kill_move_pct,
        underlying_entry=trade_plan.entry_price,
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
            limit_price=mid_price,
            position_intent=PositionIntent.BUY_TO_OPEN,
        )
        order = tc.submit_order(order_data=order_req)
        pos.order_id = str(order.id)
        pos.status = "PENDING"
        log.info(
            "[LEAP] %s: Order placed — %s $%.0f exp %s @ $%.2f limit | %s",
            result.ticker, option_type, strike, expiry[:10], mid_price, occ_symbol,
        )
        log_paper_event(
            pos.order_id, result.ticker, "LEAP_ENTRY",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10], price=mid_price,
            metadata={
                "occ_symbol": occ_symbol,
                "strategy_type": "LEAP",
                "conviction": result.conviction,
                "composite_score": result.composite_score,
                "premium_target_pct": pos.premium_target_pct,
                "premium_stop_pct": pos.premium_stop_pct,
                "trail_activate_pct": pos.trail_activate_pct,
                "trail_stop_pct": pos.trail_stop_pct,
                "max_hold_days": pos.max_hold_days,
                "underlying_entry": pos.underlying_entry,
            },
        )
        _send_paper_telegram(
            f"🔭 <b>LEAP TRADE OPENED</b>\n"
            f"{result.ticker} {option_type} ${strike:.0f} exp {expiry[:10]}\n"
            f"Limit: ${mid_price:.2f} | {result.conviction}\n"
            f"Stop: {pos.premium_stop_pct:.0f}% | No fixed TP — trail decides exit\n"
            f"Trail: +{pos.trail_activate_pct:.0f}% activate → {pos.trail_stop_pct:.0f}% from peak\n"
            f"Max hold: {pos.max_hold_days}d | No theta kill"
        )
    except Exception as e:
        log.error("[LEAP] %s: Order failed — %s", result.ticker, e)
        pos.status = "REJECTED"
        pos.close_reason = str(e)
        log_paper_event(
            occ_symbol, result.ticker, "LEAP_ORDER_REJECTED",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10],
            close_reason=str(e),
        )

    if not _positions_loaded:
        _load_positions()
    _positions.append(pos)
    _save_positions()
    return pos if pos.status != "REJECTED" else None


def get_portfolio_summary() -> dict:
    """Return summary stats for all paper positions."""
    if not _positions_loaded:
        _load_positions()

    open_pos = [p for p in _positions if p.status == "FILLED"]
    closed_rows = load_closed_paper_positions()
    closed_pnls = [float(r["pnl_pct"]) for r in closed_rows]
    wins = [p for p in closed_pnls if p > 0]
    losses = [p for p in closed_pnls if p <= 0]

    return {
        "open_positions": len(open_pos),
        "total_closed": len(closed_pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(closed_pnls) * 100 if closed_pnls else 0,
        "avg_win": sum(wins) / len(wins) if wins else 0,
        "avg_loss": sum(losses) / len(losses) if losses else 0,
        "total_pnl": sum(closed_pnls),
    }


_trade_stream = None
_option_stream = None
_subscribed_symbols: set[str] = set()


def _sync_save():
    """Thread-safe save — called from async handlers via run_in_executor."""
    _save_positions()


async def _handle_trade_update(data):
    """Process a real-time trade update from Alpaca's trading stream."""
    try:
        event = str(getattr(data, "event", "") or "")
        order_obj = getattr(data, "order", None)
        if not order_obj:
            return
        order_id = str(getattr(order_obj, "id", "") or "")
        if not order_id:
            return

        if not _positions_loaded:
            _load_positions()

        pos = next((p for p in _positions if p.order_id == order_id), None)
        if not pos:
            return

        now = datetime.now()
        loop = asyncio.get_event_loop()

        if event == "fill":
            raw_price = getattr(order_obj, "filled_avg_price", None)
            avg_price = float(raw_price) if raw_price else pos.limit_price
            pos.status = "FILLED"
            pos.filled_price = avg_price
            pos.filled_at = now.isoformat()
            pos.peak_premium = avg_price
            if pos.strategy_type == "LEAP":
                try:
                    from alpaca.data.requests import StockLatestQuoteRequest
                    sdc = _get_stock_data_client()
                    if sdc:
                        sq = await loop.run_in_executor(
                            None, lambda: sdc.get_stock_latest_quote(
                                StockLatestQuoteRequest(symbol_or_symbols=[pos.ticker])))
                        stock_quote = sq.get(pos.ticker)
                        if stock_quote:
                            bid = float(stock_quote.bid_price or 0)
                            ask = float(stock_quote.ask_price or 0)
                            if bid > 0 and ask > 0:
                                pos.underlying_entry = (bid + ask) / 2
                            elif bid > 0 or ask > 0:
                                pos.underlying_entry = max(bid, ask)
                except Exception as e:
                    log.error("[PAPER] %s: Failed to update underlying on fill: %s", pos.ticker, e)
            log.info("[PAPER] %s: FILLED @ $%.2f (real-time)", pos.ticker, avg_price)
            await loop.run_in_executor(None, lambda: log_paper_event(
                pos.order_id, pos.ticker, "FILL",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                filled_price=avg_price, price=avg_price,
            ))
            await loop.run_in_executor(None, lambda: _send_paper_telegram(
                f"✅ <b>PAPER FILL</b>\n"
                f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
                f"Filled @ ${avg_price:.2f}"
            ))
            await loop.run_in_executor(None, _sync_save)
            await _subscribe_option_quotes([pos.occ_symbol])

        elif event in ("canceled", "expired", "rejected"):
            pos.status = "CLOSED"
            pos.close_reason = event
            pos.closed_at = now.isoformat()
            log.info("[PAPER] %s: Order %s (real-time)", pos.ticker, event.upper())
            await loop.run_in_executor(None, lambda: log_paper_event(
                pos.order_id, pos.ticker, f"ORDER_{event.upper()}",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                close_reason=event,
            ))
            await loop.run_in_executor(None, _sync_save)

    except Exception as e:
        log.error("[PAPER] Trade stream event error: %s", e)


async def _handle_option_quote(data):
    """Process real-time option quote — run exit checks instantly."""
    try:
        symbol = str(getattr(data, "symbol", "") or "")
        if not symbol:
            return

        bid = float(getattr(data, "bid_price", 0) or 0)
        ask = float(getattr(data, "ask_price", 0) or 0)
        if bid <= 0 and ask <= 0:
            return
        current_price = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else max(bid, ask)

        if not _positions_loaded:
            _load_positions()

        pos = next((p for p in _positions
                     if p.occ_symbol == symbol and p.status == "FILLED" and p.filled_price),
                    None)
        if not pos:
            return

        entry = pos.filled_price
        pnl_pct = (current_price - entry) / entry * 100

        if pos.peak_premium is None or current_price > pos.peak_premium:
            pos.peak_premium = current_price

        tc = _get_trading_client()
        if not tc:
            return

        now = datetime.now()
        loop = asyncio.get_event_loop()

        if pnl_pct <= pos.premium_stop_pct:
            await loop.run_in_executor(
                None, lambda: _close_position(tc, pos, current_price, f"hard stop ({pnl_pct:+.1f}%)"))
            await loop.run_in_executor(None, _sync_save)
            await _unsubscribe_option_quotes([symbol])
            return

        if pos.strategy_type == "LEAP" and pos.underlying_entry > 0:
            try:
                from alpaca.data.requests import StockLatestQuoteRequest
                sdc = _get_stock_data_client()
                if sdc:
                    sq = await loop.run_in_executor(
                        None, lambda: sdc.get_stock_latest_quote(
                            StockLatestQuoteRequest(symbol_or_symbols=[pos.ticker])))
                    stock_quote = sq.get(pos.ticker)
                    if stock_quote:
                        sbid = float(stock_quote.bid_price or 0)
                        sask = float(stock_quote.ask_price or 0)
                        if sbid > 0 or sask > 0:
                            stock_price = (sbid + sask) / 2 if (sbid > 0 and sask > 0) else max(sbid, sask)
                            stock_move_pct = (stock_price - pos.underlying_entry) / pos.underlying_entry * 100
                            is_bull = pos.direction == "BULLISH"
                            if (is_bull and stock_move_pct <= -25) or (not is_bull and stock_move_pct >= 25):
                                await loop.run_in_executor(
                                    None, lambda: _close_position(
                                        tc, pos, current_price,
                                        f"underlying stop ({pos.ticker} {stock_move_pct:+.1f}% "
                                        f"from ${pos.underlying_entry:.2f})"))
                                await loop.run_in_executor(None, _sync_save)
                                await _unsubscribe_option_quotes([symbol])
                                return
            except Exception as e:
                log.error("[PAPER] LEAP underlying check failed for %s: %s", pos.ticker, e)

        if pnl_pct >= pos.premium_target_pct:
            await loop.run_in_executor(
                None, lambda: _close_position(tc, pos, current_price, f"profit target ({pnl_pct:+.1f}%)"))
            await loop.run_in_executor(None, _sync_save)
            await _unsubscribe_option_quotes([symbol])
            return

        if pnl_pct >= pos.trail_activate_pct and not pos.trail_active:
            pos.trail_active = True
            await loop.run_in_executor(None, lambda: log_paper_event(
                pos.order_id, pos.ticker, "TRAIL_ACTIVATED",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                price=current_price, filled_price=pos.filled_price,
                pnl_pct=round(pnl_pct, 2), peak_premium=pos.peak_premium,
                trail_active=True,
            ))

        if pos.trail_active and pos.peak_premium:
            drawdown_from_peak = (pos.peak_premium - current_price) / pos.peak_premium * 100
            if drawdown_from_peak >= pos.trail_stop_pct:
                await loop.run_in_executor(
                    None, lambda: _close_position(
                        tc, pos, current_price,
                        f"trailing stop (peak=${pos.peak_premium:.2f}, drawdown={drawdown_from_peak:.1f}%)"))
                await loop.run_in_executor(None, _sync_save)
                await _unsubscribe_option_quotes([symbol])
                return

        hold_start = pos.filled_at or pos.opened_at
        if hold_start:
            days_held = (now - datetime.fromisoformat(hold_start)).days
            if days_held >= pos.theta_kill_days and abs(pnl_pct) < pos.theta_kill_move_pct:
                await loop.run_in_executor(
                    None, lambda: _close_position(
                        tc, pos, current_price,
                        f"theta kill (day {days_held}, move={pnl_pct:+.1f}%)"))
                await loop.run_in_executor(None, _sync_save)
                await _unsubscribe_option_quotes([symbol])
                return
            if days_held >= pos.max_hold_days:
                await loop.run_in_executor(
                    None, lambda: _close_position(tc, pos, current_price, f"max hold ({days_held}d)"))
                await loop.run_in_executor(None, _sync_save)
                await _unsubscribe_option_quotes([symbol])
                return

    except Exception as e:
        log.error("[PAPER] Option quote handler error for %s: %s",
                  getattr(data, "symbol", "?"), e)


async def _subscribe_option_quotes(symbols: list[str]):
    """Subscribe to real-time quotes for option symbols."""
    global _option_stream
    if not _option_stream or not symbols:
        return
    new_syms = [s for s in symbols if s not in _subscribed_symbols]
    if not new_syms:
        return
    try:
        _option_stream.subscribe_quotes(_handle_option_quote, *new_syms)
        _subscribed_symbols.update(new_syms)
        log.info("[PAPER] Subscribed to real-time quotes: %s", ", ".join(new_syms))
    except Exception as e:
        log.error("[PAPER] Failed to subscribe option quotes: %s", e)


async def _unsubscribe_option_quotes(symbols: list[str]):
    """Unsubscribe from closed position quotes."""
    global _option_stream
    if not _option_stream or not symbols:
        return
    try:
        _option_stream.unsubscribe_quotes(*symbols)
        _subscribed_symbols.discard(symbols[0]) if len(symbols) == 1 else _subscribed_symbols.difference_update(symbols)
        log.info("[PAPER] Unsubscribed from quotes: %s", ", ".join(symbols))
    except Exception as e:
        log.error("[PAPER] Failed to unsubscribe option quotes: %s", e)


async def start_trade_stream():
    """Start Alpaca's real-time trading stream for instant fill detection."""
    global _trade_stream
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        log.warning("[PAPER] No Alpaca credentials — trade stream disabled")
        return
    try:
        from alpaca.trading.stream import TradingStream
        _trade_stream = TradingStream(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
        _trade_stream.subscribe_trade_updates(_handle_trade_update)
        log.info("[PAPER] 🔴 Real-time trade stream starting")
        await _trade_stream._run_forever()
    except Exception as e:
        log.error("[PAPER] Trade stream error: %s", e)


async def start_option_stream():
    """Start real-time option quote stream for instant exit management."""
    global _option_stream
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        log.warning("[PAPER] No Alpaca credentials — option stream disabled")
        return
    try:
        from alpaca.data.live.option import OptionDataStream
        _option_stream = OptionDataStream(ALPACA_API_KEY, ALPACA_SECRET_KEY)

        if not _positions_loaded:
            _load_positions()
        filled_symbols = [p.occ_symbol for p in _positions if p.status == "FILLED" and p.filled_price]
        if filled_symbols:
            _option_stream.subscribe_quotes(_handle_option_quote, *filled_symbols)
            _subscribed_symbols.update(filled_symbols)
            log.info("[PAPER] Subscribed to %d option quote streams: %s",
                     len(filled_symbols), ", ".join(filled_symbols))

        log.info("[PAPER] 📊 Real-time option quote stream starting")
        await _option_stream._run_forever()
    except Exception as e:
        log.error("[PAPER] Option stream error: %s", e)
