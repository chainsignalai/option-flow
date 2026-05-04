"""Webull paper trading integration for ChainSignal.

Places option orders on Webull's test environment based on StrategyResult
trade plans. Tracks positions and manages exits (profit target, stop loss,
trailing stop, theta kill).
"""
from __future__ import annotations

import os
import uuid
import json
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional

from dotenv import load_dotenv
from db import log_paper_event

load_dotenv()

log = logging.getLogger(__name__)

WEBULL_APP_KEY = os.getenv("WEBULL_APP_KEY", "")
WEBULL_APP_SECRET = os.getenv("WEBULL_APP_SECRET", "")
WEBULL_ACCOUNT_ID = os.getenv("WEBULL_ACCOUNT_ID", "")
WEBULL_ENDPOINT = os.getenv("WEBULL_ENDPOINT", "us-openapi-alb.uat.webullbroker.com")

_client = None
_trade_client = None


def _get_trade_client():
    global _client, _trade_client
    if _trade_client is not None:
        return _trade_client
    if not WEBULL_APP_KEY or not WEBULL_APP_SECRET:
        log.warning("[PAPER] Webull credentials not set — paper trading disabled")
        return None
    from webull.core.client import ApiClient
    from webull.trade.trade_client import TradeClient
    _client = ApiClient(WEBULL_APP_KEY, WEBULL_APP_SECRET, "us")
    _client.add_endpoint("us", WEBULL_ENDPOINT)
    _trade_client = TradeClient(_client)
    log.info("[PAPER] Webull client initialized (endpoint: %s)", WEBULL_ENDPOINT)
    return _trade_client


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
    client_order_id: str
    webull_order_id: str = ""
    status: str = "PENDING"
    filled_price: Optional[float] = None
    filled_at: Optional[str] = None
    # Trade plan targets
    premium_target_pct: float = 50.0
    premium_stop_pct: float = -40.0
    trail_activate_pct: float = 30.0
    trail_stop_pct: float = 20.0
    max_hold_days: int = 10
    theta_kill_days: int = 5
    theta_kill_move_pct: float = 10.0
    underlying_entry: float = 0.0
    # Tracking
    peak_premium: Optional[float] = None
    trail_active: bool = False
    opened_at: str = ""
    closed_at: str = ""
    close_reason: str = ""
    close_price: Optional[float] = None
    pnl_pct: Optional[float] = None


# In-memory position tracker (persisted to JSON file)
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
    """Place a paper option order based on a StrategyResult with a trade plan.

    Returns the PaperPosition if order was placed, None otherwise.
    """
    tc = _get_trade_client()
    if not tc or not WEBULL_ACCOUNT_ID:
        log.warning("[PAPER] No trade client or account ID — skipping")
        return None

    tp = result.trade_plan
    if not tp or not tp.entry_price or not tp.suggested_strike:
        log.info("[PAPER] %s: No trade plan or no suggested contract — skipping", result.ticker)
        return None

    from strategies import Signal
    if result.direction == Signal.NEUTRAL:
        return None

    is_bull = result.direction == Signal.BULLISH
    option_type = "CALL" if is_bull else "PUT"

    # Use trade plan's suggested contract
    strike = tp.suggested_strike
    expiry = tp.suggested_expiry
    if not expiry:
        log.info("[PAPER] %s: No expiry in trade plan — skipping", result.ticker)
        return None

    # Estimate a reasonable limit price from the premium target
    # For paper trading, use a limit price at the mid-market estimate
    # We'll set it slightly above to ensure fill in test env
    estimated_premium = tp.entry_price * (tp.target_pct / 100) / tp.option_leverage
    limit_price = round(max(estimated_premium, 0.50), 2)

    client_order_id = uuid.uuid4().hex[:32]

    new_orders = [{
        "client_order_id": client_order_id,
        "combo_type": "NORMAL",
        "option_strategy": "SINGLE",
        "instrument_type": "OPTION",
        "market": "US",
        "symbol": result.ticker,
        "order_type": "LIMIT",
        "side": "BUY",
        "quantity": "1",
        "entrust_type": "QTY",
        "time_in_force": "GTC",
        "limit_price": str(limit_price),
        "legs": [{
            "side": "BUY",
            "quantity": "1",
            "symbol": result.ticker,
            "strike_price": f"{strike:.2f}",
            "option_expire_date": expiry[:10],
            "instrument_type": "OPTION",
            "option_type": option_type,
            "market": "US",
        }],
    }]

    pos = PaperPosition(
        ticker=result.ticker,
        direction=result.direction.value,
        option_type=option_type,
        strike=strike,
        expiry=expiry[:10],
        quantity=1,
        limit_price=limit_price,
        client_order_id=client_order_id,
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
        resp = tc.order_v2.place_option(WEBULL_ACCOUNT_ID, new_orders)
        data = resp.json()
        pos.webull_order_id = data.get("order_id", "")
        pos.status = "PENDING"
        log.info(
            "[PAPER] %s: Order placed — %s $%.0f %s exp %s @ $%.2f limit | order_id=%s",
            result.ticker, option_type, strike, expiry[:10], limit_price,
            limit_price, pos.webull_order_id,
        )
        log_paper_event(
            client_order_id, result.ticker, "ENTRY",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10], price=limit_price,
            metadata={
                "webull_order_id": pos.webull_order_id,
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
    except Exception as e:
        log.error("[PAPER] %s: Order failed — %s", result.ticker, e)
        pos.status = "REJECTED"
        pos.close_reason = str(e)
        log_paper_event(
            client_order_id, result.ticker, "ORDER_REJECTED",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10],
            close_reason=str(e),
        )

    if not _positions:
        _load_positions()
    _positions.append(pos)
    _save_positions()
    return pos


def check_and_manage_positions():
    """Check all open positions, update fills, and manage exits.

    Call this periodically (e.g. every few minutes from the live monitor).
    """
    tc = _get_trade_client()
    if not tc or not WEBULL_ACCOUNT_ID:
        return

    if not _positions:
        _load_positions()

    open_positions = [p for p in _positions if p.status in ("PENDING", "FILLED")]
    if not open_positions:
        return

    now = datetime.now()

    for pos in open_positions:
        try:
            # Check order status for pending orders
            if pos.status == "PENDING":
                resp = tc.order_v2.get_order_detail(WEBULL_ACCOUNT_ID, pos.client_order_id)
                detail = resp.json()
                orders = detail.get("orders", [])
                if orders:
                    order = orders[0]
                    wb_status = order.get("status", "")
                    filled_qty = float(order.get("filled_quantity", "0"))
                    if wb_status == "FILLED" or filled_qty > 0:
                        avg_price = float(order.get("avg_filled_price", order.get("limit_price", "0")))
                        pos.status = "FILLED"
                        pos.filled_price = avg_price
                        pos.filled_at = now.isoformat()
                        pos.peak_premium = avg_price
                        log.info("[PAPER] %s: FILLED @ $%.2f", pos.ticker, avg_price)
                        log_paper_event(
                            pos.client_order_id, pos.ticker, "FILL",
                            direction=pos.direction, option_type=pos.option_type,
                            strike=pos.strike, expiry=pos.expiry,
                            filled_price=avg_price, price=avg_price,
                        )
                    elif wb_status in ("CANCELLED", "REJECTED", "EXPIRED"):
                        pos.status = "CLOSED"
                        pos.close_reason = wb_status.lower()
                        pos.closed_at = now.isoformat()
                        log.info("[PAPER] %s: Order %s", pos.ticker, wb_status)
                        log_paper_event(
                            pos.client_order_id, pos.ticker,
                            f"ORDER_{wb_status}",
                            direction=pos.direction, option_type=pos.option_type,
                            strike=pos.strike, expiry=pos.expiry,
                            close_reason=wb_status.lower(),
                        )
                continue

            # For filled positions, check exit conditions
            if pos.status != "FILLED" or not pos.filled_price:
                continue

            # Get current option price from position data
            current_price = _get_current_option_price(tc, pos)
            if current_price is None:
                continue

            entry = pos.filled_price
            pnl_pct = (current_price - entry) / entry * 100

            # Update peak
            if pos.peak_premium is None or current_price > pos.peak_premium:
                pos.peak_premium = current_price

            # 1. Hard stop
            if pnl_pct <= pos.premium_stop_pct:
                _close_position(tc, pos, current_price, f"hard stop ({pnl_pct:+.1f}%)")
                continue

            # 2. Profit target
            if pnl_pct >= pos.premium_target_pct:
                _close_position(tc, pos, current_price, f"profit target ({pnl_pct:+.1f}%)")
                continue

            # 3. Trailing stop
            if pnl_pct >= pos.trail_activate_pct and not pos.trail_active:
                pos.trail_active = True
                log_paper_event(
                    pos.client_order_id, pos.ticker, "TRAIL_ACTIVATED",
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

            # 4. Theta kill
            days_held = (now - datetime.fromisoformat(pos.opened_at)).days
            if days_held >= pos.theta_kill_days:
                underlying_move = abs(pnl_pct)
                if underlying_move < pos.theta_kill_move_pct:
                    _close_position(tc, pos, current_price,
                                    f"theta kill (day {days_held}, move={pnl_pct:+.1f}%)")
                    continue

            # 5. Max hold
            if days_held >= pos.max_hold_days:
                _close_position(tc, pos, current_price, f"max hold ({days_held}d)")
                continue

        except Exception as e:
            log.error("[PAPER] Error managing %s: %s", pos.ticker, e)

    _save_positions()


def _get_current_option_price(tc, pos: PaperPosition) -> Optional[float]:
    """Get the current price of the option from Webull positions."""
    try:
        resp = tc.account_v2.get_account_position(WEBULL_ACCOUNT_ID)
        positions = resp.json()
        for p in positions:
            if p.get("instrument_type") != "OPTION":
                continue
            legs = p.get("legs", [])
            for leg in legs:
                if (leg.get("symbol") == pos.ticker and
                    leg.get("option_type") == pos.option_type and
                    leg.get("option_exercise_price") == f"{pos.strike:.2f}" and
                    leg.get("option_expire_date") == pos.expiry):
                    return float(leg.get("last_price", 0))
    except Exception as e:
        log.error("[PAPER] Failed to get price for %s: %s", pos.ticker, e)
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
    """Close a position by placing a sell order."""
    client_order_id = uuid.uuid4().hex[:32]
    new_orders = [{
        "client_order_id": client_order_id,
        "combo_type": "NORMAL",
        "option_strategy": "SINGLE",
        "instrument_type": "OPTION",
        "market": "US",
        "symbol": pos.ticker,
        "order_type": "LIMIT",
        "side": "SELL",
        "quantity": str(pos.quantity),
        "entrust_type": "QTY",
        "time_in_force": "DAY",
        "limit_price": f"{current_price:.2f}",
        "legs": [{
            "side": "SELL",
            "quantity": str(pos.quantity),
            "symbol": pos.ticker,
            "strike_price": f"{pos.strike:.2f}",
            "option_expire_date": pos.expiry,
            "instrument_type": "OPTION",
            "option_type": pos.option_type,
            "market": "US",
        }],
    }]

    try:
        tc.order_v2.place_option(WEBULL_ACCOUNT_ID, new_orders)
    except Exception as e:
        log.error("[PAPER] Failed to close %s: %s", pos.ticker, e)

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
        pos.client_order_id, pos.ticker, event_type,
        direction=pos.direction, option_type=pos.option_type,
        strike=pos.strike, expiry=pos.expiry,
        price=current_price, filled_price=pos.filled_price,
        pnl_pct=pos.pnl_pct, peak_premium=pos.peak_premium,
        trail_active=pos.trail_active, close_reason=reason,
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
