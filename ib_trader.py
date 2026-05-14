"""Interactive Brokers paper trading integration for ChainSignal.

Mirrors the public API of paper_trader.py but uses ib_insync to communicate
with TWS/IB Gateway.  Key advantage: reqMktData gives tick-by-tick option
quote callbacks so exit logic fires on every price change, not on a polling
interval.
"""
from __future__ import annotations

import os
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from paper_trader import (
    PaperPosition,
    SAME_COMPANY_TICKERS,
    _has_same_company_position,
    _build_occ_symbol,
    _progressive_stop,
    _send_paper_telegram,
    _reason_to_event_type,
)
from db import (
    log_paper_event as _log_paper_event_raw,
    load_paper_positions,
    save_paper_positions,
    load_closed_paper_positions,
    load_closed_paper_positions_since,
)


def log_paper_event(position_id, ticker, event_type, **kwargs):
    kwargs.setdefault("broker", "ib")
    _log_paper_event_raw(position_id, ticker, event_type, **kwargs)

load_dotenv()

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "7497"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

MAX_POSITION_COST = float(os.getenv("MAX_POSITION_COST", "3000"))

# ---------------------------------------------------------------------------
# Dedicated IB order thread — all ib_insync sync methods must run on the
# same thread/event-loop where the connection was created.  Python 3.14
# removed the implicit per-thread event loop, so calls from asyncio_1 or
# background Thread-xxx would fail with "no current event loop".  A single-
# worker executor pins every IB API call to one thread.
# ---------------------------------------------------------------------------

_ib_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ib_order")


def _on_ib_thread():
    return threading.current_thread().name.startswith("ib_order")


def _ib_run(fn, timeout=30):
    """Run fn() on the dedicated IB order thread. No-op if already there."""
    if _on_ib_thread():
        return fn()
    return _ib_executor.submit(fn).result(timeout=timeout)


# ---------------------------------------------------------------------------
# IB connection management (three client IDs: orders + quote streaming + order status)
# ---------------------------------------------------------------------------

_ib_order = None          # clientId for orders/queries (sync)
_ib_stream = None         # clientId+1 for streaming (async)
_ib_lock = threading.Lock()


def _get_ib():
    """Lazy connection to TWS for order placement and position queries."""
    global _ib_order
    with _ib_lock:
        if _ib_order is not None:
            try:
                if _ib_order.isConnected():
                    return _ib_order
            except Exception:
                pass
            _ib_order = None
        try:
            def _connect():
                global _ib_order
                try:
                    asyncio.get_event_loop()
                except RuntimeError:
                    asyncio.set_event_loop(asyncio.new_event_loop())
                from ib_insync import IB
                ib = IB()
                ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, readonly=False, timeout=45)
                _ib_order = ib
                log.info("[IB] Order connection established (clientId=%d)", IB_CLIENT_ID)
            _ib_run(_connect, timeout=60)
            return _ib_order
        except Exception as e:
            log.error("[IB] Failed to connect to TWS at %s:%d — %s", IB_HOST, IB_PORT, e)
            return None


# ---------------------------------------------------------------------------
# OCC ↔ IB Contract mapping
# ---------------------------------------------------------------------------

def _occ_to_ib_contract(occ_symbol: str, exchange: str = "SMART"):
    """Parse OCC symbol (e.g. AAPL260515C00200000) into an ib_insync Option."""
    from ib_insync import Option
    i = 0
    while i < len(occ_symbol) and not occ_symbol[i].isdigit():
        i += 1
    root = occ_symbol[:i]
    rest = occ_symbol[i:]
    date_str = rest[:6]        # YYMMDD
    right = rest[6]            # C or P
    strike = int(rest[7:15]) / 1000.0
    expiry = "20" + date_str   # YYYYMMDD
    return Option(root, expiry, strike, right, exchange)


def _contract_to_occ(contract) -> str:
    """Convert an ib_insync Contract back to an OCC symbol."""
    expiry = contract.lastTradeDateOrContractMonth  # YYYYMMDD
    yy = expiry[2:4]
    mm = expiry[4:6]
    dd = expiry[6:8]
    right = contract.right  # C or P
    strike_int = int(round(contract.strike * 1000))
    return f"{contract.symbol}{yy}{mm}{dd}{right}{strike_int:08d}"


# ---------------------------------------------------------------------------
# Position state (mirrors paper_trader.py module globals)
# ---------------------------------------------------------------------------

_positions: list[PaperPosition] = []
_positions_loaded: bool = False


def _load_positions():
    global _positions, _positions_loaded
    data = load_paper_positions(broker="ib")
    if data is None:
        log.warning("[IB] Supabase load failed — will retry next cycle")
        return
    known_fields = set(PaperPosition.__dataclass_fields__)
    _positions = [PaperPosition(**{k: v for k, v in row.items() if k in known_fields})
                  for row in data]
    _reconcile_with_ib()
    for pos in _positions:
        if pos.strategy_type != "LEAP" and pos.trail_activate_pct > 40.0:
            pos.trail_activate_pct = 40.0
        if pos.strategy_type != "LEAP" and pos.filled_price and pos.peak_premium:
            peak_pnl = (pos.peak_premium - pos.filled_price) / pos.filled_price * 100
            new_floor = _progressive_stop(peak_pnl)
            if new_floor is not None and new_floor > pos.premium_stop_pct:
                pos.premium_stop_pct = new_floor
    _positions_loaded = True


def _reconcile_with_ib():
    """Reconcile bot state with IB: close orphaned orders, reopen orphaned positions."""
    ib = _get_ib()
    if not ib:
        return
    try:
        def _get_ids():
            return {trade.order.orderId for trade in ib.trades()}
        ib_order_ids = _ib_run(_get_ids, timeout=15)
        now = datetime.now().isoformat()
        closed = 0
        for pos in _positions:
            if pos.status != "PENDING" or not pos.order_id:
                continue
            try:
                ib_id = int(pos.order_id.replace("IB-", ""))
            except (ValueError, AttributeError):
                continue
            if ib_id not in ib_order_ids:
                pos.status = "CLOSED"
                pos.close_reason = "ORDER_CANCELLED"
                pos.closed_at = now
                closed += 1
                log.info("[IB] Reconcile: %s (order %s) not in IB — marked CLOSED",
                         pos.ticker, pos.order_id)
        if closed:
            _save_positions()
            log.info("[IB] Reconcile: closed %d orphaned positions", closed)
    except Exception as e:
        log.error("[IB] Reconcile failed: %s", e)

    closing = [p for p in _positions if p.status == "CLOSING"]
    if closing:
        log.info("[IB] Reconcile: %d position(s) stuck in CLOSING — checking IB", len(closing))
        for pos in closing:
            residual = _check_ib_residual(pos)
            if residual > 0:
                pos.status = "FILLED"
                pos.quantity = residual
                log.info("[IB] Reconcile: %s still in IB (%d contracts) — reverted to FILLED",
                         pos.ticker, residual)
            elif residual == 0:
                pos.status = "CLOSED"
                pos.closed_at = datetime.now().isoformat()
                log.info("[IB] Reconcile: %s gone from IB — marked CLOSED", pos.ticker)
            else:
                pos.status = "FILLED"
                log.warning("[IB] Reconcile: %s — could not verify IB state, reverting to FILLED for safety",
                            pos.ticker)
        _save_positions()

    try:
        def _get_ib_positions():
            return {p.contract.symbol: p for p in ib.positions()
                    if hasattr(p.contract, 'right') and p.position > 0}
        ib_positions = _ib_run(_get_ib_positions, timeout=15)
        bot_open_tickers = {p.ticker for p in _positions if p.status in ("PENDING", "FILLED")}
        orphans = []
        for ticker, ib_pos in ib_positions.items():
            if ticker not in bot_open_tickers:
                occ = _contract_to_occ(ib_pos.contract)
                orphans.append((ticker, occ, ib_pos))
        if orphans:
            names = [f"{t} ({occ})" for t, occ, _ in orphans]
            log.warning("[IB] Reconcile: IB has %d position(s) bot doesn't track: %s",
                        len(orphans), ", ".join(names))
            _send_paper_telegram(
                f"⚠️ <b>ORPHANED IB POSITIONS</b>\n"
                f"IB has {len(orphans)} position(s) the bot doesn't track:\n"
                + "\n".join(f"  • {t} ({occ})" for t, occ, _ in orphans)
                + "\n\nThese have NO risk management. Close manually or investigate."
            )
    except Exception as e:
        log.error("[IB] Reconcile (orphan check) failed: %s", e)


def _save_positions():
    from dataclasses import asdict
    rows = [asdict(p) for p in _positions if p.order_id]
    save_paper_positions(rows)


def get_open_positions() -> list[PaperPosition]:
    if not _positions_loaded:
        _load_positions()
    return [p for p in _positions if p.status in ("PENDING", "FILLED")]


# ---------------------------------------------------------------------------
# IB helpers
# ---------------------------------------------------------------------------

_FALLBACK_EXCHANGES = ["SMART", "AMEX", "CBOE", "ISE", "NASDAQOM", "PHLX", "BOX"]


def _qualify(ib, contract):
    """Qualify a contract against IB's database, trying fallback exchanges.

    If the initial exchange fails, tries AMEX/CBOE/ISE/etc. This handles
    tickers like SNDK whose options don't resolve via SMART routing.
    """
    try:
        ok = _ib_run(lambda: len(ib.qualifyContracts(contract)) > 0, timeout=15)
        if ok:
            return True
    except Exception:
        pass

    original_exchange = contract.exchange
    for exchange in _FALLBACK_EXCHANGES:
        if exchange == original_exchange:
            continue
        try:
            contract.exchange = exchange
            ok = _ib_run(lambda: len(ib.qualifyContracts(contract)) > 0, timeout=15)
            if ok:
                log.info("[IB] Qualified %s via fallback exchange %s", contract.localSymbol or contract.symbol, exchange)
                return True
        except Exception:
            continue

    log.error("[IB] Failed to qualify contract %s on any exchange", contract)
    contract.exchange = original_exchange
    return False


def _get_ib_tickers(ib) -> set[str]:
    """Get tickers with open positions or pending orders on IB."""
    def _fetch():
        tickers = set()
        try:
            for pos in ib.positions():
                c = pos.contract
                if hasattr(c, 'symbol'):
                    tickers.add(c.symbol)
        except Exception as e:
            log.error("[IB] Failed to get positions for dedup: %s", e)
        try:
            for trade in ib.openTrades():
                c = trade.contract
                if hasattr(c, 'symbol'):
                    tickers.add(c.symbol)
        except Exception as e:
            log.error("[IB] Failed to get open orders for dedup: %s", e)
        return tickers
    try:
        return _ib_run(_fetch, timeout=15)
    except Exception:
        return set()


def _get_option_mid_price(occ_symbol: str) -> Optional[float]:
    """Get current mid price for an option via IB snapshot."""
    ib = _get_ib()
    if not ib:
        return None
    try:
        contract = _occ_to_ib_contract(occ_symbol)
        if not _qualify(ib, contract):
            log.warning("[IB] Could not qualify %s", occ_symbol)
            return None
        def _fetch():
            ticker = ib.reqMktData(contract, '', True, False)
            ib.sleep(2)
            bid = ticker.bid if ticker.bid and ticker.bid > 0 else 0
            ask = ticker.ask if ticker.ask and ticker.ask > 0 else 0
            ib.cancelMktData(contract)
            return bid, ask
        bid, ask = _ib_run(_fetch, timeout=15)
        if bid <= 0 and ask <= 0:
            return None
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2, 2)
        return round(max(bid, ask), 2)
    except Exception as e:
        log.error("[IB] Failed to get mid price for %s: %s", occ_symbol, e)
        return None


def _get_current_option_price(pos: PaperPosition) -> Optional[float]:
    return _get_option_mid_price(pos.occ_symbol)


def _get_spy_daily_return() -> Optional[float]:
    """Get SPY's intraday return via IB historical data."""
    ib = _get_ib()
    if not ib:
        return None
    try:
        def _fetch():
            from ib_insync import Stock
            spy = Stock('SPY', 'SMART', 'USD')
            ib.qualifyContracts(spy)
            bars = ib.reqHistoricalData(
                spy, '', '2 D', '1 day', 'TRADES', True, formatDate=1)
            if bars and len(bars) >= 2:
                prev_close = bars[-2].close
                current = bars[-1].close
                if prev_close > 0:
                    return round((current - prev_close) / prev_close * 100, 2)
            return None
        return _ib_run(_fetch, timeout=15)
    except Exception as e:
        log.debug("[IB] Failed to get SPY daily return: %s", e)
    return None


def _get_account_equity() -> Optional[float]:
    """Get net liquidation value from IB account."""
    ib = _get_ib()
    if not ib:
        return None
    try:
        def _fetch():
            for av in ib.accountValues():
                if av.tag == 'NetLiquidation' and av.currency == 'USD':
                    return float(av.value)
            return None
        return _ib_run(_fetch, timeout=15)
    except Exception as e:
        log.error("[IB] Failed to get account equity: %s", e)
    return None


# ---------------------------------------------------------------------------
# Position close
# ---------------------------------------------------------------------------

def _check_ib_residual(pos: PaperPosition) -> int:
    """Check if IB still holds contracts for this position. Returns qty or -1 on error."""
    ib = _get_ib()
    if not ib:
        return -1
    try:
        contract = _occ_to_ib_contract(pos.occ_symbol)
        if not _qualify(ib, contract):
            log.error("[IB] %s: Cannot verify residual — contract qualify failed", pos.ticker)
            return -1
        def _check():
            ib.sleep(0.5)
            for p in ib.positions():
                if p.contract.conId == contract.conId and p.position > 0:
                    return int(p.position)
            return 0
        return _ib_run(_check, timeout=15)
    except Exception as e:
        log.error("[IB] %s: Failed to check residual position: %s", pos.ticker, e)
        return -1


def _submit_ib_sell(pos: PaperPosition) -> Optional[float]:
    """Submit IB market sell and poll for fill. Returns fill price or None.

    BLOCKING (up to ~10s). Must NOT be called while holding _tick_lock.
    """
    ib = _get_ib()
    if not ib:
        log.error("[IB] No connection — cannot close %s", pos.ticker)
        return None
    try:
        contract = _occ_to_ib_contract(pos.occ_symbol)
        _qualify(ib, contract)
        def _sell():
            from ib_insync import MarketOrder
            order = MarketOrder('SELL', pos.quantity)
            trade = ib.placeOrder(contract, order)
            for _ in range(20):
                ib.sleep(0.5)
                if trade.orderStatus.status in ('Filled', 'Cancelled', 'Inactive'):
                    break
            if trade.orderStatus.status == 'Filled' and trade.orderStatus.avgFillPrice > 0:
                return trade.orderStatus.avgFillPrice
            return None
        return _ib_run(_sell, timeout=30)
    except Exception as e:
        err_str = str(e).lower()
        if "no position" in err_str or "not found" in err_str:
            log.warning("[IB] %s: Position already gone on IB", pos.ticker)
        else:
            log.error("[IB] Failed to close %s via IB: %s", pos.ticker, e)
    return None


def _mark_closed(pos: PaperPosition, exit_price: float, reason: str):
    """Mark position CLOSED with bookkeeping. No Telegram — call _notify_close after fill."""
    pos.status = "CLOSED"
    pos.close_reason = reason
    pos.close_price = exit_price
    pos.closed_at = datetime.now().isoformat()
    pos.es_overnight_pct = _get_es_overnight_pct()
    if pos.filled_price:
        pos.pnl_pct = round((exit_price - pos.filled_price) / pos.filled_price * 100, 2)


def _notify_close(pos: PaperPosition):
    """Log, persist event, and send Telegram for a closed position."""
    exit_price = pos.close_price
    reason = pos.close_reason or "unknown"

    es_str = f" ES={pos.es_overnight_pct:+.2f}%" if pos.es_overnight_pct is not None else ""
    log.info(
        "[IB] %s: CLOSED — %s | entry=$%.2f exit=$%.2f pnl=%s%%%s",
        pos.ticker, reason, pos.filled_price or 0, exit_price,
        f"{pos.pnl_pct:+.1f}" if pos.pnl_pct is not None else "?",
        es_str
    )

    event_type = _reason_to_event_type(reason)
    log_paper_event(
        pos.order_id, pos.ticker, event_type,
        direction=pos.direction, option_type=pos.option_type,
        strike=pos.strike, expiry=pos.expiry,
        price=exit_price, filled_price=pos.filled_price,
        pnl_pct=pos.pnl_pct, peak_premium=pos.peak_premium,
        trail_active=pos.trail_active, close_reason=reason,
    )

    pnl_emoji = "\U0001f7e2" if pos.pnl_pct and pos.pnl_pct > 0 else "\U0001f534"
    entry_str = f"${pos.filled_price:.2f}" if pos.filled_price is not None else "N/A"
    pnl_str = f"{pos.pnl_pct:+.1f}%" if pos.pnl_pct is not None else "?%"
    pnl_dollar = (exit_price - pos.filled_price) * (pos.quantity or 1) * 100 if pos.filled_price else 0
    cost_dollar = (pos.filled_price or 0) * (pos.quantity or 1) * 100
    exit_dollar = exit_price * (pos.quantity or 1) * 100
    peak_str = ""
    if pos.peak_premium and pos.filled_price:
        peak_pnl = (pos.peak_premium - pos.filled_price) / pos.filled_price * 100
        peak_dollar = (pos.peak_premium - pos.filled_price) * (pos.quantity or 1) * 100
        peak_str = f"\nPeak: ${pos.peak_premium:.2f} (+{peak_pnl:.1f}% / +${peak_dollar:,.0f})"
    es_line = ""
    if pos.es_overnight_pct is not None:
        es_line = f"\nES: {pos.es_overnight_pct:+.2f}%"
    _send_paper_telegram(
        f"{pnl_emoji} <b>IB PAPER TRADE CLOSED</b>\n"
        f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
        f"Entry: {entry_str} (${cost_dollar:,.0f}) → Exit: ${exit_price:.2f} (${exit_dollar:,.0f})\n"
        f"PnL: {pnl_str} / ${pnl_dollar:+,.0f}"
        f"{peak_str}{es_line}\n"
        f"Reason: {reason}"
    )


def _finalize_close(pos: PaperPosition, exit_price: float, reason: str):
    """Mark closed + notify. Used by poll path where fill price is already known."""
    _mark_closed(pos, exit_price, reason)
    _notify_close(pos)


def _close_position(pos: PaperPosition, current_price: float, reason: str):
    """Full blocking close: submit sell, verify fill, then record bookkeeping.

    Used by poll path and flow contradiction. Only marks CLOSED after
    confirming the sell filled. If sell fails and IB still holds contracts,
    leaves position as FILLED for retry on next cycle.
    """
    ib = _get_ib()
    if not ib:
        log.error("[IB] No connection — cannot close %s", pos.ticker)
        return
    fill_price = _submit_ib_sell(pos)
    if fill_price:
        log.info("[IB] %s: Sell filled at $%.2f (trigger quote was $%.2f, diff=$%.2f)",
                 pos.ticker, fill_price, current_price, fill_price - current_price)
        _finalize_close(pos, fill_price, reason)
    else:
        residual = _check_ib_residual(pos)
        if residual > 0:
            log.error("[IB] %s: SELL FAILED — %d contracts still in IB. "
                      "Will retry on next cycle.", pos.ticker, residual)
        elif residual == 0:
            _finalize_close(pos, current_price, reason)
        else:
            log.error("[IB] %s: SELL FAILED and could not verify IB state. "
                      "Leaving FILLED for safety.", pos.ticker)


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

def place_paper_trade(result) -> Optional[PaperPosition]:
    """Place a paper option order via IB based on a StrategyResult."""
    ib = _get_ib()
    if not ib:
        log.warning("[IB] No connection — skipping")
        return None

    tp = result.trade_plan
    if not tp or not tp.entry_price or not tp.suggested_strike:
        log.info("[IB] %s: No trade plan or no suggested contract — skipping", result.ticker)
        return None

    from strategies import Signal
    if result.direction == Signal.NEUTRAL:
        return None

    ib_tickers = _get_ib_tickers(ib)
    if _has_same_company_position(result.ticker, ib_tickers):
        log.info("[IB] %s: Already has open position on IB — skipping duplicate", result.ticker)
        return None

    if not _positions_loaded:
        _load_positions()
    open_swing_tickers = {p.ticker for p in _positions
                          if p.status in ("PENDING", "FILLED") and p.strategy_type == "SWING"}
    if _has_same_company_position(result.ticker, open_swing_tickers):
        log.info("[IB] %s: Already has open swing position in local tracking — skipping duplicate", result.ticker)
        return None

    MAX_POSITIONS = 8
    HIGH_ONLY_THRESHOLD = 3
    open_count = sum(1 for p in _positions if p.status in ("PENDING", "FILLED"))
    if open_count >= MAX_POSITIONS:
        log.info("[IB] %s: At position cap (%d/%d) — skipping", result.ticker, open_count, MAX_POSITIONS)
        return None
    if open_count >= HIGH_ONLY_THRESHOLD and result.conviction not in ("HIGH", "VERY_HIGH"):
        log.info("[IB] %s: %d positions open, only HIGH+ allowed (conviction=%s) — skipping",
                 result.ticker, open_count, result.conviction)
        return None

    is_bull = result.direction == Signal.BULLISH
    option_type = "CALL" if is_bull else "PUT"

    strike = tp.suggested_strike
    expiry = tp.suggested_expiry
    if not expiry:
        log.info("[IB] %s: No expiry in trade plan — skipping", result.ticker)
        return None

    occ_symbol = _build_occ_symbol(result.ticker, expiry, option_type, strike)

    mid_price = _get_option_mid_price(occ_symbol)
    if mid_price and mid_price > 0:
        limit_price = mid_price
    else:
        limit_price = round(max(tp.entry_price * (tp.target_pct / 100) / tp.option_leverage, 0.50), 2)
        log.warning("[IB] %s: Could not get market price for %s — using estimate $%.2f",
                    result.ticker, occ_symbol, limit_price)

    quantity = 1
    if MAX_POSITION_COST > 0:
        cost_per = limit_price * 100
        quantity = max(1, int(MAX_POSITION_COST / cost_per))
        if cost_per > MAX_POSITION_COST:
            log.warning("[IB] %s: Single contract $%.0f exceeds position cap $%.0f — SKIPPING",
                        result.ticker, cost_per, MAX_POSITION_COST)
            return None

    pos = PaperPosition(
        ticker=result.ticker,
        direction=result.direction.value,
        option_type=option_type,
        strike=strike,
        expiry=expiry[:10],
        quantity=quantity,
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
        market_return_pct=_get_spy_daily_return(),
        broker="ib",
    )

    try:
        contract = _occ_to_ib_contract(occ_symbol)
        if not _qualify(ib, contract):
            log.error("[IB] %s: Could not qualify contract %s — skipping", result.ticker, occ_symbol)
            pos.status = "REJECTED"
            pos.close_reason = "contract not found"
            with _tick_lock:
                if not _positions_loaded:
                    _load_positions()
                _positions.append(pos)
                _save_positions()
            return None

        def _place_buy():
            from ib_insync import LimitOrder
            order = LimitOrder('BUY', quantity, limit_price)
            order.tif = 'GTC'
            trade = ib.placeOrder(contract, order)
            return trade.order.orderId
        order_id = _ib_run(_place_buy, timeout=15)
        pos.order_id = f"IB-{order_id}"
        pos.status = "PENDING"
        log.info(
            "[IB] %s: Order placed — %s $%.0f exp %s @ $%.2f limit | %s | order_id=%s",
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
                "broker": "ib",
            },
        )
        _send_paper_telegram(
            f"\U0001f4c4 <b>IB PAPER TRADE OPENED</b>\n"
            f"{result.ticker} {option_type} ${strike:.0f} exp {expiry[:10]}\n"
            f"Limit: ${limit_price:.2f} | {result.conviction}\n"
            f"TP: {pos.premium_target_pct:+.0f}% | Stop: {pos.premium_stop_pct:+.0f}% | "
            f"Trail: {pos.trail_activate_pct:.0f}%/{pos.trail_stop_pct:.0f}%"
        )
    except Exception as e:
        log.error("[IB] %s: Order failed — %s", result.ticker, e)
        pos.status = "REJECTED"
        pos.close_reason = str(e)
        log_paper_event(
            occ_symbol, result.ticker, "ORDER_REJECTED",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10],
            close_reason=str(e),
        )

    with _tick_lock:
        if not _positions_loaded:
            _load_positions()
        _positions.append(pos)
        _save_positions()
    return pos if pos.status != "REJECTED" else None


def place_leap_trade(result, trade_plan) -> Optional[PaperPosition]:
    """Place a paper LEAP option order via IB."""
    ib = _get_ib()
    if not ib:
        log.warning("[IB-LEAP] No connection — skipping")
        return None

    if not trade_plan or not trade_plan.suggested_strike or not trade_plan.suggested_expiry:
        log.info("[IB-LEAP] %s: No strike/expiry in trade plan — skipping", result.ticker)
        return None

    from strategies import Signal
    is_bull = result.direction == Signal.BULLISH
    option_type = "CALL" if is_bull else "PUT"

    strike = trade_plan.suggested_strike
    expiry = trade_plan.suggested_expiry
    occ_symbol = _build_occ_symbol(result.ticker, expiry, option_type, strike)

    mid_price = _get_option_mid_price(occ_symbol)
    if not mid_price or mid_price <= 0:
        log.warning("[IB-LEAP] %s: Could not get option price for %s — skipping", result.ticker, occ_symbol)
        return None

    ib_tickers = _get_ib_tickers(ib)
    if _has_same_company_position(result.ticker, ib_tickers):
        log.info("[IB-LEAP] %s: Already has open position on IB — skipping duplicate", result.ticker)
        return None

    if not _positions_loaded:
        _load_positions()
    open_leap_tickers = {p.ticker for p in _positions
                         if p.status in ("PENDING", "FILLED") and p.strategy_type == "LEAP"}
    if _has_same_company_position(result.ticker, open_leap_tickers):
        log.info("[IB-LEAP] %s: Already has open LEAP position — skipping duplicate", result.ticker)
        return None

    MAX_POSITIONS = 8
    HIGH_ONLY_THRESHOLD = 3
    open_count = sum(1 for p in _positions if p.status in ("PENDING", "FILLED"))
    if open_count >= MAX_POSITIONS:
        log.info("[IB-LEAP] %s: At position cap (%d/%d) — skipping", result.ticker, open_count, MAX_POSITIONS)
        return None
    if open_count >= HIGH_ONLY_THRESHOLD and result.conviction not in ("HIGH", "VERY_HIGH"):
        log.info("[IB-LEAP] %s: %d positions open, only HIGH+ allowed (conviction=%s) — skipping",
                 result.ticker, open_count, result.conviction)
        return None

    leap_qty = 1
    if MAX_POSITION_COST > 0:
        cost_per = mid_price * 100
        if cost_per > MAX_POSITION_COST:
            log.warning("[IB-LEAP] %s: Single contract $%.0f exceeds position cap $%.0f — SKIPPING",
                        result.ticker, cost_per, MAX_POSITION_COST)
            return None
        leap_qty = max(1, int(MAX_POSITION_COST / cost_per))

    MAX_LEAP_ALLOCATION = 0.20
    try:
        equity = _get_account_equity()
        if equity is not None and equity > 0:
            leap_exposure = sum(
                (p.filled_price or p.limit_price) * 100 * p.quantity
                for p in _positions
                if p.status in ("PENDING", "FILLED") and p.strategy_type == "LEAP"
            )
            new_cost = mid_price * 100 * leap_qty
            if (leap_exposure + new_cost) / equity > MAX_LEAP_ALLOCATION:
                log.info(
                    "[IB-LEAP] %s: Would exceed 20%% LEAP allocation "
                    "(current=$%,.0f + new=$%,.0f vs limit=$%,.0f) — skipping",
                    result.ticker, leap_exposure, new_cost, equity * MAX_LEAP_ALLOCATION,
                )
                return None
    except Exception as e:
        log.error("[IB-LEAP] %s: Could not check allocation — skipping: %s", result.ticker, e)
        return None

    pos = PaperPosition(
        ticker=result.ticker,
        direction=result.direction.value,
        option_type=option_type,
        strike=strike,
        expiry=expiry[:10],
        quantity=leap_qty,
        limit_price=mid_price,
        occ_symbol=occ_symbol,
        premium_target_pct=trade_plan.premium_target_pct or 99999.0,
        premium_stop_pct=trade_plan.premium_stop_pct,
        trail_activate_pct=trade_plan.trail_activate_pct or 100.0,
        trail_stop_pct=trade_plan.trail_stop_pct,
        max_hold_days=trade_plan.max_hold_days,
        theta_kill_days=trade_plan.theta_kill_days,
        theta_kill_move_pct=trade_plan.theta_kill_move_pct,
        underlying_entry=trade_plan.entry_price or 0,
        opened_at=datetime.now().isoformat(),
        strategy_type="LEAP",
        market_return_pct=_get_spy_daily_return(),
        broker="ib",
    )

    try:
        contract = _occ_to_ib_contract(occ_symbol)
        if not _qualify(ib, contract):
            log.error("[IB-LEAP] %s: Could not qualify contract %s", result.ticker, occ_symbol)
            return None

        def _place_buy():
            from ib_insync import LimitOrder
            order = LimitOrder('BUY', leap_qty, mid_price)
            order.tif = 'GTC'
            trade = ib.placeOrder(contract, order)
            return trade.order.orderId
        order_id = _ib_run(_place_buy, timeout=15)
        pos.order_id = f"IB-{order_id}"
        pos.status = "PENDING"
        log.info(
            "[IB-LEAP] %s: Order placed — %s $%.0f exp %s @ $%.2f | %s",
            result.ticker, option_type, strike, expiry[:10], mid_price, occ_symbol,
        )
        log_paper_event(
            pos.order_id, result.ticker, "LEAP_ENTRY",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10], price=mid_price,
            metadata={"occ_symbol": occ_symbol, "strategy_type": "LEAP", "broker": "ib"},
        )
        _send_paper_telegram(
            f"\U0001f4c4 <b>IB LEAP TRADE OPENED</b>\n"
            f"{result.ticker} {option_type} ${strike:.0f} exp {expiry[:10]}\n"
            f"Limit: ${mid_price:.2f} | {result.conviction}"
        )
    except Exception as e:
        log.error("[IB-LEAP] %s: Order failed — %s", result.ticker, e)
        pos.status = "REJECTED"
        pos.close_reason = str(e)
        log_paper_event(
            occ_symbol, result.ticker, "LEAP_ORDER_REJECTED",
            direction=result.direction.value, option_type=option_type,
            strike=strike, expiry=expiry[:10], close_reason=str(e),
        )

    with _tick_lock:
        if not _positions_loaded:
            _load_positions()
        _positions.append(pos)
        _save_positions()
    return pos if pos.status != "REJECTED" else None


# ---------------------------------------------------------------------------
# Pending order checking
# ---------------------------------------------------------------------------

def _check_pending_order(pos: PaperPosition, now: datetime):
    """Check if a pending IB order has been filled or cancelled."""
    if not pos.order_id:
        return
    ib = _get_ib()
    if not ib:
        return

    ib_order_id = int(pos.order_id.replace("IB-", ""))

    try:
        trades_list = _ib_run(lambda: list(ib.trades()), timeout=15)
        for trade in trades_list:
            if trade.order.orderId == ib_order_id:
                status = trade.orderStatus.status.lower()

                if status == "filled":
                    avg_price = trade.orderStatus.avgFillPrice or pos.limit_price
                    pos.status = "FILLED"
                    pos.filled_price = avg_price
                    pos.filled_at = now.isoformat()
                    pos.peak_premium = avg_price

                    if pos.strategy_type == "LEAP":
                        try:
                            def _get_stock_price():
                                from ib_insync import Stock
                                stock = Stock(pos.ticker, 'SMART', 'USD')
                                ib.qualifyContracts(stock)
                                td = ib.reqMktData(stock, '', True, False)
                                ib.sleep(1)
                                b = td.bid if td.bid and td.bid > 0 else 0
                                a = td.ask if td.ask and td.ask > 0 else 0
                                ib.cancelMktData(stock)
                                return b, a
                            bid, ask = _ib_run(_get_stock_price, timeout=15)
                            if bid > 0 and ask > 0:
                                pos.underlying_entry = (bid + ask) / 2
                            elif bid > 0 or ask > 0:
                                pos.underlying_entry = max(bid, ask)
                        except Exception as e:
                            log.error("[IB] %s: Failed to update underlying on fill: %s", pos.ticker, e)

                    log.info("[IB] %s: FILLED @ $%.2f", pos.ticker, avg_price)
                    log_paper_event(
                        pos.order_id, pos.ticker, "FILL",
                        direction=pos.direction, option_type=pos.option_type,
                        strike=pos.strike, expiry=pos.expiry,
                        filled_price=avg_price, price=avg_price,
                    )
                    _send_paper_telegram(
                        f"✅ <b>IB PAPER FILL</b>\n"
                        f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
                        f"Filled @ ${avg_price:.2f}"
                    )
                    _subscribe_ib_quotes(pos.occ_symbol)

                elif status in ("cancelled", "canceled", "inactive"):
                    pos.status = "CLOSED"
                    pos.close_reason = "canceled"
                    pos.closed_at = now.isoformat()
                    log.info("[IB] %s: Order %s", pos.ticker, status.upper())
                    log_paper_event(
                        pos.order_id, pos.ticker, "ORDER_CANCELLED",
                        direction=pos.direction, option_type=pos.option_type,
                        strike=pos.strike, expiry=pos.expiry,
                        close_reason=status,
                    )

                elif status in ("presubmitted", "submitted"):
                    stale_hours = 48 if pos.strategy_type == "LEAP" else 24
                    opened = datetime.fromisoformat(pos.opened_at) if pos.opened_at else now
                    if (now - opened).total_seconds() > stale_hours * 3600:
                        try:
                            _ib_run(lambda t=trade: ib.cancelOrder(t.order), timeout=15)
                            pos.status = "CLOSED"
                            pos.close_reason = "canceled"
                            pos.closed_at = now.isoformat()
                            log.info("[IB] %s: Stale order canceled (%dh)", pos.ticker, stale_hours)
                        except Exception as e:
                            log.error("[IB] %s: Failed to cancel stale order: %s", pos.ticker, e)

                return
    except Exception as e:
        log.error("[IB] Error checking order for %s: %s", pos.ticker, e)


# ---------------------------------------------------------------------------
# Position management (poll-based safety net)
# ---------------------------------------------------------------------------

def check_and_manage_positions(ticker: str = None):
    """Check open positions, update fills, and manage exits.

    If ticker is provided, only check positions for that ticker (used by
    UW websocket event-driven monitoring for low-latency exit checks).
    """
    ib = _get_ib()
    if not ib:
        return

    if not _positions_loaded:
        _load_positions()

    open_positions = [p for p in _positions if p.status in ("PENDING", "FILLED")]
    if ticker:
        match_tickers = {ticker}
        alt = SAME_COMPANY_TICKERS.get(ticker)
        if alt:
            match_tickers.add(alt)
        open_positions = [p for p in open_positions if p.ticker in match_tickers]
    if not open_positions:
        return

    now = datetime.now()

    pending = [p for p in open_positions if p.status == "PENDING"]
    filled = [p for p in open_positions
              if p.status == "FILLED" and p.filled_price]

    prices = {}
    for pos in filled:
        try:
            prices[pos.occ_symbol] = _get_current_option_price(pos)
        except Exception as e:
            log.error("[IB] Error getting price for %s: %s", pos.ticker, e)

    with _tick_lock:
        for pos in pending:
            if pos.status != "PENDING":
                continue
            try:
                _check_pending_order(pos, now)
            except Exception as e:
                log.error("[IB] Error checking pending %s: %s", pos.ticker, e)

        for pos in filled:
            try:
                if pos.status != "FILLED":
                    continue
                current_price = prices.get(pos.occ_symbol)
                if current_price is None:
                    log.warning("[IB] %s: Price unavailable — position UNMANAGED this cycle (%s)",
                                pos.ticker, pos.occ_symbol)
                    continue
                _run_exit_logic(pos, current_price, now)
            except Exception as e:
                log.error("[IB] Error managing %s: %s", pos.ticker, e)

        _save_positions()

    stuck_closing = [p for p in _positions if p.status == "CLOSING"]
    for pos in stuck_closing:
        log.warning("[IB] %s: Position stuck in CLOSING — will be recovered on next restart",
                    pos.ticker)


def _run_exit_logic(pos: PaperPosition, current_price: float, now: datetime,
                    from_stream: bool = False):
    """Core exit logic shared by poll and streaming paths.

    Returns close reason string (truthy) if position should exit, None otherwise.
    When from_stream=False (poll path): also executes the blocking close.
    When from_stream=True (streaming): caller handles close outside the lock.
    LEAP underlying check is skipped in stream path (poll handles it).
    """
    entry = pos.filled_price
    pnl_pct = (current_price - entry) / entry * 100

    if pos.peak_premium is None or current_price > pos.peak_premium:
        pos.peak_premium = current_price
    if pos.trough_premium is None or current_price < pos.trough_premium:
        pos.trough_premium = current_price

    if pos.strategy_type != "LEAP" and pos.peak_premium:
        peak_pnl = (pos.peak_premium - entry) / entry * 100
        new_floor = _progressive_stop(peak_pnl)
        if new_floor is not None and new_floor > pos.premium_stop_pct:
            old_stop = pos.premium_stop_pct
            pos.premium_stop_pct = new_floor
            log.info("[IB] %s: Stop ratcheted %.1f%% → %.1f%% (peak +%.1f%%)",
                     pos.ticker, old_stop, new_floor, peak_pnl)

    if pnl_pct <= pos.premium_stop_pct:
        reason = (f"progressive stop ({pnl_pct:+.1f}%, floor was {pos.premium_stop_pct:+.1f}%)"
                  if pos.premium_stop_pct > 0
                  else f"breakeven stop ({pnl_pct:+.1f}%)" if pos.premium_stop_pct == 0.0
                  else f"hard stop ({pnl_pct:+.1f}%)")
        if not from_stream:
            _close_position(pos, current_price, reason)
        return reason

    if not from_stream and pos.strategy_type == "LEAP" and pos.underlying_entry > 0:
        ib = _get_ib()
        if ib:
            try:
                def _get_stock():
                    from ib_insync import Stock
                    stock = Stock(pos.ticker, 'SMART', 'USD')
                    ib.qualifyContracts(stock)
                    td = ib.reqMktData(stock, '', True, False)
                    ib.sleep(1)
                    b = td.bid if td.bid and td.bid > 0 else 0
                    a = td.ask if td.ask and td.ask > 0 else 0
                    ib.cancelMktData(stock)
                    return b, a
                bid, ask = _ib_run(_get_stock, timeout=15)
                if bid > 0 or ask > 0:
                    stock_price = (bid + ask) / 2 if (bid > 0 and ask > 0) else max(bid, ask)
                    stock_move_pct = (stock_price - pos.underlying_entry) / pos.underlying_entry * 100
                    is_bull = pos.direction == "BULLISH"
                    if (is_bull and stock_move_pct <= -25) or (not is_bull and stock_move_pct >= 25):
                        reason = (f"underlying stop ({pos.ticker} {stock_move_pct:+.1f}% "
                                  f"from ${pos.underlying_entry:.2f})")
                        _close_position(pos, current_price, reason)
                        return reason
            except Exception as e:
                log.error("[IB] LEAP underlying check failed for %s: %s", pos.ticker, e)

    if pnl_pct >= pos.premium_target_pct:
        reason = f"profit target ({pnl_pct:+.1f}%)"
        if not from_stream:
            _close_position(pos, current_price, reason)
        return reason

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
            reason = (f"trailing stop (peak=${pos.peak_premium:.2f}, "
                      f"drawdown={drawdown_from_peak:.1f}%)")
            if not from_stream:
                _close_position(pos, current_price, reason)
            return reason

    hold_start = pos.filled_at or pos.opened_at
    if hold_start:
        days_held = (now - datetime.fromisoformat(hold_start)).days
        if days_held >= pos.theta_kill_days and abs(pnl_pct) < pos.theta_kill_move_pct:
            reason = f"theta kill (day {days_held}, move={pnl_pct:+.1f}%)"
            if not from_stream:
                _close_position(pos, current_price, reason)
            return reason
        if days_held >= pos.max_hold_days:
            reason = f"max hold ({days_held}d)"
            if not from_stream:
                _close_position(pos, current_price, reason)
            return reason

    return None


# ---------------------------------------------------------------------------
# Flow contradiction
# ---------------------------------------------------------------------------

def check_flow_contradiction(result, min_conviction: str = "MEDIUM"):
    """Close/cancel held positions when re-analysis contradicts the original thesis."""
    if not _positions_loaded:
        _load_positions()

    from strategies import Signal

    match_tickers = {result.ticker}
    alt = SAME_COMPANY_TICKERS.get(result.ticker)
    if alt:
        match_tickers.add(alt)
    held = [p for p in _positions
            if p.ticker in match_tickers and p.status in ("PENDING", "FILLED")]
    if not held:
        return

    conviction_order = ["NONE", "LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
    new_dir = result.direction
    new_conv_idx = (conviction_order.index(result.conviction)
                    if result.conviction in conviction_order else 0)
    min_conv_idx = (conviction_order.index(min_conviction)
                    if min_conviction in conviction_order else 2)

    actions = []
    for pos in held:
        pos_is_bull = pos.direction == "BULLISH"

        direction_flipped = (
            (pos_is_bull and new_dir == Signal.BEARISH)
            or (not pos_is_bull and new_dir == Signal.BULLISH)
        )
        conviction_collapsed = new_conv_idx < min_conv_idx

        if not direction_flipped and not conviction_collapsed:
            log.info(
                "[IB] %s: Re-analysis still supports %s position "
                "(new direction=%s, conviction=%s, score=%.0f)",
                pos.ticker, pos.direction, new_dir.value,
                result.conviction, result.composite_score,
            )
            continue

        if direction_flipped:
            reason = (f"flow contradiction: {pos.direction} → {new_dir.value} "
                      f"(score={result.composite_score:.0f}, conviction={result.conviction})")
        else:
            reason = (f"conviction collapsed: {result.conviction} "
                      f"(score={result.composite_score:.0f}, direction={new_dir.value})")

        if pos.status == "PENDING":
            actions.append(("cancel", pos, reason, None))
        else:
            current_price = _get_current_option_price(pos)
            if current_price is None:
                log.warning("[IB] %s: Cannot get price for contradiction close", pos.ticker)
                continue
            actions.append(("close", pos, reason, current_price))

    if not actions:
        return

    # Two-phase close: mark positions under lock (fast), then execute
    # blocking broker calls outside the lock to avoid blocking streaming ticks.
    cancel_queue = []
    close_queue = []

    with _tick_lock:
        for action, pos, reason, price in actions:
            if pos.status not in ("PENDING", "FILLED"):
                continue

            if action == "cancel":
                cancel_queue.append((pos, reason))

            elif action == "close":
                pos.status = "CLOSING"
                close_queue.append((pos, price, reason))

        if cancel_queue or close_queue:
            _save_positions()

    for pos, reason in cancel_queue:
        ib = _get_ib()
        if not ib:
            continue
        try:
            ib_order_id = int(pos.order_id.replace("IB-", ""))
            def _cancel(oid=ib_order_id):
                for trade in ib.openTrades():
                    if trade.order.orderId == oid:
                        ib.cancelOrder(trade.order)
                        ib.sleep(1)
                        return True
                return False
            _ib_run(_cancel, timeout=15)

            # Check if IB filled any contracts before the cancel took effect.
            filled_qty = 0
            fill_price = None
            qualify_ok = False
            try:
                contract = _occ_to_ib_contract(pos.occ_symbol)
                qualify_ok = _qualify(ib, contract)
                if qualify_ok:
                    def _check_pos():
                        ib.sleep(0.5)
                        for p in ib.positions():
                            if p.contract.conId == contract.conId and p.position > 0:
                                return int(p.position), p.avgCost / 100
                        return 0, None
                    filled_qty, fill_price = _ib_run(_check_pos, timeout=15)
                else:
                    log.error("[IB] %s: Cannot check cancel race — contract qualify failed", pos.ticker)
            except Exception as e:
                log.error("[IB] %s: Failed to check residual position after cancel: %s",
                          pos.ticker, e)

            if not qualify_ok:
                log.error("[IB] %s: Cannot determine cancel race outcome — leaving PENDING for manual review",
                          pos.ticker)
            elif filled_qty > 0:
                log.warning(
                    "[IB] %s: Cancel race — %d contracts filled at $%.2f before cancel. "
                    "Selling residual position.",
                    pos.ticker, filled_qty, fill_price or 0)
                with _tick_lock:
                    pos.status = "FILLED"
                    pos.filled_price = fill_price
                    pos.filled_at = datetime.now().isoformat()
                    pos.quantity = filled_qty
                    _save_positions()
                sell_fill = _submit_ib_sell(pos)
                if sell_fill:
                    with _tick_lock:
                        _mark_closed(pos, sell_fill, reason + " (cancel race — filled before cancel)")
                        _save_positions()
                    _notify_close(pos)
                else:
                    residual = _check_ib_residual(pos)
                    if residual > 0:
                        log.error("[IB] %s: CANCEL RACE SELL FAILED — %d contracts still in IB. "
                                  "Reverting to FILLED.", pos.ticker, residual)
                        with _tick_lock:
                            pos.status = "FILLED"
                            pos.quantity = residual
                            _save_positions()
                    elif residual == 0:
                        with _tick_lock:
                            _mark_closed(pos, fill_price or 0, reason + " (cancel race — filled before cancel)")
                            _save_positions()
                        _notify_close(pos)
                    else:
                        log.error("[IB] %s: CANCEL RACE SELL FAILED and could not verify IB state. "
                                  "Leaving FILLED for safety.", pos.ticker)
            else:
                with _tick_lock:
                    pos.status = "CLOSED"
                    pos.close_reason = reason
                    pos.closed_at = datetime.now().isoformat()
                    _save_positions()
                log.info("[IB] %s: Cancelled PENDING order — %s", pos.ticker, reason)
                log_paper_event(
                    pos.order_id, pos.ticker, "FLOW_CONTRADICTION",
                    direction=pos.direction, option_type=pos.option_type,
                    strike=pos.strike, expiry=pos.expiry,
                    close_reason=reason,
                    metadata={"new_direction": new_dir.value,
                              "new_conviction": result.conviction,
                              "new_score": result.composite_score},
                )
                _send_paper_telegram(
                    f"⚠️ <b>FLOW CONTRADICTION — ORDER CANCELLED</b>\n"
                    f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
                    f"{reason}"
                )
        except Exception as e:
            log.error("[IB] %s: Failed to cancel on contradiction: %s", pos.ticker, e)

    for pos, price, reason in close_queue:
        fill_price = _submit_ib_sell(pos)
        if fill_price:
            with _tick_lock:
                _mark_closed(pos, fill_price, reason)
                _save_positions()
            _notify_close(pos)
        else:
            residual = _check_ib_residual(pos)
            if residual > 0:
                log.error("[IB] %s: SELL FAILED — %d contracts still in IB. "
                          "Reverting to FILLED.", pos.ticker, residual)
                with _tick_lock:
                    pos.status = "FILLED"
                    pos.quantity = residual
                    _save_positions()
            elif residual == 0:
                with _tick_lock:
                    _mark_closed(pos, price, reason)
                    _save_positions()
                _notify_close(pos)
            else:
                log.error("[IB] %s: SELL FAILED and could not verify IB state. "
                          "Reverting to FILLED for safety.", pos.ticker)
                with _tick_lock:
                    pos.status = "FILLED"
                    _save_positions()


# ---------------------------------------------------------------------------
# Real-time streaming: reqMktData tick-by-tick exits (THE KEY FEATURE)
# ---------------------------------------------------------------------------

_subscribed_contracts: dict[str, object] = {}  # occ_symbol -> qualified Contract
_stream_ib = None
_tick_lock = threading.Lock()

_stream_loop = None  # asyncio event loop for the streaming connection

# ---------------------------------------------------------------------------
# ES futures streaming state
# ---------------------------------------------------------------------------
_es_contract = None           # qualified ContFuture for ES
_es_prev_close: float = 0.0   # yesterday's session close (4 PM ET)
_es_day_open: float = 0.0     # today's RTH open (9:30 AM ET)
_es_current: float = 0.0      # latest ES price from stream
_es_flagged_for_close = False  # overnight -1% breach — close at open


async def _subscribe_es_stream():
    """Subscribe to ES continuous futures for gap risk monitoring."""
    global _es_contract, _es_prev_close, _es_day_open, _es_current, _es_flagged_for_close
    if not _stream_ib:
        return
    try:
        from ib_insync import ContFuture
        es = ContFuture('ES', 'CME')
        qualified = await _stream_ib.qualifyContractsAsync(es)
        if not qualified:
            log.warning("[IB-ES] Could not qualify ES ContFuture")
            _send_paper_telegram(
                "⚠️ <b>ES GAP PROTECTION UNAVAILABLE</b>\n"
                "Could not qualify ES futures contract.\n"
                "Overnight gap alerts will NOT fire today."
            )
            return

        bars = await _stream_ib.reqHistoricalDataAsync(
            es, '', '5 D', '1 day', 'TRADES', True, 1
        )
        if bars and len(bars) >= 1:
            _es_prev_close = bars[-1].close
            log.info("[IB-ES] Previous session close: %.2f", _es_prev_close)
        else:
            log.warning("[IB-ES] No historical bars for ES reference price")
            _send_paper_telegram(
                "⚠️ <b>ES GAP PROTECTION UNAVAILABLE</b>\n"
                "Could not load ES previous close from IB.\n"
                "Overnight gap alerts will NOT fire today."
            )

        _es_contract = es
        _es_flagged_for_close = False
        _stream_ib.reqMktData(es, '', False, False)
        log.info("[IB-ES] Subscribed to ES futures stream")
    except Exception as e:
        log.error("[IB-ES] Failed to subscribe ES stream: %s", e)
        _send_paper_telegram(
            f"⚠️ <b>ES GAP PROTECTION UNAVAILABLE</b>\n"
            f"ES stream subscription failed: {e}\n"
            f"Overnight gap alerts will NOT fire today."
        )


def _get_es_overnight_pct() -> float | None:
    """Current ES return vs previous session close. Thread-safe."""
    if _es_prev_close > 0 and _es_current > 0:
        return round((_es_current - _es_prev_close) / _es_prev_close * 100, 3)
    return None


async def _subscribe_ib_quotes_async(occ_symbol: str):
    """Subscribe to real-time quotes — must run on the streaming event loop."""
    if not _stream_ib or occ_symbol in _subscribed_contracts:
        return
    try:
        contract = _occ_to_ib_contract(occ_symbol)
        qualified = await _stream_ib.qualifyContractsAsync(contract)
        if not qualified:
            for exchange in _FALLBACK_EXCHANGES:
                if exchange == "SMART":
                    continue
                contract = _occ_to_ib_contract(occ_symbol, exchange=exchange)
                qualified = await _stream_ib.qualifyContractsAsync(contract)
                if qualified:
                    log.info("[IB-STREAM] Qualified %s via fallback exchange %s", occ_symbol, exchange)
                    break
        if not qualified:
            log.warning("[IB-STREAM] Could not qualify %s on any exchange", occ_symbol)
            return
        _stream_ib.reqMktData(contract, '', False, False)
        _subscribed_contracts[occ_symbol] = contract
        log.info("[IB-STREAM] Subscribed to real-time quotes: %s", occ_symbol)
    except Exception as e:
        log.error("[IB-STREAM] Failed to subscribe %s: %s", occ_symbol, e)


async def _unsubscribe_ib_quotes_async(occ_symbol: str):
    """Unsubscribe from closed position quotes — must run on the streaming event loop."""
    contract = _subscribed_contracts.pop(occ_symbol, None)
    if contract and _stream_ib:
        try:
            _stream_ib.cancelMktData(contract)
            log.info("[IB-STREAM] Unsubscribed from quotes: %s", occ_symbol)
        except Exception as e:
            log.error("[IB-STREAM] Failed to unsubscribe %s: %s", occ_symbol, e)


def _subscribe_ib_quotes(occ_symbol: str):
    """Thread-safe wrapper — schedules subscribe on the streaming event loop."""
    if not _stream_loop or not _stream_ib:
        return
    asyncio.run_coroutine_threadsafe(_subscribe_ib_quotes_async(occ_symbol), _stream_loop)


def _unsubscribe_ib_quotes(occ_symbol: str):
    """Thread-safe wrapper — schedules unsubscribe on the streaming event loop."""
    if not _stream_loop or not _stream_ib:
        return
    asyncio.run_coroutine_threadsafe(_unsubscribe_ib_quotes_async(occ_symbol), _stream_loop)


def _on_pending_tickers(tickers):
    """Fired on every bid/ask change for subscribed options AND ES futures.

    ES ticks update the _es_current price and log threshold breaches.
    Option ticks snapshot the position list under lock for thread-safe lookup,
    then offload exit logic to a background thread.
    """
    global _es_current, _es_day_open, _es_flagged_for_close
    now = datetime.now()

    for t in tickers:
        if t.contract is not None and t.contract == _es_contract:
            last = t.last if t.last and t.last > 0 else 0
            bid = t.bid if t.bid and t.bid > 0 else 0
            ask = t.ask if t.ask and t.ask > 0 else 0
            price = last if last > 0 else (
                round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else max(bid, ask))
            if price > 0:
                _es_current = price
                if _es_day_open == 0.0:
                    _es_day_open = price
                if _es_prev_close > 0:
                    pct = (_es_current - _es_prev_close) / _es_prev_close * 100
                    if pct <= -1.0 and not _es_flagged_for_close:
                        _es_flagged_for_close = True
                        log.warning("[IB-ES] ES breached -1.0%%: %.2f%% (prev close=%.2f, current=%.2f)",
                                    pct, _es_prev_close, _es_current)
                        _send_paper_telegram(
                            f"\U0001f534 <b>ES GAP ALERT</b> {pct:+.2f}%\n"
                            f"ES prev close: {_es_prev_close:.2f} → Current: {_es_current:.2f}\n"
                            f"All bullish swing positions flagged"
                        )

    with _tick_lock:
        pos_by_occ = {p.occ_symbol: p for p in _positions
                      if p.status == "FILLED" and p.filled_price}

    updates = []
    for t in tickers:
        if t.contract is None or t.contract == _es_contract:
            continue
        occ = _contract_to_occ(t.contract)
        pos = pos_by_occ.get(occ)
        if not pos:
            continue

        bid = t.bid if t.bid and t.bid > 0 else 0
        ask = t.ask if t.ask and t.ask > 0 else 0
        if bid <= 0 and ask <= 0:
            continue
        current_price = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else max(bid, ask)
        updates.append((pos, occ, current_price))

    if updates:
        threading.Thread(
            target=_process_tick_updates,
            args=(updates, now),
            daemon=True,
        ).start()


def _process_tick_updates(updates: list, now: datetime):
    """Run exit logic for a batch of tick updates in a background thread.

    Two-phase close: mark CLOSING under lock to prevent re-trigger,
    then submit IB sell orders outside the lock. Only mark CLOSED after
    the sell fills — if the sell fails, revert to FILLED.
    """
    close_queue = []
    with _tick_lock:
        for pos, occ, current_price in updates:
            if pos.status != "FILLED":
                continue
            try:
                reason = _run_exit_logic(pos, current_price, now, from_stream=True)
                if reason:
                    pos.status = "CLOSING"
                    close_queue.append((pos, occ, current_price, reason))
            except Exception as e:
                log.error("[IB-STREAM] Tick exit error for %s: %s", pos.ticker, e)
        if close_queue:
            _save_positions()

    for pos, occ, trigger_price, reason in close_queue:
        fill_price = _submit_ib_sell(pos)

        if fill_price:
            _unsubscribe_ib_quotes(occ)
            exit_price = fill_price
            if abs(fill_price - trigger_price) > 0.005:
                log.info("[IB] %s: Fill $%.2f vs trigger $%.2f (diff=$%.2f)",
                         pos.ticker, fill_price, trigger_price,
                         fill_price - trigger_price)
            with _tick_lock:
                _mark_closed(pos, exit_price, reason)
                _save_positions()
            _notify_close(pos)
        else:
            residual = _check_ib_residual(pos)
            if residual > 0:
                log.error(
                    "[IB] %s: SELL FAILED — %d contracts still in IB. "
                    "Reverting to FILLED for retry.",
                    pos.ticker, residual)
                with _tick_lock:
                    pos.status = "FILLED"
                    pos.quantity = residual
                    _save_positions()
            elif residual == 0:
                _unsubscribe_ib_quotes(occ)
                with _tick_lock:
                    _mark_closed(pos, trigger_price, reason)
                    _save_positions()
                _notify_close(pos)
            else:
                log.error(
                    "[IB] %s: SELL FAILED and could not verify IB state. "
                    "Reverting to FILLED for safety.", pos.ticker)
                with _tick_lock:
                    pos.status = "FILLED"
                    _save_positions()


async def start_option_stream():
    """Start IB streaming quotes for all filled positions via reqMktData.

    Uses a dedicated IB connection (clientId+1) so streaming doesn't
    compete with order placement on the main connection.
    """
    global _stream_ib, _stream_loop
    if _stream_ib:
        try:
            _stream_ib.disconnect()
        except Exception:
            pass
        _stream_ib = None
        _stream_loop = None

    from ib_insync import IB
    ib = IB()

    try:
        await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID + 1, readonly=True, timeout=45)
        _stream_ib = ib
        _stream_loop = asyncio.get_event_loop()
        log.info("[IB-STREAM] \U0001f4ca Streaming connection established (clientId=%d)", IB_CLIENT_ID + 1)
    except Exception as e:
        log.error("[IB-STREAM] Failed to connect: %s", e)
        raise

    _subscribed_contracts.clear()

    if not _positions_loaded:
        _load_positions()

    filled_symbols = [p.occ_symbol for p in _positions if p.status == "FILLED" and p.filled_price]
    for sym in filled_symbols:
        await _subscribe_ib_quotes_async(sym)

    await _subscribe_es_stream()

    ib.pendingTickersEvent += _on_pending_tickers
    log.info("[IB-STREAM] Subscribed to %d option quote streams + ES futures", len(filled_symbols))

    while True:
        await asyncio.sleep(1)
        if not ib.isConnected():
            _stream_ib = None
            _stream_loop = None
            raise ConnectionError("IB streaming connection lost")


# ---------------------------------------------------------------------------
# Real-time fill detection
# ---------------------------------------------------------------------------

def _update_leap_underlying_on_fill(pos: PaperPosition):
    """Fetch current stock price and update underlying_entry for a LEAP fill.

    Runs in a background thread so blocking IB calls don't hold _tick_lock.
    """
    ib = _get_ib()
    if not ib:
        return
    try:
        def _fetch():
            from ib_insync import Stock
            stock = Stock(pos.ticker, 'SMART', 'USD')
            ib.qualifyContracts(stock)
            td = ib.reqMktData(stock, '', True, False)
            ib.sleep(1)
            b = td.bid if td.bid and td.bid > 0 else 0
            a = td.ask if td.ask and td.ask > 0 else 0
            ib.cancelMktData(stock)
            return b, a
        bid, ask = _ib_run(_fetch, timeout=15)
        if bid > 0 and ask > 0:
            new_underlying = (bid + ask) / 2
        elif bid > 0 or ask > 0:
            new_underlying = max(bid, ask)
        else:
            return
        with _tick_lock:
            pos.underlying_entry = new_underlying
            _save_positions()
        log.info("[IB] %s: Updated LEAP underlying_entry to $%.2f on fill",
                 pos.ticker, new_underlying)
    except Exception as e:
        log.error("[IB] %s: Failed to update underlying on fill: %s", pos.ticker, e)


def _on_order_status(trade):
    """Handle IB order status changes — dispatches to background thread."""
    order_id_str = f"IB-{trade.order.orderId}"
    status = trade.orderStatus.status.lower()
    avg_price = trade.orderStatus.avgFillPrice
    threading.Thread(
        target=_process_order_status,
        args=(order_id_str, status, avg_price),
        daemon=True,
    ).start()


def _process_order_status(order_id_str: str, status: str, avg_price: float):
    """Process order status update in a background thread."""
    with _tick_lock:
        if not _positions_loaded:
            _load_positions()

        pos = next((p for p in _positions if p.order_id == order_id_str), None)
        if not pos:
            return

        now = datetime.now()

        if status == "filled" and pos.status == "PENDING":
            fill_price = avg_price or pos.limit_price
            pos.status = "FILLED"
            pos.filled_price = fill_price
            pos.filled_at = now.isoformat()
            pos.peak_premium = fill_price
            log.info("[IB] %s: FILLED @ $%.2f (real-time)", pos.ticker, fill_price)
            log_paper_event(
                pos.order_id, pos.ticker, "FILL",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                filled_price=fill_price, price=fill_price,
            )
            _send_paper_telegram(
                f"✅ <b>IB PAPER FILL</b>\n"
                f"{pos.ticker} {pos.option_type} ${pos.strike:.0f} exp {pos.expiry}\n"
                f"Filled @ ${fill_price:.2f}"
            )
            _subscribe_ib_quotes(pos.occ_symbol)
            _save_positions()
            if pos.strategy_type == "LEAP":
                threading.Thread(
                    target=_update_leap_underlying_on_fill,
                    args=(pos,),
                    daemon=True,
                ).start()

        elif status in ("cancelled", "canceled", "inactive") and pos.status == "PENDING":
            pos.status = "CLOSED"
            pos.close_reason = status
            pos.closed_at = now.isoformat()
            log.info("[IB] %s: Order %s (real-time)", pos.ticker, status.upper())
            log_paper_event(
                pos.order_id, pos.ticker, f"ORDER_{status.upper()}",
                direction=pos.direction, option_type=pos.option_type,
                strike=pos.strike, expiry=pos.expiry,
                close_reason=status,
            )
            _save_positions()


_trade_stream_ib = None


async def start_trade_stream():
    """Monitor IB order status changes for real-time fill detection.

    Uses a dedicated IB connection (clientId+2) established in the async
    context so ib_insync integrates with the asyncio event loop and
    orderStatusEvent fires immediately — not only during ib.sleep() calls.
    """
    global _trade_stream_ib
    if _trade_stream_ib:
        try:
            _trade_stream_ib.disconnect()
        except Exception:
            pass
        _trade_stream_ib = None

    from ib_insync import IB
    ib = IB()
    try:
        await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID + 2, readonly=True, timeout=45)
        _trade_stream_ib = ib
        log.info("[IB] \U0001f534 Order status monitoring active (clientId=%d)", IB_CLIENT_ID + 2)
    except Exception as e:
        raise ConnectionError(f"IB trade stream connection failed: {e}")

    await ib.reqAllOpenOrdersAsync()
    ib.orderStatusEvent += lambda trade: _on_order_status(trade)

    while True:
        await asyncio.sleep(1)
        if not ib.isConnected():
            _trade_stream_ib = None
            raise ConnectionError("IB trade stream connection lost")


# ---------------------------------------------------------------------------
# EOD report
# ---------------------------------------------------------------------------

def _reconcile_eod():
    """Pre-report check: verify IB positions match bot state."""
    ib = _get_ib()
    if not ib:
        return
    try:
        def _get_ib_pos():
            return {(p.contract.symbol, p.contract.strike, p.contract.right): int(p.position)
                    for p in ib.positions()
                    if hasattr(p.contract, 'right') and p.position > 0}
        ib_pos = _ib_run(_get_ib_pos, timeout=15)
    except Exception as e:
        log.error("[IB] EOD reconcile failed: %s", e)
        return

    bot_open = {p.ticker: p for p in _positions if p.status in ("FILLED", "CLOSING")}
    mismatches = []

    for (sym, strike, right), qty in ib_pos.items():
        if sym not in bot_open:
            mismatches.append(f"  ⚠️ {sym} ${strike:.0f} {'CALL' if right == 'C' else 'PUT'}: "
                              f"IB has {qty} contracts, bot has NONE")

    for ticker, pos in bot_open.items():
        key = (pos.ticker, pos.strike, 'C' if pos.option_type == 'CALL' else 'P')
        ib_qty = ib_pos.get(key, 0)
        if ib_qty != (pos.quantity or 0):
            mismatches.append(f"  ⚠️ {ticker}: bot says {pos.quantity} contracts, IB has {ib_qty}")

    if mismatches:
        log.warning("[IB] EOD reconcile found %d mismatch(es)", len(mismatches))
        _send_paper_telegram(
            "⚠️ <b>POSITION MISMATCH DETECTED</b>\n"
            "Bot state differs from IB:\n"
            + "\n".join(mismatches)
        )


def send_eod_report():
    """Send end-of-day P&L report via Telegram.

    Reconciles with IB before reporting: checks for positions the bot
    thinks are closed but IB still holds (orphans).
    """
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    now_et = datetime.now(et)
    today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    if not _positions_loaded:
        _load_positions()

    _reconcile_eod()

    closed_today = load_closed_paper_positions_since(today_start, broker="ib")
    closed_today = [c for c in closed_today
                    if c.get("filled_price") is not None
                    and c.get("pnl_pct") is not None]

    realized_lines = []
    total_realized = 0.0
    for c in closed_today:
        fp = c.get("filled_price") or 0
        pnl_pct = c.get("pnl_pct") or 0
        qty = c.get("quantity") or 1
        pnl_dollar = fp * 100 * qty * pnl_pct / 100
        total_realized += pnl_dollar
        emoji = "\U0001f7e2" if pnl_pct > 0 else "\U0001f534"
        reason = (c.get("close_reason") or "")[:35]
        realized_lines.append(
            f"  {emoji} {c['ticker']} {c.get('option_type','')} ${c.get('strike',0):.0f} "
            f"→ {pnl_pct:+.1f}% (${pnl_dollar:+,.0f}) {reason}"
        )

    open_filled = [p for p in _positions if p.status == "FILLED"]
    unrealized_lines = []
    total_unrealized = 0.0
    for pos in open_filled:
        current_price = _get_current_option_price(pos)
        if current_price is None or not pos.filled_price:
            unrealized_lines.append(f"  ⚪ {pos.ticker} — no quote")
            continue
        pnl_pct = (current_price - pos.filled_price) / pos.filled_price * 100
        pnl_dollar = (current_price - pos.filled_price) * 100 * pos.quantity
        total_unrealized += pnl_dollar
        emoji = "\U0001f7e2" if pnl_pct > 0 else "\U0001f534"
        unrealized_lines.append(
            f"  {emoji} {pos.ticker} {pos.option_type} ${pos.strike:.0f} "
            f"→ {pnl_pct:+.1f}% (${pnl_dollar:+,.0f})"
        )

    equity_line = ""
    equity = _get_account_equity()
    if equity:
        equity_line = f"\n\U0001f4b0 Account equity: ${equity:,.0f}"

    all_closed = load_closed_paper_positions(broker="ib")
    all_pnls = [float(r["pnl_pct"]) for r in all_closed]
    wins = [p for p in all_pnls if p > 0]
    losses = [p for p in all_pnls if p <= 0]
    win_rate = len(wins) / len(all_pnls) * 100 if all_pnls else 0

    net = total_realized + total_unrealized
    net_emoji = "\U0001f7e2" if net >= 0 else "\U0001f534"

    msg = f"\U0001f4ca <b>DAILY P&L REPORT (IB)</b> — {now_et.strftime('%b %d, %Y')}\n"
    msg += f"{'—' * 30}\n"

    if realized_lines:
        msg += f"\n<b>Realized ({len(closed_today)} closed)</b>\n"
        msg += "\n".join(realized_lines)
        msg += f"\n  <b>Subtotal: ${total_realized:+,.0f}</b>\n"
    else:
        msg += "\n<b>Realized: no closes today</b>\n"

    msg += f"\n<b>Unrealized ({len(open_filled)} open)</b>\n"
    msg += "\n".join(unrealized_lines) if unrealized_lines else "  None"
    msg += f"\n  <b>Subtotal: ${total_unrealized:+,.0f}</b>\n"

    msg += f"\n{'—' * 30}\n"
    msg += f"{net_emoji} <b>Net today: ${net:+,.0f}</b>"
    msg += equity_line
    msg += f"\n\U0001f4c8 All-time: {len(wins)}W / {len(losses)}L ({win_rate:.0f}% win rate)"

    _send_paper_telegram(msg)
    log.info("[IB] EOD report sent — realized=$%+,.0f unrealized=$%+,.0f net=$%+,.0f",
             total_realized, total_unrealized, net)


def get_portfolio_summary() -> dict:
    """Return summary stats for all paper positions."""
    if not _positions_loaded:
        _load_positions()

    open_pos = [p for p in _positions if p.status == "FILLED"]
    closed_rows = load_closed_paper_positions(broker="ib")
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
