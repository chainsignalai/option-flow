from __future__ import annotations

import os
import logging
from dataclasses import asdict
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("[DB] Supabase credentials not set — persistence disabled")
        return None
    from supabase import create_client
    _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("[DB] Supabase client initialized")
    return _client


def save_signal(result, mode: str = "live", regime: str = None):
    """Persist a StrategyResult to the signals table."""
    client = _get_client()
    if not client:
        return None
    try:
        row = {
            "ticker": result.ticker,
            "direction": result.direction.value if hasattr(result.direction, "value") else str(result.direction),
            "conviction": result.conviction,
            "composite_score": result.composite_score,
            "layers_aligned": result.layers_aligned,
            "mode": mode,
            "regime": regime,
            "flow_signal": result.flow.signal.value,
            "flow_score": result.flow.score,
            "darkpool_signal": result.darkpool.signal.value,
            "darkpool_score": result.darkpool.score,
            "gex_signal": result.gex.signal.value,
            "gex_score": result.gex.score,
            "iv_signal": result.iv.signal.value,
            "iv_score": result.iv.score,
            "technicals_signal": result.technicals.signal.value,
            "technicals_score": result.technicals.score,
            "catalyst_signal": result.catalyst.signal.value,
            "catalyst_score": result.catalyst.score,
            "social_signal": result.social.signal.value,
            "social_score": result.social.score,
            "live_enhancements": result.live_enhancements or [],
            "raw_result": _serialize_result(result),
        }
        resp = client.table("signals").insert(row).execute()
        log.info(f"[DB] Signal saved: {result.ticker} {mode}")
        return resp.data[0]["id"] if resp.data else None
    except Exception as e:
        log.error(f"[DB] Failed to save signal for {result.ticker}: {e}")
        return None


def save_backtest_run(start_date: str, end_date: str, top_n: int,
                      min_conviction: str, delay: float,
                      stats: dict) -> str | None:
    """Persist backtest run metadata. Returns the run ID."""
    client = _get_client()
    if not client:
        return None
    try:
        row = {
            "start_date": start_date,
            "end_date": end_date,
            "top_n": top_n,
            "min_conviction": min_conviction,
            "delay": delay,
            "total_trades": stats.get("total_trades"),
            "win_rate": stats.get("win_rate"),
            "profit_factor": stats.get("profit_factor"),
            "sharpe": stats.get("sharpe"),
            "max_drawdown": stats.get("max_drawdown"),
            "total_return": stats.get("total_return"),
        }
        resp = client.table("backtest_runs").insert(row).execute()
        run_id = resp.data[0]["id"] if resp.data else None
        log.info(f"[DB] Backtest run saved: {start_date} to {end_date} (id={run_id})")
        return run_id
    except Exception as e:
        log.error(f"[DB] Failed to save backtest run: {e}")
        return None


def save_backtest_trades(trades: list, run_id: str):
    """Persist backtest trades in bulk."""
    client = _get_client()
    if not client or not trades:
        return
    try:
        rows = []
        for t in trades:
            d = asdict(t) if hasattr(t, "__dataclass_fields__") else t
            rows.append({
                "backtest_run_id": run_id,
                "ticker": d["ticker"],
                "date": d["date"],
                "direction": d["direction"],
                "conviction": d["conviction"],
                "composite_score": d["composite_score"],
                "entry_price": d["entry_price"],
                "exit_price": d["exit_price"],
                "return_pct": d["return_pct"],
                "win": d["win"],
                "regime": d.get("regime"),
                "layer_signals": d.get("layer_signals", {}),
                "layer_scores": d.get("layer_scores", {}),
            })
        resp = client.table("backtest_trades").insert(rows).execute()
        log.info(f"[DB] {len(rows)} backtest trades saved for run {run_id}")
    except Exception as e:
        log.error(f"[DB] Failed to save backtest trades: {e}")


def _serialize_result(result) -> dict:
    """Convert StrategyResult to a JSON-safe dict."""
    try:
        d = asdict(result)
        for layer in ("flow", "darkpool", "gex", "iv", "technicals", "catalyst", "social"):
            if layer in d and "signal" in d[layer]:
                sig = d[layer]["signal"]
                d[layer]["signal"] = sig.value if hasattr(sig, "value") else str(sig)
        if "direction" in d:
            sig = d["direction"]
            d["direction"] = sig.value if hasattr(sig, "value") else str(sig)
        return d
    except Exception:
        return {}
