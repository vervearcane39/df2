"""
FEATURE LAYER — volatility.py
Responsibility: IV regime, realized vol, IV change rate, skew.
All functions are pure (no DB, no state).
"""
import math
import numpy as np
import pandas as pd
from typing import Tuple, List

from features.greeks import (atm_iv_approx, compute_iv_newton, bs_delta,
                              RISK_FREE_RATE)


# ── IV regime detection ───────────────────────────────────────────────────────

def compute_iv_regime(atm_iv: float, iv_history: list,
                      iv_df: pd.DataFrame) -> dict:
    """
    Fix 5 (Doc1): use IV change rate, not just percentile.
    iv_history: list of recent atm_iv values (session state).
    iv_df: 30-day IV log from DB.
    """
    # Percentile (kept for context but not sole metric)
    iv_pct = 50.0
    if not iv_df.empty and len(iv_df) >= 5 and atm_iv > 0:
        iv_pct = float((iv_df['atm_iv'] < atm_iv).mean() * 100.0)

    # IV change rate (10-min window = last ~10 entries at 1/min)
    iv_change_rate = 0.0
    iv_direction   = "➡️ STABLE"
    if len(iv_history) >= 6:
        recent = np.mean(iv_history[-3:])
        past   = np.mean(iv_history[:3])
        iv_change_rate = float(recent - past)
        # Doc1 Fix 5 thresholds: >+0.5% = expansion, < -0.5% = crush
        if   iv_change_rate >  0.005: iv_direction = "📈 EXPANDING"
        elif iv_change_rate < -0.005: iv_direction = "📉 CRUSHING"

    if   iv_pct > 80: label = "❗HIGH IV"
    elif iv_pct < 20: label = "🟢 LOW IV"
    else:             label = "NORMAL IV"

    return {
        'atm_iv':         atm_iv,
        'iv_pct':         round(iv_pct, 1),
        'iv_label':       label,
        'iv_direction':   iv_direction,
        'iv_change_rate': round(iv_change_rate * 100, 3),  # in % points
    }


def compute_realized_vol(spot_df: pd.DataFrame) -> float:
    """Annualised realised vol from log returns of recent spot prices."""
    if spot_df is None or len(spot_df) < 6:
        return 0.0
    prices      = spot_df['spot_price'].values[-30:]
    log_returns = np.diff(np.log(prices))
    if log_returns.std() == 0:
        return 0.0
    return round(float(log_returns.std()) * math.sqrt(252 * 375), 4)


def compute_iv_rv_label(atm_iv: float, rv: float) -> str:
    if atm_iv <= 0 or rv <= 0:
        return "N/A"
    ratio = atm_iv / rv
    if   ratio < 0.8:  return f"📉 IV CHEAP ({ratio:.2f}× RV) — buyer edge"
    elif ratio > 1.5:  return f"📈 IV RICH  ({ratio:.2f}× RV) — costly"
    return f"⚖️ IV FAIR ({ratio:.2f}× RV)"


def compute_25d_skew(merged: pd.DataFrame, spot: float,
                     dte: float, iv_base: float) -> Tuple[float, float, float]:
    """
    25-delta skew: IV_25P − IV_25C.
    Positive = put fear premium (bearish sentiment).
    """
    if merged.empty or dte <= 0 or iv_base <= 0:
        return 0.0, 0.0, 0.0
    try:
        target = 0.25
        ce_rows = merged[merged['type_now'] == 'CE'].copy()
        pe_rows = merged[merged['type_now'] == 'PE'].copy()
        if ce_rows.empty or pe_rows.empty:
            return 0.0, 0.0, 0.0

        ce_rows['d_dist'] = (ce_rows['delta'].abs() - target).abs()
        pe_rows['d_dist'] = (pe_rows['delta'].abs() - target).abs()
        best_ce = ce_rows.nsmallest(1, 'd_dist').iloc[0]
        best_pe = pe_rows.nsmallest(1, 'd_dist').iloc[0]

        iv_25c = compute_iv_newton(float(best_ce['close_now']), spot,
                                   float(best_ce['strike_now']), dte, RISK_FREE_RATE, 'CE')
        iv_25p = compute_iv_newton(float(best_pe['close_now']), spot,
                                   float(best_pe['strike_now']), dte, RISK_FREE_RATE, 'PE')

        skew = round(iv_25p - iv_25c, 4) if iv_25c > 0 and iv_25p > 0 else 0.0
        return skew, round(iv_25c, 4), round(iv_25p, 4)
    except Exception:
        return 0.0, 0.0, 0.0


def expected_move(spot: float, atm_iv: float, dte_years: float) -> float:
    """1-SD expected move = S × IV × √T."""
    if spot <= 0 or atm_iv <= 0 or dte_years <= 0:
        return 0.0
    return round(spot * atm_iv * math.sqrt(dte_years), 1)
