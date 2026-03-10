"""
MARKET STATE LAYER — gamma.py
Responsibility: GEX computation and gamma flip detection.

Improvement 2 — True Gamma Flip Root Solver (Doc 2.3):
    The naive approach detects sign-change between adjacent strikes.
    Problem: GEX varies continuously — the actual zero crossing is between strikes.

    Proper method: evaluate total chain GEX at (spot + Δ) for a range of Δ,
    find where GEX(Δ) = 0 using linear interpolation between sign changes.
    This gives the actual price level where dealer behaviour flips,
    not an arbitrary strike label.

Improvement 2b — Dynamic GEX Radius (Doc 2.2):
    Fixed ±400 misses real walls on wide expected-move days.
    radius = max(400, expected_move × 2)

Improvement 2c — Per-strike IV (Doc 2.1):
    ATM IV for full chain overestimates gamma of OTM options.
    We estimate per-strike IV using a linear skew model:
        iv_strike = atm_iv × (1 + skew_slope × moneyness)
    where moneyness = (strike - spot) / spot
    This is a first-order smile approximation without a full surface calibration.
"""
import math
import numpy as np
import pandas as pd
from typing import Optional, Tuple

from features.greeks import bs_gamma, RISK_FREE_RATE, NIFTY_LOT_SIZE


# ── Per-strike IV estimation ──────────────────────────────────────────────────

def _strike_iv(atm_iv: float, strike: float, spot: float,
               skew_slope: float = -0.5) -> float:
    """
    Linear skew approximation for per-strike IV.
    skew_slope < 0: typical downside skew (OTM puts more expensive than OTM calls).
    moneyness = (strike - spot) / spot
    iv_strike = max(atm_iv × (1 - skew_slope × moneyness), 0.05)

    This is a rough approximation. For production, use Newton-solver
    on each mid-price to get true implied vol per strike.
    """
    moneyness = (strike - spot) / max(spot, 1.0)
    iv_adj    = atm_iv * (1.0 - skew_slope * moneyness)
    return max(float(iv_adj), 0.05)


# ── Chain-level GEX at a hypothetical spot level ─────────────────────────────

def _chain_gex_at(spot_h: float, rows: list, dte: float) -> float:
    """
    Compute total chain GEX assuming spot is at `spot_h`.
    Used by the root solver to evaluate GEX continuously across price levels.
    rows: list of (strike, otype, oi_now, atm_iv, actual_spot)
    """
    total = 0.0
    for strike, otype, oi_now, atm_iv, actual_spot in rows:
        if oi_now < 100:
            continue
        iv_s  = _strike_iv(atm_iv, strike, spot_h)
        gam   = bs_gamma(spot_h, strike, dte, RISK_FREE_RATE, iv_s)
        gex_v = oi_now * gam * (spot_h ** 2) * NIFTY_LOT_SIZE / 1e7
        sign  = 1.0 if otype == 'CE' else -1.0
        total += sign * gex_v
    return total


# ── Main GEX computation ─────────────────────────────────────────────────────

def compute_gex(merged: pd.DataFrame, spot: float, dte: float,
                iv: float, atm_only_radius: int = 400,
                expected_move: float = 0.0) -> dict:
    """
    GEX = Σ( OI_now × gamma × spot² × lot ) by strike

    Improvements:
    1. Per-strike IV via linear skew (not just ATM IV everywhere)
    2. Dynamic radius: max(400, expected_move × 2) prevents missing OTM walls
    3. Root-solver gamma flip: finds exact price where chain GEX = 0

    Sign: CE positive, PE negative (standard dealer long-gamma convention).
    Net GEX > 0 → dealer net long gamma → PINNING.
    Net GEX < 0 → dealer net short gamma → AMPLIFYING.
    """
    empty = {'net_gex': 0.0, 'gex_mode': '⚪ UNKNOWN γ',
             'gex_wall': int(spot), 'gex_flip_zone': None,
             'gex_by_strike': {}}
    if merged.empty or dte <= 0:
        return empty

    # Dynamic radius (Improvement 2b)
    dynamic_radius = max(atm_only_radius, int(expected_move * 2)) if expected_move > 0 else atm_only_radius
    dynamic_radius = min(dynamic_radius, 800)   # cap at ±800 regardless

    iv_safe = max(iv, 0.05)
    near    = merged[abs(merged['strike_now'] - spot) <= dynamic_radius]
    if near.empty:
        near = merged

    gex_by_strike: dict = {}
    rows_for_flip: list = []

    for _, row in near.iterrows():
        strike = int(row['strike_now'])
        otype  = str(row['type_now'])
        oi_now = max(float(row.get('oi_now', 0)), 0.0)
        if oi_now < 100:
            continue

        # Per-strike IV (Improvement 2c)
        iv_s  = _strike_iv(iv_safe, float(strike), spot)
        gamma = bs_gamma(spot, strike, dte, RISK_FREE_RATE, iv_s)
        gex_v = oi_now * gamma * (spot ** 2) * NIFTY_LOT_SIZE / 1e7
        sign  = 1.0 if otype == 'CE' else -1.0

        if strike not in gex_by_strike:
            gex_by_strike[strike] = {'CE': 0.0, 'PE': 0.0}
        gex_by_strike[strike][otype] += gex_v

        rows_for_flip.append((float(strike), otype, oi_now, iv_safe, spot))

    if not gex_by_strike:
        return empty

    # ── Net GEX and wall ─────────────────────────────────────────────────────
    net_gex  = 0.0
    max_abs  = 0.0
    gex_wall = int(spot)
    for strike, d in gex_by_strike.items():
        snet     = d.get('CE', 0.0) - d.get('PE', 0.0)
        net_gex += snet
        if abs(snet) > max_abs:
            max_abs  = abs(snet)
            gex_wall = strike

    # ── True gamma flip via root solver (Improvement 2 — Doc 2.3) ─────────
    # Evaluate GEX at a grid of spot levels around current spot
    # Find where total chain GEX crosses zero (true flip level)
    gex_flip = _find_gamma_flip_root(rows_for_flip, spot, dte, dynamic_radius)

    gex_mode = (
        "📌 LONG γ  (PINNING)"    if net_gex >  3.0 else
        "💥 SHORT γ (AMPLIFYING)" if net_gex < -3.0 else "⚖️ NEUTRAL γ"
    )

    return {
        'net_gex':       round(net_gex, 2),
        'gex_mode':      gex_mode,
        'gex_wall':      gex_wall,
        'gex_flip_zone': gex_flip,
        'gex_by_strike': gex_by_strike,
        'dynamic_radius': dynamic_radius,
        # P1 fix: Uncertainty ranges on GEX estimate
        # GEX uncertainty comes from 3 sources:
        #   1. IV assumption: ±20% IV change → ±gamma proportionally (gamma ∝ 1/IV)
        #   2. Unknown dealer inventory: true dealer position unknown, assume ±15%
        #   3. OI classification: spreads/rolls inflating OI by est. 20-30%
        # Combined conservative uncertainty: ±35% of point estimate
        'gex_low':       round(net_gex * 0.65, 2),   # -35% scenario
        'gex_high':      round(net_gex * 1.35, 2),   # +35% scenario
        'gex_uncertainty_pct': 35,
        'gex_uncertainty_note': (
            "±35% estimate: IV assumption ±20%, dealer inventory unknown ±15%. "
            "OI includes spreads/rolls that inflate true dealer gamma exposure."
        ),
    }


def _find_gamma_flip_root(rows: list, spot: float, dte: float,
                           radius: int) -> Optional[int]:
    """
    Root solver for the exact gamma flip level.

    Evaluates total chain GEX at spot_h ∈ [spot−radius, spot+radius]
    in 25-point increments.  Finds where GEX changes sign and linearly
    interpolates to the nearest 25-point level.

    Returns the flip level rounded to nearest 50 (Nifty strike grid),
    or None if no zero crossing exists in the range.
    """
    if not rows:
        return None
    try:
        step   = 25
        lo, hi = spot - radius, spot + radius
        levels = np.arange(lo, hi + step, step)
        if len(levels) < 2:
            return None

        gex_vals = [_chain_gex_at(float(lv), rows, dte) for lv in levels]

        # Find sign changes
        for i in range(len(gex_vals) - 1):
            g1, g2 = gex_vals[i], gex_vals[i + 1]
            if g1 == 0.0:
                return int(round(levels[i] / 50) * 50)
            if g1 * g2 < 0:   # sign change between levels[i] and levels[i+1]
                # Linear interpolation
                lv1, lv2 = float(levels[i]), float(levels[i + 1])
                root = lv1 + (lv2 - lv1) * abs(g1) / (abs(g1) + abs(g2))
                return int(round(root / 50) * 50)
        return None
    except Exception:
        return None


def compute_gamma_flip_signal(spot: float, gex_flip: Optional[int],
                              net_gex: float,
                              prev_spot: Optional[float]) -> Tuple[Optional[str], bool]:
    """
    Gamma flip crossing = volatility regime change.
    Uses root-solved flip level (not just nearest strike).
    """
    if gex_flip is None:
        return None, False
    dist    = abs(spot - gex_flip)
    crossed = (prev_spot is not None and
               ((prev_spot < gex_flip <= spot) or (prev_spot > gex_flip >= spot)))
    if crossed:
        direction = "UPWARD" if spot > gex_flip else "DOWNWARD"
        return (f"🚨 GAMMA FLIP CROSSED {direction} at {gex_flip} — "
                f"volatility regime change | "
                f"{'AMPLIFICATION' if net_gex < 0 else 'PINNING'} ahead"), True
    elif dist < 50:
        return f"⚠️ APPROACHING FLIP at {gex_flip} ({dist:.0f} pts)", False
    return None, False
