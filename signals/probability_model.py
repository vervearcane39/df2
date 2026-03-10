"""
SIGNAL LAYER — probability_model.py
Responsibility: Probability-based filters and expected-move comparisons.

Upgrade 4 — Probability Model:
    P(move > straddle) using realized vol vs implied vol.
    Also: straddle overpricing filter for option buyers.

Problem 6 Fix — Expected Move Filter:
    If straddle_price > expected_move → options are overpriced → avoid buying.
    expected_move = S × IV × sqrt(T)
"""
import math
import numpy as np
from scipy.stats import norm


def prob_move_exceeds_straddle(spot: float, straddle_price: float,
                               rv: float, dte_years: float) -> float:
    """
    P(|spot_at_expiry - spot_now| > straddle_price)
    using log-normal realized vol distribution.

    Returns 0–1.  Higher = market more likely to move past breakeven.
    Buyer's edge when > 0.50.
    """
    if rv <= 0 or dte_years <= 0 or straddle_price <= 0 or spot <= 0:
        return 0.5
    try:
        sigma = rv * math.sqrt(dte_years)
        # Approximate: 2 × P(spot moves > straddle / spot) in log-normal
        threshold = straddle_price / spot
        z = threshold / sigma
        prob = 2.0 * (1.0 - norm.cdf(z))
        return round(float(np.clip(prob, 0.0, 1.0)), 3)
    except Exception:
        return 0.5


def straddle_overpriced(straddle_price: float, expected_move: float,
                        threshold: float = 1.20) -> bool:
    """
    True if straddle costs > threshold × expected_move.
    Default: straddle > 1.2× expected move = overpriced for buyers.
    Avoid buying when True.
    """
    if expected_move <= 0:
        return False
    return straddle_price > expected_move * threshold


def iv_rv_edge(atm_iv: float, rv: float) -> dict:
    """
    Assess IV/RV ratio as buyer edge.
    iv_rv < 0.80 → IV cheap → good time to buy
    iv_rv > 1.50 → IV rich  → avoid buying
    """
    if atm_iv <= 0 or rv <= 0:
        return {'ratio': 1.0, 'edge': 'NEUTRAL', 'buyer_ok': True}
    ratio = atm_iv / rv
    if   ratio < 0.80: edge, ok = 'CHEAP_IV',   True
    elif ratio > 1.50: edge, ok = 'EXPENSIVE_IV', False
    else:              edge, ok = 'FAIR_IV',      True
    return {'ratio': round(ratio, 3), 'edge': edge, 'buyer_ok': ok}


def compute_breakeven_probability(spot: float, straddle: float, atm_iv: float,
                                  rv: float, dte_years: float) -> dict:
    """
    Comprehensive probability assessment for the current option setup.
    Returns all relevant metrics in one dict.
    """
    exp_move  = round(spot * atm_iv * math.sqrt(max(dte_years, 1e-9)), 1) if atm_iv > 0 else 0.0
    prob_win  = prob_move_exceeds_straddle(spot, straddle, rv, dte_years)
    overpriced = straddle_overpriced(straddle, exp_move)
    iv_edge   = iv_rv_edge(atm_iv, rv)

    return {
        'expected_move':  exp_move,
        'prob_win':       prob_win,
        'prob_win_pct':   round(prob_win * 100, 1),
        'overpriced':     overpriced,
        'iv_edge':        iv_edge,
        'buy_signal':     prob_win > 0.50 and not overpriced and iv_edge['buyer_ok'],
        'summary':        (
            f"P(win):{prob_win*100:.0f}% ExpMove:±{exp_move:.0f} "
            f"Straddle:{straddle:.0f} {'⚠️OVERPRICED' if overpriced else '✅OK'} "
            f"IV/RV:{iv_edge['ratio']:.2f}"
        ),
    }
