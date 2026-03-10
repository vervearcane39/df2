"""
FEATURE LAYER — greeks.py
Responsibility: Black-Scholes maths and applying greeks to a merged DataFrame.
No DB access.  No state.
"""
import math
import numpy as np
import pandas as pd
from typing import Tuple

RISK_FREE_RATE = 0.065
NIFTY_LOT_SIZE = 25


# ── Pure BS maths ─────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _d1(S, K, T, r, sigma):
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))

def bs_price(S, K, T, r, sigma, otype='CE') -> float:
    if T <= 0 or sigma <= 0:
        return max(S - K, 0) if otype == 'CE' else max(K - S, 0)
    try:
        d1 = _d1(S, K, T, r, sigma)
        d2 = d1 - sigma * math.sqrt(T)
        if otype == 'CE':
            return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)
    except Exception:
        return 0.0

def bs_delta(S, K, T, r, sigma, otype='CE') -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return (1.0 if S > K else 0.5) if otype == 'CE' else (-1.0 if S < K else -0.5)
    try:
        d1 = _d1(S, K, T, r, sigma)
        return _norm_cdf(d1) if otype == 'CE' else _norm_cdf(d1) - 1.0
    except Exception:
        return 0.5 if otype == 'CE' else -0.5

def bs_gamma(S, K, T, r, sigma) -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = _d1(S, K, T, r, sigma)
        return _norm_pdf(d1) / (S * sigma * math.sqrt(T))
    except Exception:
        return 0.0

def bs_vega(S, K, T, r, sigma) -> float:
    """Vega = S × N'(d1) × √T  (same for CE and PE)."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = _d1(S, K, T, r, sigma)
        return S * _norm_pdf(d1) * math.sqrt(T)
    except Exception:
        return 0.0

def compute_iv_newton(market_price, S, K, T, r, otype='CE',
                      max_iter=60, tol=1e-6) -> float:
    if T <= 0 or market_price <= 0 or S <= 0:
        return 0.0
    sigma = market_price / (S * math.sqrt(max(T, 1e-9) * 2.0 / math.pi))
    sigma = max(0.01, min(sigma, 5.0))
    for _ in range(max_iter):
        try:
            diff = bs_price(S, K, T, r, sigma, otype) - market_price
            if abs(diff) < tol:
                break
            d1   = _d1(S, K, T, r, sigma)
            vega = S * _norm_pdf(d1) * math.sqrt(T)
            if vega < 1e-10:
                break
            sigma -= diff / vega
            sigma  = max(0.01, min(sigma, 10.0))
        except Exception:
            break
    return round(sigma, 4) if 0.01 < sigma < 10.0 else 0.0

def atm_iv_approx(straddle_price, spot, dte_years) -> float:
    if dte_years <= 0 or straddle_price <= 0 or spot <= 0:
        return 0.0
    return float(np.clip(straddle_price / (spot * math.sqrt(2.0 * dte_years / math.pi)), 0.01, 10.0))


# ── Vectorised application to merged DataFrame ────────────────────────────────

def _ndtr_approx(x: np.ndarray) -> np.ndarray:
    """
    Vectorised N(x) — pure numpy, no scipy required.
    Uses Abramowitz & Stegun rational approximation (error < 7.5e-8).
    """
    x = np.clip(x, -8.0, 8.0)
    t = 1.0 / (1.0 + 0.2316419 * np.abs(x))
    poly = t * (0.319381530
              + t * (-0.356563782
              + t * (1.781477937
              + t * (-1.821255978
              + t * 1.330274429))))
    cdf = 1.0 - (1.0 / np.sqrt(2.0 * np.pi)) * np.exp(-0.5 * x * x) * poly
    return np.where(x >= 0, cdf, 1.0 - cdf)

def _ndpdf_approx(x: np.ndarray) -> np.ndarray:
    """Fast vectorised N'(x)."""
    return np.exp(-0.5 * np.clip(x, -8.0, 8.0) ** 2) / np.sqrt(2.0 * np.pi)


def apply_greeks(df: pd.DataFrame, spot: float, dte: float, iv: float) -> pd.DataFrame:
    """
    Adds 'delta', 'gamma', 'vega', 'weight' columns to df.
    P5 vectorised: replaces 3× df.apply() row-wise with numpy array ops.
    ~30–50× faster on a 100-row chain.
    """
    df = df.copy()
    iv_safe  = max(float(iv), 0.05)
    T        = max(float(dte), 1e-9)
    S        = float(spot)
    r        = RISK_FREE_RATE
    sqrtT    = np.sqrt(T)
    sigSqrtT = iv_safe * sqrtT

    K   = df['strike_now'].values.astype(float)
    typ = df['type_now'].values   # array of 'CE' / 'PE'

    # d1 — vectorised
    with np.errstate(divide='ignore', invalid='ignore'):
        log_sk = np.where(K > 0, np.log(np.maximum(S / K, 1e-12)), 0.0)
        d1 = np.where(
            K > 0,
            (log_sk + (r + 0.5 * iv_safe ** 2) * T) / sigSqrtT,
            0.0)

    Nd1   = _ndtr_approx(d1)
    Nd1m  = _ndtr_approx(-d1)    # N(-d1) for PE delta
    nd1   = _ndpdf_approx(d1)    # N'(d1) — same for gamma and vega

    is_ce = (typ == 'CE')
    # Delta: CE = N(d1), PE = N(d1) - 1
    delta = np.where(is_ce, Nd1, Nd1m - 1.0)
    # Gamma = N'(d1) / (S × σ × √T)  — same for CE and PE
    denom = np.where(K > 0, S * iv_safe * sqrtT, 1.0)
    gamma = np.where(K > 0, nd1 / denom, 0.0)
    # Vega = S × N'(d1) × √T
    vega  = np.where(K > 0, S * nd1 * sqrtT, 0.0)

    df['delta']  = delta
    df['gamma']  = gamma
    df['vega']   = vega
    df['weight'] = np.abs(delta)   # delta-based weighting
    return df