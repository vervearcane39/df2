"""
MARKET STATE LAYER — sensitivity.py
Upgrade 1: Spot Sensitivity Map — dGEX/dSpot

Answers: "If spot moves +50 pts, how does gamma exposure change?"
This shows where gamma EXPLODES — inflection points professional desks
watch to determine where volatility regimes can flip suddenly.

Formula:
    GEX(spot+δ) - GEX(spot-δ)
    ─────────────────────────
              2δ

Computed at each strike, then summed across the chain.
Peaks in dGEX/dSpot = high sensitivity zones = spot likely to accelerate or reverse.
"""
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict

from features.greeks import bs_gamma, RISK_FREE_RATE, NIFTY_LOT_SIZE


def compute_spot_sensitivity(merged: pd.DataFrame, spot: float,
                              dte: float, iv: float,
                              delta_s: float = 25.0,
                              radius: int = 500) -> dict:
    """
    Computes dGEX/dSpot at each strike and across the full chain.

    Returns:
        by_strike:      {strike: dGEX/dSpot value}
        sensitivity_df: DataFrame for charting
        max_sens_strike: strike with highest absolute sensitivity
        chain_total:    net chain sensitivity (signed, M/point)
        explosion_zones: strikes where sensitivity > p80 — watch these
    """
    iv_safe = max(iv, 0.05)
    near    = merged[abs(merged['strike_now'] - spot) <= radius]
    if near.empty:
        near = merged.copy()

    results = []
    for _, row in near.iterrows():
        strike = int(row['strike_now'])
        otype  = str(row['type_now'])
        oi_now = max(float(row.get('oi_now', 0)), 0.0)
        if oi_now < 100:
            continue

        # GEX at spot+δ
        gam_up   = bs_gamma(spot + delta_s, strike, dte, RISK_FREE_RATE, iv_safe)
        gex_up   = oi_now * gam_up * ((spot + delta_s) ** 2) * NIFTY_LOT_SIZE / 1e7

        # GEX at spot-δ
        gam_dn   = bs_gamma(spot - delta_s, strike, dte, RISK_FREE_RATE, iv_safe)
        gex_dn   = oi_now * gam_dn * ((spot - delta_s) ** 2) * NIFTY_LOT_SIZE / 1e7

        dGEX_dS  = (gex_up - gex_dn) / (2.0 * delta_s)
        sign     = 1.0 if otype == 'CE' else -1.0
        results.append({
            'strike':    strike,
            'type':      otype,
            'dGEX_dS':   dGEX_dS * sign,
            'oi_now':    oi_now,
        })

    if not results:
        return {'by_strike': {}, 'sensitivity_df': pd.DataFrame(),
                'max_sens_strike': int(round(spot / 50) * 50),
                'chain_total': 0.0, 'explosion_zones': []}

    df = pd.DataFrame(results)
    by_strike = df.groupby('strike')['dGEX_dS'].sum().to_dict()

    chain_total = sum(by_strike.values())
    abs_vals    = np.abs(list(by_strike.values()))
    if len(abs_vals) >= 3:
        p80_thresh  = np.percentile(abs_vals, 80)
        explosion_zones = [s for s, v in by_strike.items() if abs(v) >= p80_thresh]
    else:
        explosion_zones = []

    max_s = max(by_strike, key=lambda s: abs(by_strike[s])) if by_strike else int(round(spot / 50) * 50)

    sens_df = pd.DataFrame([
        {'strike': s, 'dGEX_dS': v, 'color': 'bullish' if v > 0 else 'bearish'}
        for s, v in sorted(by_strike.items())
    ])

    return {
        'by_strike':       by_strike,
        'sensitivity_df':  sens_df,
        'max_sens_strike': max_s,
        'chain_total':     round(chain_total, 3),
        'explosion_zones': sorted(explosion_zones),
    }
