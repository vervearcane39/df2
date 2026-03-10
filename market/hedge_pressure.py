"""
MARKET STATE LAYER — hedge_pressure.py
Upgrade 1: Dealer Hedge Pressure Map  (dHedge/dSpot)

The single most important missing piece.

Right now the system computes net GEX and hedge scenarios at ±50pt.
But what institutions actually watch is dHedge/dSpot:
    "If spot moves 1 point RIGHT NOW, how many futures must dealers buy/sell?"

This is the real engine of expiry explosions.

Formula:
    dHedge/dSpot = Σ( OI_i × gamma_i × lot_size × dealer_sign_i )

Since gamma = d(delta)/d(spot), this is literally the rate of change
of dealer hedge demand per unit spot move.

Units: futures contracts per point (or M rupees per point)

Pressure score:
    pressure_per_pt = dHedge/dSpot  (absolute value)
    velocity_pressure = pressure_per_pt × spot_velocity

Interpretation:
    low      < 0.5M/pt → market stable, moves grind
    medium   0.5–1.5M  → trending possible
    high     1.5–3.0M  → cascade risk
    extreme  > 3.0M    → hero-zero possible

Why this differs from net_gex:
    net_gex is a stock (total gamma exposure at current spot)
    hedge_pressure is a flow (how fast that exposure changes per unit move)
    High net_gex with low velocity = pinning
    Low net_gex with HIGH velocity = directional cascade
"""
import numpy as np
import pandas as pd
from typing import Optional

from features.greeks import NIFTY_LOT_SIZE, bs_gamma, RISK_FREE_RATE


def compute_hedge_pressure(
    merged: pd.DataFrame,
    spot: float,
    dte: float,
    atm_iv: float,
    velocity: float,
    radius: int = 600,
) -> dict:
    """
    Computes hedge pressure map — dHedge/dSpot per strike and total.

    For each strike, dealer_sign is inferred from price-OI flow
    (LONG_BUILD = dealer short = sign -1; SHORT_BUILD = dealer long = sign +1).

    Returns:
        pressure_per_pt:   M rupees of futures per 1pt spot move (unsigned)
        pressure_signed:   signed — positive = bullish pressure (dealers must buy if up)
        velocity_pressure: pressure × velocity (realised flow rate)
        pressure_level:    'LOW' | 'MEDIUM' | 'HIGH' | 'EXTREME'
        by_strike:         {strike: pressure contribution}
        hero_zone_strikes: strikes with >20% of total pressure (concentrated)
        pressure_curve_df: DataFrame for visualisation
    """
    empty = {
        'pressure_per_pt': 0.0, 'pressure_signed': 0.0,
        'velocity_pressure': 0.0, 'pressure_level': 'LOW',
        'by_strike': {}, 'hero_zone_strikes': [],
        'pressure_curve_df': pd.DataFrame(),
    }
    if merged.empty or dte <= 0:
        return empty

    try:
        iv_safe = max(atm_iv, 0.05)
        near = merged[abs(merged['strike_now'] - spot) <= radius].copy()
        if near.empty:
            near = merged

        by_strike: dict = {}
        total_bull = 0.0
        total_bear = 0.0

        for _, row in near.iterrows():
            strike  = int(row['strike_now'])
            otype   = str(row['type_now'])
            oi_now  = max(float(row.get('oi_now', 0)), 0.0)
            if oi_now < 100:
                continue

            # Per-strike IV (linear skew)
            m_ness  = (strike - spot) / max(spot, 1.0)
            iv_s    = max(iv_safe * (1.0 + 0.5 * abs(m_ness)), 0.05)

            gamma   = bs_gamma(spot, strike, dte, RISK_FREE_RATE, iv_s)

            # Dealer sign from price-OI classification
            flow    = str(row.get('oi_flow_type', 'NEUTRAL'))
            dsign   = -1.0 if flow in ('LONG_BUILD', 'SHORT_COVER') else 1.0

            # dHedge/dSpot for this strike
            # When spot moves +1: dealer delta changes by gamma × 1
            # Dealer must trade: dsign × gamma × OI × lot
            dh_ds = -dsign * gamma * oi_now * NIFTY_LOT_SIZE  # negative dsign: dealer short → must buy

            if otype == 'CE':
                # Calls: dealer short → positive hedge (must buy) when short
                contribution = dh_ds
            else:
                # Puts: dealer short → negative hedge (must sell) when short
                contribution = -dh_ds

            if strike not in by_strike:
                by_strike[strike] = 0.0
            by_strike[strike] += contribution

            if contribution > 0:
                total_bull += abs(contribution)
            else:
                total_bear += abs(contribution)

        total_unsigned = sum(abs(v) for v in by_strike.values())
        total_signed   = sum(by_strike.values())

        # Convert to M rupees / point
        p_unsigned = total_unsigned / 1e7
        p_signed   = total_signed   / 1e7

        # Velocity pressure (realised flow rate per minute)
        vel_p = p_unsigned * velocity

        # Pressure level
        if   p_unsigned > 3.0:  level = 'EXTREME'
        elif p_unsigned > 1.5:  level = 'HIGH'
        elif p_unsigned > 0.5:  level = 'MEDIUM'
        else:                   level = 'LOW'

        # Hero zones: strikes with >15% of total pressure
        hero_zones = [s for s, v in by_strike.items()
                      if total_unsigned > 0 and abs(v) / total_unsigned > 0.15]

        # Build pressure curve
        curve_data = [{
            'strike':       s,
            'pressure':     v / 1e7,
            'direction':    'BULL' if v > 0 else 'BEAR',
        } for s, v in sorted(by_strike.items())]
        curve_df = pd.DataFrame(curve_data) if curve_data else pd.DataFrame()

        return {
            'pressure_per_pt':   round(p_unsigned, 3),
            'pressure_signed':   round(p_signed, 3),
            'velocity_pressure': round(vel_p, 3),
            'pressure_level':    level,
            'by_strike':         {s: round(v/1e7, 4) for s, v in by_strike.items()},
            'hero_zone_strikes': hero_zones,
            'pressure_curve_df': curve_df,
        }
    except Exception:
        return empty
