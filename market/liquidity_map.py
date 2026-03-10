"""
MARKET STATE LAYER — liquidity_map.py
Upgrade 3 — Full Liquidity Density Map

Replaces the binary vacuum/wall detection with a continuous OI density curve.
Professional desks call this the "Options Liquidity Surface."

Key insight:
    OI density at each strike = how much friction price faces there.
    Low density = fast moves.
    High density = price deceleration / reversal.
    The density curve reveals acceleration zones before price enters them.

Outputs:
    density_curve:      {strike: density_score (0–1)} normalised by z-score
    acceleration_zones: low-density ranges where price will move fast
    deceleration_zones: high-density walls where price will slow
    liquidity_profile:  'UNIFORM' | 'CLUSTERED' | 'GAPPED'
    nearest_acceleration: distance to nearest fast zone in each direction
    visual_data:        for Plotly bar chart rendering
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple


def compute_liquidity_map(merged: pd.DataFrame, spot: float,
                           radius: int = 800) -> dict:
    """
    Computes full OI density landscape around spot.

    Method:
    1. Sum CE + PE OI at each strike → total OI per strike
    2. Compute z-score normalisation: z = (OI − μ) / σ
    3. Convert z to density score [0, 1] via sigmoid
    4. Classify zones:
       - acceleration: z < −0.5 (below average = fast move zone)
       - deceleration: z > +1.0 (well above average = wall)
    5. Find contiguous regions of each type

    Returns structured dict for both decision logic and visualisation.
    """
    empty = {
        'density_curve': {}, 'acceleration_zones': [], 'deceleration_zones': [],
        'profile': 'UNKNOWN', 'nearest_acc_above': None, 'nearest_acc_below': None,
        'nearest_wall_above': None, 'nearest_wall_below': None,
        'visual_data': pd.DataFrame(),
    }
    if merged.empty:
        return empty
    try:
        near = merged[abs(merged['strike_now'] - spot) <= radius].copy()
        if near.empty:
            return empty

        oi_by_strike = near.groupby('strike_now')['oi_now'].sum().sort_index()
        if len(oi_by_strike) < 3:
            return empty

        strikes = list(oi_by_strike.index)
        ois     = oi_by_strike.values.astype(float)

        # Z-score normalisation
        mu, sigma = ois.mean(), ois.std() + 1.0
        z_scores  = (ois - mu) / sigma

        # Sigmoid → [0, 1] density
        def _sigmoid(z): return 1.0 / (1.0 + np.exp(-z))
        density   = _sigmoid(z_scores)

        density_curve = {int(s): round(float(d), 4) for s, d in zip(strikes, density)}
        z_dict        = {int(s): float(z) for s, z in zip(strikes, z_scores)}

        # Zone classification
        acc_zones  = []   # fast-move zones
        dec_zones  = []   # wall zones (deceleration)
        in_acc, in_dec = False, False
        acc_start, dec_start = 0, 0

        for s, z in zip(strikes, z_scores):
            if z < -0.5 and not in_acc:
                in_acc = True; acc_start = s
            elif z >= -0.5 and in_acc:
                acc_zones.append((acc_start, s - 50)); in_acc = False
            if z > 1.0 and not in_dec:
                in_dec = True; dec_start = s
            elif z <= 1.0 and in_dec:
                dec_zones.append((dec_start, s - 50)); in_dec = False

        if in_acc: acc_zones.append((acc_start, strikes[-1]))
        if in_dec: dec_zones.append((dec_start, strikes[-1]))

        # Profile classification
        high_oi_count = (z_scores > 1.0).sum()
        low_oi_count  = (z_scores < -0.5).sum()
        n = len(strikes)

        if high_oi_count > n * 0.3:  profile = 'CLUSTERED'
        elif low_oi_count > n * 0.4: profile = 'GAPPED'
        else:                         profile = 'UNIFORM'

        # Nearest zones in each direction
        def _nearest_above(zones, s):
            above = [(lo, hi) for lo, hi in zones if lo > s]
            return above[0][0] if above else None

        def _nearest_below(zones, s):
            below = [(lo, hi) for lo, hi in zones if hi < s]
            return below[-1][1] if below else None

        acc_above = _nearest_above(acc_zones, spot)
        acc_below = _nearest_below(acc_zones, spot)
        wall_above = _nearest_above(dec_zones, spot)
        wall_below = _nearest_below(dec_zones, spot)

        # Build visual dataframe
        vis_df = pd.DataFrame({
            'strike':  strikes,
            'density': density,
            'z_score': z_scores,
            'oi':      ois,
            'zone':    ['ACC' if z < -0.5 else ('WALL' if z > 1.0 else 'NORMAL')
                        for z in z_scores],
        })

        return {
            'density_curve':      density_curve,
            'acceleration_zones': acc_zones,
            'deceleration_zones': dec_zones,
            'profile':            profile,
            'nearest_acc_above':  acc_above,
            'nearest_acc_below':  acc_below,
            'nearest_wall_above': wall_above,
            'nearest_wall_below': wall_below,
            'visual_data':        vis_df,
            'z_dict':             z_dict,
        }
    except Exception:
        return empty
