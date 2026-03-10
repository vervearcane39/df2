"""
MARKET STATE LAYER — liquidity.py
Improvement 4 — Liquidity Vacuum Detection

Fast moves happen in OI gaps — price levels where nobody has open interest.
When spot enters a gap, there are no hedgers or writers to absorb flow,
so price accelerates until the next OI concentration.

This module detects:
1. OI gaps (strikes with very low OI between two high-OI zones)
2. Whether spot is currently IN a gap
3. The next OI wall above and below spot
4. Gap severity score

Also computes the improved signed futures volume (Doc 3):
    signed_volume = volume × sign(close - vwap)
instead of price_change × volume (which double-counts momentum).
"""
import numpy as np
import pandas as pd
from typing import Optional, Tuple, List


def compute_oi_liquidity_profile(merged: pd.DataFrame, spot: float,
                                  radius: int = 600) -> dict:
    """
    Builds an OI density profile around spot and detects vacuum zones.

    Vacuum = strike where OI < (median chain OI × gap_threshold).
    A gap zone = 3+ consecutive vacuum strikes between two OI walls.

    Returns:
        in_vacuum:         True if spot is near a vacuum zone
        vacuum_zones:      list of (start, end) tuples
        next_wall_above:   nearest high-OI strike above spot
        next_wall_below:   nearest high-OI strike below spot
        vacuum_severity:   0–1 (fraction of ±radius that is vacuum)
        gap_alert:         human-readable alert string or None
    """
    empty = {'in_vacuum': False, 'vacuum_zones': [], 'next_wall_above': None,
             'next_wall_below': None, 'vacuum_severity': 0.0, 'gap_alert': None,
             'oi_profile': {}}
    if merged.empty:
        return empty
    try:
        near = merged[abs(merged['strike_now'] - spot) <= radius].copy()
        if near.empty:
            return empty

        # Total OI per strike
        oi_by_strike = (near.groupby('strike_now')['oi_now'].sum()
                        .sort_index().to_dict())

        if not oi_by_strike:
            return empty

        strikes  = sorted(oi_by_strike.keys())
        oi_vals  = np.array([oi_by_strike[s] for s in strikes])
        median_oi = float(np.median(oi_vals[oi_vals > 0])) if (oi_vals > 0).any() else 1.0

        # Vacuum threshold: <25% of median is a vacuum
        vac_threshold = median_oi * 0.25

        # Classify each strike
        is_vac = [v < vac_threshold for v in oi_vals]

        # Find vacuum zones (consecutive vacuum strikes)
        vacuum_zones: List[Tuple[int, int]] = []
        in_zone   = False
        zone_start = 0
        for i, vac in enumerate(is_vac):
            if vac and not in_zone:
                in_zone    = True
                zone_start = strikes[i]
            elif not vac and in_zone:
                # Only report gaps of ≥2 consecutive strikes
                if i - strikes.index(zone_start) >= 2:
                    vacuum_zones.append((zone_start, strikes[i - 1]))
                in_zone = False
        if in_zone and len(strikes) - strikes.index(zone_start) >= 2:
            vacuum_zones.append((zone_start, strikes[-1]))

        # Is spot in a vacuum?
        in_vacuum = any(lo - 25 <= spot <= hi + 25 for lo, hi in vacuum_zones)

        # Next OI walls: first non-vacuum strike above and below spot
        wall_threshold = median_oi * 0.75
        walls_above = [s for s in strikes if s > spot and oi_by_strike[s] >= wall_threshold]
        walls_below = [s for s in reversed(strikes) if s < spot and oi_by_strike[s] >= wall_threshold]
        next_above  = walls_above[0] if walls_above else None
        next_below  = walls_below[0] if walls_below else None

        # Severity: fraction of zone that's vacuum
        vac_count    = sum(1 for v in is_vac if v)
        severity     = vac_count / max(len(is_vac), 1)

        # Gap alert
        gap_alert = None
        if in_vacuum:
            gap_alert = (
                f"⚡ LIQUIDITY VACUUM — spot in OI gap | "
                f"{'Next wall ↑ ' + str(next_above) if next_above else 'No wall above'} | "
                f"{'Next wall ↓ ' + str(next_below) if next_below else 'No wall below'}")
        elif severity > 0.40:
            gap_alert = f"⚠️ SPARSE OI ZONE — {severity*100:.0f}% of chain is vacuum"

        return {
            'in_vacuum':       in_vacuum,
            'vacuum_zones':    vacuum_zones,
            'next_wall_above': next_above,
            'next_wall_below': next_below,
            'vacuum_severity': round(severity, 3),
            'gap_alert':       gap_alert,
            'oi_profile':      oi_by_strike,
        }
    except Exception:
        return empty


def signed_futures_volume(futures_df: pd.DataFrame) -> dict:
    """
    Improvement to futures flow (Doc 3):
        signed_volume = volume × sign(close - vwap)

    VWAP deviation tells us whether buyers or sellers are in control
    of each minute's volume, even when tick direction is unavailable.

    Returns dict compatible with compute_futures_flow() output.
    """
    empty = {'net_signed_vol': 0.0, 'norm_signal': 0.0,
             'state': '⚪ No Futures Data', 'available': False}
    if futures_df is None or futures_df.empty or len(futures_df) < 3:
        return empty
    try:
        df   = futures_df.copy().sort_values('timestamp')
        vwap = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum().replace(0, 1)
        df['vwap']       = vwap
        df['deviation']  = df['close'] - df['vwap']
        df['signed_vol'] = df['volume'] * np.sign(df['deviation'])

        recent_svol  = float(df['signed_vol'].tail(5).sum())
        avg_vol      = float(df['volume'].mean())

        # Normalise to [-1, +1]
        scale = max(avg_vol * 5.0, 1.0)
        norm  = float(np.clip(recent_svol / scale, -1.0, 1.0))

        if   norm >  0.4: state = "🚀 FUTURES BUYING above VWAP (bullish)"
        elif norm < -0.4: state = "🔴 FUTURES SELLING below VWAP (bearish)"
        else:             state = "⚪ Neutral futures flow"

        return {'net_signed_vol': round(recent_svol / 1e6, 3),
                'norm_signal': round(norm, 3),
                'state': state, 'available': True}
    except Exception:
        return empty
