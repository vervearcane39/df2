"""
FEATURE LAYER — structure.py
Responsibility: Price structure — VWAP, PDH/PDL/ORH/ORL, time regime.
Pure functions; no state.
"""
import numpy as np
import pandas as pd
from datetime import datetime, time as dt_time


# ── Time regime (5-bucket) ────────────────────────────────────────────────────

REGIME_META = {
    'OPEN_DISC':   {'label': 'OPENING DISCOVERY',  'q_mod': -20, 's_mod': -0.25,
                    'desc': '9:15-9:45 fakeouts — avoid buying'},
    'INST_FLOW':   {'label': 'INSTITUTIONAL FLOW', 'q_mod': +10, 's_mod': +0.10,
                    'desc': '9:45-11:30 — best buyer window'},
    'VACUUM':      {'label': 'LIQUIDITY VACUUM',   'q_mod': -20, 's_mod': -0.25,
                    'desc': '11:30-13:30 — thin market'},
    'TREND_EXP':   {'label': 'TREND EXPANSION',    'q_mod': +5,  's_mod':  0.0,
                    'desc': '13:30-15:00 — second buyer window'},
    'EXPIRY_HDGE': {'label': 'EXPIRY HEDGING',     'q_mod': -30, 's_mod': -0.50,
                    'desc': '15:00-15:30 — seller territory'},
}

def get_time_regime() -> str:
    now = datetime.now().time()
    if   now < dt_time(9,  45): return 'OPEN_DISC'
    elif now < dt_time(11, 30): return 'INST_FLOW'
    elif now < dt_time(13, 30): return 'VACUUM'
    elif now < dt_time(15,  0): return 'TREND_EXP'
    else:                       return 'EXPIRY_HDGE'


# ── Equal-weight VWAP (Doc2 Fix 3) ───────────────────────────────────────────

def compute_vwap(spot_today_df: pd.DataFrame, current_spot: float) -> dict:
    """
    Equal-weight VWAP — options volume is NOT a price discovery driver.
    Doc2 #3: using options vol VWAP incorrectly shifts the anchor.
    """
    fallback = {'vwap': current_spot, 'upper_1sd': current_spot + 50,
                'lower_1sd': current_spot - 50, 'vwap_slope': 0.0,
                'dist': 0.0, 'above_vwap': True, 'reliable': False}
    if spot_today_df is None or spot_today_df.empty or len(spot_today_df) < 2:
        return fallback

    df    = spot_today_df.copy()
    vwap  = float(df['spot_price'].mean())
    std   = float(df['spot_price'].std()) if len(df) > 2 else 1.0
    slope = 0.0
    if len(df) >= 10:
        run   = df['spot_price'].rolling(5).mean()
        slope = float(run.iloc[-1] - run.iloc[-5]) if not run.isna().all() else 0.0

    dist = abs(current_spot - vwap)
    return {
        'vwap':       round(vwap, 2),
        'upper_1sd':  round(vwap + std, 2),
        'lower_1sd':  round(vwap - std, 2),
        'vwap_slope': round(slope, 2),
        'dist':       round(dist, 2),
        'above_vwap': current_spot >= vwap,
        'reliable':   len(df) >= 5,
    }


# ── Market structure levels ───────────────────────────────────────────────────

def compute_market_structure(spot_today_df: pd.DataFrame,
                             spot_yesterday_df: pd.DataFrame,
                             current_spot: float) -> dict:
    r = {'pdh': 0.0, 'pdl': 0.0, 'orh': 0.0, 'orl': 0.0,
         'has_pdh': False, 'has_orh': False,
         'above_pdh': False, 'below_pdl': False,
         'above_orh': False, 'below_orl': False}

    if spot_yesterday_df is not None and not spot_yesterday_df.empty:
        r['pdh']      = float(spot_yesterday_df['spot_price'].max())
        r['pdl']      = float(spot_yesterday_df['spot_price'].min())
        r['has_pdh']  = True
        r['above_pdh'] = current_spot > r['pdh']
        r['below_pdl'] = current_spot < r['pdl']

    if spot_today_df is not None and not spot_today_df.empty:
        or_end = int(datetime.now().replace(hour=9, minute=45, second=0, microsecond=0).timestamp())
        or_df  = spot_today_df[spot_today_df['timestamp'] <= or_end]
        if not or_df.empty:
            r['orh']     = float(or_df['spot_price'].max())
            r['orl']     = float(or_df['spot_price'].min())
            r['has_orh'] = True
            r['above_orh'] = current_spot > r['orh']
            r['below_orl'] = current_spot < r['orl']
    return r
