"""
SIGNAL LAYER — compression.py
Responsibility: Compression and breakout detection.

Fix 4 (Doc1): Combine straddle compression with ATR-based price range compression.
Straddle velocity alone is noisy; price range compression confirms genuine coiling.
"""
import numpy as np
import pandas as pd
from typing import Optional, Tuple


def detect_compression(straddle_history: list, spot_range_history: list,
                       spot_df: pd.DataFrame) -> dict:
    """
    Fix 4:
      ATR method: rolling 20-period mean of |spot diff|.
      Compress if current ATR < 50% of rolling mean.
    Combined with straddle compression for higher confidence.
    """
    # ── ATR compression ───────────────────────────────────────────────────
    atr_compressing = False
    current_atr     = 0.0
    avg_atr         = 0.0
    if spot_df is not None and len(spot_df) >= 20:
        diffs       = spot_df['spot_price'].diff().abs()
        rolling_atr = diffs.rolling(20).mean()
        if not rolling_atr.dropna().empty:
            current_atr     = float(diffs.iloc[-1])
            avg_atr         = float(rolling_atr.iloc[-1])
            atr_compressing = (avg_atr > 0 and current_atr < avg_atr * 0.5)

    # ── Range compression (session state) ─────────────────────────────────
    range_compressing = False
    if len(spot_range_history) >= 10:
        recent = np.mean(spot_range_history[-3:])
        older  = np.mean(spot_range_history[-10:-5])
        range_compressing = recent < older * 0.6

    # ── Straddle compression ───────────────────────────────────────────────
    straddle_compressing = False
    straddle_velocity    = 0.0
    if len(straddle_history) >= 5:
        straddle_velocity    = (np.mean(straddle_history[-3:]) -
                                np.mean(straddle_history[:3]))
        straddle_compressing = straddle_velocity < -0.5

    is_compressing = (atr_compressing or straddle_compressing) and range_compressing

    alert = None
    if is_compressing:
        parts = []
        if atr_compressing:         parts.append(f"ATR {current_atr:.1f} < 50% avg")
        if straddle_compressing:    parts.append(f"straddle −{abs(straddle_velocity):.1f}pts")
        if range_compressing:       parts.append("range shrinking")
        alert = f"🌀 COMPRESSION COILING — {' + '.join(parts)} — BREAKOUT FORMING"

    return {
        'is_compressing':      is_compressing,
        'atr_compressing':     atr_compressing,
        'range_compressing':   range_compressing,
        'straddle_compressing': straddle_compressing,
        'straddle_velocity':   straddle_velocity,
        'alert':               alert,
    }


def detect_volume_shock(fast_df: pd.DataFrame, spot: float) -> Tuple[Optional[str], Optional[str]]:
    """15-second volume spike detection."""
    if fast_df is None or fast_df.empty:
        return None, None
    try:
        fast_df  = fast_df.sort_values(['token', 'timestamp'])
        all_ts   = sorted(fast_df['timestamp'].unique())
        if len(all_ts) < 6:
            return None, None
        latest   = all_ts[-1]
        prior    = fast_df[fast_df['timestamp'] < latest]
        avg_vol  = prior.groupby('token')['volume'].mean().reset_index()
        avg_vol.columns = ['token', 'avg_vol']
        latest_d = fast_df[fast_df['timestamp'] == latest].merge(avg_vol, on='token', how='left')
        latest_d['avg_vol']   = latest_d['avg_vol'].fillna(1.0)
        latest_d['vol_ratio'] = latest_d['volume'] / (latest_d['avg_vol'] + 1.0)
        shocks   = latest_d[(latest_d['vol_ratio'] > 3.0) & (latest_d['volume'] > 200) &
                            (abs(latest_d['strike'] - spot) <= 500)]
        if shocks.empty:
            return None, None
        shocks = shocks.copy()
        shocks['dist'] = abs(shocks['strike'] - spot)
        top = shocks.nsmallest(1, 'dist').iloc[0]
        return (f"⚡ VOL SPIKE: {top['type']} {int(top['strike'])} "
                f"({top['vol_ratio']:.1f}× avg in 15s)"), top['type']
    except Exception:
        return None, None
