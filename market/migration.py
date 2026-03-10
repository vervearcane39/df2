"""
MARKET STATE LAYER — migration.py
Responsibility: Detect where institutional OI is repositioning.
"""
import pandas as pd
from typing import Tuple, Optional


def compute_strike_migration(oi_hist_df: pd.DataFrame,
                             spot: float) -> Tuple[Optional[int], str]:
    """
    Improvement 2 (Doc1): Track OI shift between oldest and latest snapshot.
    Weighted center of positive OI delta shows where money is repositioning.
    """
    if oi_hist_df is None or oi_hist_df.empty:
        return None, "NONE"
    try:
        all_ts  = sorted(oi_hist_df['timestamp'].unique())
        if len(all_ts) < 2:
            return None, "NONE"
        latest  = all_ts[-1]
        oldest  = all_ts[0]
        now_oi  = oi_hist_df[oi_hist_df['timestamp'] == latest].groupby('strike')['oi'].sum()
        past_oi = oi_hist_df[oi_hist_df['timestamp'] == oldest].groupby('strike')['oi'].sum()
        delta   = (now_oi - past_oi).dropna()
        pos     = delta[delta > 0]
        if pos.empty:
            return None, "NONE"
        wc      = float((pos.index * pos.values).sum() / pos.sum())
        strike  = int(round(wc / 50) * 50)
        if   wc > spot + 100: direction = "⬆️ UPWARD MIGRATION (bulls repositioning)"
        elif wc < spot - 100: direction = "⬇️ DOWNWARD MIGRATION (bears repositioning)"
        else:                 direction = "⟷ LATERAL (consolidation)"
        return strike, direction
    except Exception:
        return None, "NONE"
