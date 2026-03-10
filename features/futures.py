"""
FEATURE LAYER — futures.py
Responsibility: Futures flow signals.

Problem 4 Fix — Futures Flow:
    Nifty price discovery happens primarily through futures, not options.
    futures_delta = price_change × volume (directional flow proxy).
    Weighted 0.15 in signal scoring.

Engine integration (engine3.py change required):
    Add NIFTY50 front-month futures token to WebSocket subscription.
    Token: "26000" (NSE Nifty 50 futures — verify current month symbol).
    Store in system_health or a new futures_candles table.

Note: If futures data is unavailable, all functions return safe fallbacks.
"""
import sqlite3
import time
import pandas as pd
import numpy as np
from typing import Optional


FUTURES_TOKEN   = "26000"   # Nifty front-month futures token
FUTURES_LOT     = 25


def load_futures_data(db_path: str, lookback_mins: int = 30) -> pd.DataFrame:
    """
    Load futures candle data.
    Falls back gracefully if table does not exist (engine not yet updated).
    """
    try:
        conn = sqlite3.connect(db_path)
        cutoff = int(time.time()) - (lookback_mins * 60)
        df = pd.read_sql(
            f"SELECT timestamp, close, volume FROM futures_candles "
            f"WHERE timestamp >= {cutoff} ORDER BY timestamp ASC", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def compute_futures_flow(futures_df: pd.DataFrame) -> dict:
    """
    Futures delta = Σ(price_change × volume) over recent candles.

    Interpretation:
        Strong positive → buyers driving price through futures → bullish
        Strong negative → sellers driving through futures → bearish
        Near zero       → no directional futures conviction
    """
    empty = {'futures_delta': 0.0, 'futures_signal': 0.0,
             'futures_state': '⚪ No Futures Data', 'vol_surge': False,
             'available': False}

    if futures_df is None or futures_df.empty or len(futures_df) < 3:
        return empty

    try:
        df           = futures_df.copy().sort_values('timestamp')
        df['dP']     = df['close'].diff()
        df['flow']   = df['dP'] * df['volume']
        df['flow']   = df['flow'].fillna(0)

        recent_flow  = float(df['flow'].tail(5).sum())
        avg_vol      = float(df['volume'].mean())
        latest_vol   = float(df['volume'].iloc[-1])

        # Normalise to [-1, +1] signal
        vol_scale    = max(avg_vol * df['close'].mean() * 0.01, 1.0)
        futures_sig  = float(np.clip(recent_flow / vol_scale, -1.0, 1.0))

        # Volume surge detection
        vol_surge    = latest_vol > avg_vol * 2.5

        if   futures_sig >  0.4: state = "🟢 FUTURES BUYING (bullish futures flow)"
        elif futures_sig < -0.4: state = "🔴 FUTURES SELLING (bearish futures flow)"
        else:                    state = "⚪ Neutral Futures"

        return {'futures_delta':  round(recent_flow / 1e6, 3),
                'futures_signal': round(futures_sig, 3),
                'futures_state':  state,
                'vol_surge':      vol_surge,
                'available':      True}
    except Exception:
        return empty


def futures_signal_for_scoring(futures_signal: float) -> float:
    """
    Normalised [-1, +1] component for the weighted signal scorer.
    Returns 0.0 if futures data unavailable.
    """
    return float(np.clip(futures_signal, -1.0, 1.0))
