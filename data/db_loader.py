"""
DATA LAYER — db_loader.py
Responsibility: Load raw data from SQLite.  No logic, no transformations.
"""
import sqlite3
import time
import pandas as pd
from datetime import datetime, time as dt_time, timedelta


def load_all(db_path: str, lookback_mins: int):
    """
    Returns a dict with all raw DataFrames needed by the feature layer.
    Callers must not do further DB work — only use these frames.
    """
    conn = sqlite3.connect(db_path)
    now  = int(time.time())

    # ── Health / spot ──────────────────────────────────────────────────────
    health_df = pd.read_sql(
        "SELECT * FROM system_health ORDER BY timestamp DESC LIMIT 1", conn)
    health_score = float(health_df.iloc[0]['health_pct']) if not health_df.empty else 0.0

    spot_cutoff = now - ((lookback_mins + 60) * 60)
    spot_df = pd.read_sql(
        f"SELECT timestamp, spot_price FROM system_health "
        f"WHERE timestamp >= {spot_cutoff} ORDER BY timestamp ASC", conn)

    # ── Options (1-min candles) ────────────────────────────────────────────
    opt_cutoff = now - ((lookback_mins + 5) * 60)
    try:
        options_df = pd.read_sql(
            f"SELECT timestamp, token, strike, type, close, oi, volume, tick_count "
            f"FROM candles WHERE timestamp >= {opt_cutoff}", conn)
    except Exception:
        options_df = pd.read_sql(
            f"SELECT timestamp, token, strike, type, close, oi, volume "
            f"FROM candles WHERE timestamp >= {opt_cutoff}", conn)
        options_df['tick_count'] = 0

    # ── Fast candles (15s) ─────────────────────────────────────────────────
    try:
        fast_df = pd.read_sql(
            f"SELECT timestamp, token, strike, type, close, oi, volume "
            f"FROM fast_candles WHERE timestamp >= {now - 600} "
            f"ORDER BY timestamp ASC", conn)
    except Exception:
        fast_df = pd.DataFrame()

    # ── Token registry (ghost OI guard) ───────────────────────────────────
    try:
        reg_df     = pd.read_sql("SELECT token, first_seen_ts FROM token_registry", conn)
        token_ages = dict(zip(reg_df['token'], reg_df['first_seen_ts']))
    except Exception:
        token_ages = {}

    # ── IV history (30-day percentile) ────────────────────────────────────
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS iv_log "
                     "(timestamp INTEGER PRIMARY KEY, atm_iv REAL, straddle_price REAL)")
        conn.commit()
        iv_df = pd.read_sql(
            f"SELECT atm_iv FROM iv_log WHERE timestamp >= {now - 30 * 86400}", conn)
    except Exception:
        iv_df = pd.DataFrame()

    # ── Today spot (for VWAP) ──────────────────────────────────────────────
    today_start = int(datetime.now().replace(hour=9, minute=15, second=0, microsecond=0).timestamp())
    try:
        spot_today_df = pd.read_sql(
            f"SELECT timestamp, spot_price, "
            f"COALESCE(total_option_volume, 0) as total_option_volume "
            f"FROM system_health WHERE timestamp >= {today_start} ORDER BY timestamp ASC", conn)
    except Exception:
        spot_today_df = pd.read_sql(
            f"SELECT timestamp, spot_price FROM system_health "
            f"WHERE timestamp >= {today_start} ORDER BY timestamp ASC", conn)
        if not spot_today_df.empty:
            spot_today_df['total_option_volume'] = 1.0

    # ── Yesterday spot (PDH / PDL) ─────────────────────────────────────────
    yesterday = (datetime.now() - timedelta(days=1)).date()
    while yesterday.weekday() >= 5:
        yesterday -= timedelta(days=1)
    yday_start = int(datetime.combine(yesterday, dt_time(9, 15)).timestamp())
    yday_end   = int(datetime.combine(yesterday, dt_time(15, 30)).timestamp())
    try:
        spot_yesterday_df = pd.read_sql(
            f"SELECT timestamp, spot_price FROM system_health "
            f"WHERE timestamp >= {yday_start} AND timestamp <= {yday_end} "
            f"ORDER BY timestamp ASC", conn)
    except Exception:
        spot_yesterday_df = pd.DataFrame()

    # ── OI history (strike migration) ─────────────────────────────────────
    try:
        oi_hist_df = pd.read_sql(
            f"SELECT timestamp, strike, type, oi FROM strike_oi_history "
            f"WHERE timestamp >= {now - 3600} ORDER BY timestamp ASC", conn)
    except Exception:
        oi_hist_df = pd.DataFrame()

    # ── Futures candles (graceful fallback) ──────────────────────────────
    try:
        fut_cutoff = now - (60 * 60)   # 60-min window
        futures_df = pd.read_sql(
            f"SELECT timestamp, close, volume FROM futures_candles "
            f"WHERE timestamp >= {fut_cutoff} ORDER BY timestamp ASC", conn)
    except Exception:
        futures_df = pd.DataFrame()

    conn.close()
    return {
        'health_score':       health_score,
        'spot_df':            spot_df,
        'options_df':         options_df,
        'fast_df':            fast_df,
        'token_ages':         token_ages,
        'iv_df':              iv_df,
        'spot_today_df':      spot_today_df,
        'spot_yesterday_df':  spot_yesterday_df,
        'oi_hist_df':         oi_hist_df,
        'futures_df':         futures_df,
    }


def load_futures(db_path: str, lookback_mins: int = 30) -> 'pd.DataFrame':
    """
    Load Nifty futures candles.
    Returns empty DataFrame if table not yet present
    (engine not yet updated with futures subscription).
    """
    import sqlite3
    import time
    import pandas as pd
    try:
        conn   = sqlite3.connect(db_path)
        cutoff = int(time.time()) - (lookback_mins * 60)
        df = pd.read_sql(
            f"SELECT timestamp, close, volume FROM futures_candles "
            f"WHERE timestamp >= {cutoff} ORDER BY timestamp ASC", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()
