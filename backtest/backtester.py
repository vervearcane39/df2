"""
BACKTEST LAYER — backtester.py  (Phase 9.2 — Full Real Replay)

Issue 1 Fix: Runs the REAL orchestrator.run() on each historical snapshot.
    No more proxy signals. Trade signals come from the exact same code
    that generates live signals.

Issue 3 Fix: BS PnL with proper greeks per-entry
    dOption ≈ delta*dSpot + 0.5*gamma*dSpot² + vega*dIV - theta*dt

How real replay works:
    1. Load all historical minute snapshots from the DB.
    2. For each minute, reconstruct the `raw` dict that orchestrator.run() expects.
    3. Patch st.session_state with BacktestSession (no Streamlit needed).
    4. Call orchestrator.run() — real logic, real signals.
    5. Extract trade_allowed, directional_bias, quality, greeks.
    6. Simulate entry / exit / PnL using real greeks.

Usage:
    python -m backtest.backtester
    or:  from backtest.backtester import run_backtest
"""
import sqlite3
import math
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional

# Real orchestrator (will be patched to avoid Streamlit)
from dashboard.orchestrator import run as orchestrator_run
from backtest.session_mock  import BacktestSession, OrchestratorPatch


# ── Option PnL (real greeks from entry snapshot) ─────────────────────────────

def option_pnl(delta: float, gamma: float, vega: float, theta: float,
               d_spot: float, d_iv: float, dt_years: float) -> float:
    """
    Black-Scholes second-order approximation.
    d_spot is signed: positive for calls going up, negative for puts going up.
    """
    return (delta * d_spot
            + 0.5  * gamma * (d_spot ** 2)
            + vega  * d_iv
            - theta * dt_years)


# ── Historical snapshot builder ───────────────────────────────────────────────

def _build_raw_snapshot(ts: int, lookback_mins: int,
                        health_df: pd.DataFrame,
                        candles_df: pd.DataFrame,
                        oi_hist_df: pd.DataFrame) -> Optional[dict]:
    """
    Build the `raw` dict that orchestrator expects, using only historical data
    up to timestamp `ts`.  Mirrors db_loader.load_all() structure exactly.
    """
    cutoff    = ts - (lookback_mins * 60)
    opt_60    = ts - (65 * 60)         # +5 min buffer for flow window
    spot_cut  = ts - ((lookback_mins + 60) * 60)

    spot_df   = health_df[health_df['timestamp'] <= ts].copy()
    if spot_df.empty:
        return None
    spot_df   = spot_df[spot_df['timestamp'] >= spot_cut]

    options_df = candles_df[
        (candles_df['timestamp'] >= cutoff) &
        (candles_df['timestamp'] <= ts)
    ].copy()
    if options_df.empty:
        return None

    # Health score: latest row
    hs_row    = health_df[health_df['timestamp'] <= ts]
    health_score = float(hs_row.iloc[-1]['health_pct']) if not hs_row.empty else 0.0

    # OI history: last hour up to ts
    oi_win = oi_hist_df[(oi_hist_df['timestamp'] >= ts - 3600) &
                        (oi_hist_df['timestamp'] <= ts)].copy()

    # Spot today (from session start)
    today_ts = int(datetime.fromtimestamp(ts).replace(
        hour=9, minute=15, second=0, microsecond=0).timestamp())
    spot_today = health_df[
        (health_df['timestamp'] >= today_ts) &
        (health_df['timestamp'] <= ts)
    ].copy()
    if not spot_today.empty:
        spot_today['total_option_volume'] = 1.0

    # Yesterday: crude — use data before today_ts
    spot_yesterday = health_df[
        health_df['timestamp'] < today_ts
    ].tail(375).copy()   # ~1 session worth

    # IV df: empty for backtest (IV percentile not critical for signal)
    iv_df = pd.DataFrame()

    return {
        'health_score':      health_score,
        'spot_df':           spot_df,
        'options_df':        options_df,
        'fast_df':           pd.DataFrame(),   # no 15s candles in history
        'token_ages':        {},
        'iv_df':             iv_df,
        'spot_today_df':     spot_today,
        'spot_yesterday_df': spot_yesterday,
        'oi_hist_df':        oi_win,
        'futures_df':        pd.DataFrame(),   # futures optional
    }


# ── Snapshot loader ───────────────────────────────────────────────────────────

def _load_history(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        health  = pd.read_sql(
            "SELECT timestamp, spot_price, health_pct FROM system_health "
            "ORDER BY timestamp ASC", conn)
        candles = pd.read_sql(
            "SELECT timestamp, token, strike, type, close, oi, volume "
            "FROM candles ORDER BY timestamp ASC", conn)
        try:
            oi_hist = pd.read_sql(
                "SELECT timestamp, strike, type, oi FROM strike_oi_history "
                "ORDER BY timestamp ASC", conn)
        except Exception:
            oi_hist = pd.DataFrame()
    finally:
        conn.close()
    return health, candles, oi_hist


# ── Main backtest ─────────────────────────────────────────────────────────────

def run_backtest(db_path:           str  = "market_data.db",
                 lookback_mins:     int  = 15,
                 quality_threshold: int  = 60,
                 hold_mins:         int  = 15,
                 max_snapshots:     int  = 999_999,
                 verbose:           bool = False) -> Dict:
    """
    Real-replay backtest using the live orchestrator.

    For each historical minute:
      - Reconstructs the raw data snapshot
      - Runs orchestrator.run() with a mock Streamlit session
      - Extracts trade_allowed, direction, quality, greeks
      - Simulates ATM option trade with proper BS PnL

    Returns metrics dict with full trade log.
    """
    health_df, candles_df, oi_hist_df = _load_history(db_path)
    if health_df.empty or candles_df.empty:
        return {'error': 'No historical data found in database.'}

    all_ts  = sorted(candles_df['timestamp'].unique())
    all_ts  = all_ts[:max_snapshots]
    n_ts    = len(all_ts)

    if verbose:
        print(f"Replaying {n_ts} snapshots, lookback={lookback_mins}m, "
              f"quality>={quality_threshold}, hold={hold_mins}m")

    trades      : List[dict] = []
    in_trade    = False
    entry       : dict = {}
    skipped     = 0
    errors      = 0
    session     = BacktestSession()

    for i, ts in enumerate(all_ts):
        if i < lookback_mins + 2:
            skipped += 1
            continue

        raw = _build_raw_snapshot(ts, lookback_mins, health_df, candles_df, oi_hist_df)
        if raw is None:
            skipped += 1
            continue

        # ── Run real orchestrator with mocked session_state ──────────────
        try:
            with OrchestratorPatch(session):
                _, m = orchestrator_run(raw, lookback_mins, db_path)
        except Exception as e:
            errors += 1
            if verbose:
                print(f"  ⚠️  {datetime.fromtimestamp(ts).strftime('%H:%M')} error: {e}")
            continue

        if not m:
            skipped += 1
            continue

        spot       = m['spot']
        trade_ok   = m['trade_allowed']
        quality    = m['quality']
        bias       = m['directional_bias']
        atm_iv     = m['atm_iv']
        straddle   = m['straddle']
        dte        = m.get('dte_days', 1.0) / 365.0
        prob_win   = m.get('prob_win_pct', 50) / 100.0

        if verbose and i % 50 == 0:
            print(f"  {datetime.fromtimestamp(ts).strftime('%H:%M')} "
                  f"Q:{quality} ok:{trade_ok} bias:{bias[:20]}")

        # ── Entry ─────────────────────────────────────────────────────────
        if not in_trade and trade_ok and quality >= quality_threshold:
            # Determine direction from real bias
            if 'CALL' in bias or 'CALL LEAN' in bias:
                direction = 'call'
            elif 'PUT' in bias or 'PUT LEAN' in bias:
                direction = 'put'
            else:
                skipped += 1
                continue   # no clear edge — skip

            # ATM BS greeks
            from features.greeks import bs_delta, bs_gamma, bs_vega, RISK_FREE_RATE
            import math as _math
            iv_safe = max(atm_iv, 0.05)
            dte_safe = max(dte, 1 / (252 * 375))
            atm  = int(round(spot / 50) * 50)

            delta_v = bs_delta(spot, atm, dte_safe, RISK_FREE_RATE, iv_safe,
                               'CE' if direction == 'call' else 'PE')
            gamma_v = bs_gamma(spot, atm, dte_safe, RISK_FREE_RATE, iv_safe)
            from features.greeks import bs_vega as bsv
            vega_v  = bsv(spot, atm, dte_safe, RISK_FREE_RATE, iv_safe)
            # Rough theta (Nifty: 375 min/day, 252 days/yr)
            theta_v = -(iv_safe * spot * _math.sqrt(dte_safe) /
                        (2 * _math.sqrt(2 * _math.pi))) / (252 * 375)

            raw_prem   = max(straddle / 2.0, 5.0)
            entry_prem = raw_prem + 1.5   # entry half-spread

            entry = {
                'ts':         ts,
                'time':       datetime.fromtimestamp(ts).strftime('%H:%M'),
                'direction':  direction,
                'spot':       spot,
                'iv':         atm_iv,
                'premium':    round(entry_prem, 1),
                'delta':      abs(delta_v),
                'gamma':      gamma_v,
                'vega':       vega_v,
                'theta':      theta_v,
                'quality':    quality,
                'prob_win':   round(prob_win, 3),
                'bias':       bias[:40],
                'regime':     m.get('regime_label', ''),
            }
            in_trade = True

        elif in_trade:
            # ── Exit check ────────────────────────────────────────────────
            elapsed_min = (ts - entry['ts']) / 60.0
            d_spot      = spot - entry['spot']
            # IV change
            d_iv        = atm_iv - entry['iv']
            dt_yrs      = elapsed_min / (252 * 375)

            # Signed d_spot: calls profit from upside, puts from downside
            signed_d = d_spot if entry['direction'] == 'call' else -d_spot

            pnl_pts  = option_pnl(entry['delta'], entry['gamma'], entry['vega'],
                                   entry['theta'], signed_d, d_iv, dt_yrs)
            pnl_pct  = pnl_pts / entry['premium'] * 100.0

            exit_reason = None
            if   elapsed_min >= hold_mins: exit_reason = 'time'
            elif pnl_pct     <=      -50:  exit_reason = 'stop'
            elif pnl_pct     >=      100:  exit_reason = 'target'

            # Early exit: orchestrator says sit out during trade
            if not trade_ok and m.get('exit_warn'):
                exit_reason = 'signal_exit'

            if exit_reason:
                trades.append({
                    'entry_time':  entry['time'],
                    'exit_time':   datetime.fromtimestamp(ts).strftime('%H:%M'),
                    'direction':   entry['direction'],
                    'entry_prem':  entry['premium'],
                    'pnl_pts':     round(pnl_pts, 2),
                    'pnl_pct':     round(pnl_pct, 2),
                    'exit_reason': exit_reason,
                    'hold_mins':   round(elapsed_min, 1),
                    'quality':     entry['quality'],
                    'delta':       round(entry['delta'], 3),
                    'prob_win':    entry['prob_win'],
                    'regime':      entry['regime'],
                    'bias':        entry['bias'],
                })
                in_trade = False
                session.reset()   # fresh state between trades

    if not trades:
        return {'trades': [], 'n': 0, 'win_rate': 0.0, 'avg_return': 0.0,
                'max_drawdown': 0.0, 'total_pnl': 0.0,
                'skipped': skipped, 'errors': errors,
                'note': (f'No qualifying trades found. '
                         f'Snapshots: {n_ts}, skipped: {skipped}, errors: {errors}. '
                         f'Lower quality threshold or check data.')}

    df         = pd.DataFrame(trades)
    wins       = int((df['pnl_pct'] > 0).sum())
    win_rate   = round(wins / len(df) * 100, 1)
    avg_ret    = round(float(df['pnl_pct'].mean()), 2)
    cumul      = df['pnl_pct'].cumsum()
    max_dd     = round(float((cumul - cumul.cummax()).min()), 2)
    by_exit    = df.groupby('exit_reason')['pnl_pct'].agg(count='count', mean='mean').to_dict('index')
    by_dir     = df.groupby('direction')['pnl_pct'].agg(count='count', mean='mean').to_dict('index')
    by_regime  = df.groupby('regime')['pnl_pct'].agg(count='count', mean='mean').to_dict('index')

    return {
        'trades':       df.to_dict('records'),
        'n':            len(df),
        'win_rate':     win_rate,
        'avg_return':   avg_ret,
        'max_drawdown': max_dd,
        'total_pnl':    round(float(df['pnl_pct'].sum()), 2),
        'avg_hold':     round(float(df['hold_mins'].mean()), 1),
        'avg_delta':    round(float(df['delta'].mean()), 3),
        'by_exit':      by_exit,
        'by_dir':       by_dir,
        'by_regime':    by_regime,
        'skipped':      skipped,
        'errors':       errors,
        'snapshots':    n_ts,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    r = run_backtest(verbose=True)
    print(f"\n{'='*50}")
    print("REAL REPLAY BACKTEST — Full Orchestrator")
    print(f"{'='*50}")
    if 'error' in r:
        print(r['error'])
    elif r['n'] == 0:
        print(r.get('note', 'No trades.'))
    else:
        print(f"Snapshots:    {r['snapshots']}  (skipped: {r['skipped']}, errors: {r['errors']})")
        print(f"Trades:       {r['n']}")
        print(f"Win Rate:     {r['win_rate']}%")
        print(f"Avg Return:   {r['avg_return']}%")
        print(f"Max Drawdown: {r['max_drawdown']}%")
        print(f"Total PnL:    {r['total_pnl']}%")
        print(f"Avg Hold:     {r['avg_hold']} mins")
        print(f"\nBy Direction: {r['by_dir']}")
        print(f"By Exit:      {r['by_exit']}")
        print(f"By Regime:    {r['by_regime']}")
