"""
FEATURE LAYER — flow_features.py
Responsibility: Compute raw flow metrics from the merged options DataFrame.
No market-state interpretation — that lives in market/.
"""
import numpy as np
import pandas as pd


FLOW_RATIO_AGGRESSIVE = 5.0    # Doc1 Fix 3 — threshold for aggressive buying
FLOW_RATIO_PASSIVE    = 1.0


def build_merged(options_df: pd.DataFrame, lookback_mins: int,
                 token_ages: dict, spot_hist: float) -> pd.DataFrame:
    """
    Join now-snapshot with lookback-snapshot.
    Returns merged df with oi_chg, price_chg, vol_mom.
    Uses INNER JOIN (ghost-OI guard).
    """
    df = options_df.sort_values(['token', 'timestamp'])
    latest_ts  = df['timestamp'].max()
    target_ts  = latest_ts - (lookback_mins * 60)
    all_ts     = df['timestamp'].unique()
    if len(all_ts) == 0:
        return pd.DataFrame()

    past_ts = all_ts[np.abs(all_ts - target_ts).argmin()]

    now_df = df[df['timestamp'] == latest_ts].set_index('token')
    old_df = df[df['timestamp'] == past_ts  ].set_index('token')
    merged = now_df.join(old_df, lsuffix='_now', rsuffix='_old', how='inner')

    # Ghost-OI guard: drop tokens whose first-seen is after lookback start
    if token_ages:
        lb_start = latest_ts - (lookback_mins * 60)
        valid    = {t for t, fs in token_ages.items() if fs <= lb_start}
        merged   = merged[~merged.index.isin(token_ages.keys()) | merged.index.isin(valid)]

    if merged.empty:
        return pd.DataFrame()

    merged['oi_chg']    = merged['oi_now']    - merged['oi_old']
    merged['price_chg'] = merged['close_now'] - merged['close_old']
    merged['vol_mom']   = merged['volume_now']  - merged['volume_old']
    merged['dist']      = abs(merged['strike_now'] - spot_hist)

    # Tick count
    merged['tick_now']  = merged.get('tick_count_now', pd.Series(0, index=merged.index))

    # Lookback drift
    actual_lb_mins    = (latest_ts - past_ts) / 60.0
    merged.attrs['latest_ts']      = int(latest_ts)
    merged.attrs['past_ts']        = int(past_ts)
    merged.attrs['actual_lb_mins'] = actual_lb_mins

    return merged


def add_weighted_oi(merged: pd.DataFrame) -> pd.DataFrame:
    """Adds w_oi_chg = oi_chg × |delta|."""
    merged = merged.copy()
    merged['w_oi_chg'] = merged['oi_chg'] * merged.get('weight', 0.5)
    return merged


def classify_intent(oi_chg, raw_price_chg, spot_chg, delta, vega_val,
                    iv_chg, vol_chg, oi_now):
    """
    Intent v3: delta + vega corrected.
    residual = price_chg − delta×spot_chg − vega×iv_chg
    Only residual reflects true order flow.
    """
    if abs(oi_chg) < 500:
        return "", "UNKNOWN"

    residual  = raw_price_chg - delta * spot_chg - vega_val * iv_chg
    voi       = abs(vol_chg) / max(abs(oi_now), 1)
    flow_type = "MIXED"
    if   voi > 0.30:                              flow_type = "BUYER"
    elif voi < 0.05 and abs(oi_chg) > 10000:     flow_type = "WRITER"
    if abs(vol_chg) > 8000 and abs(residual) < 0.8: flow_type = "MM_HEDGE"

    THRESH = 0.5
    if   oi_chg > 0 and residual >  THRESH: direction = "🟢 LB"
    elif oi_chg > 0 and residual < -THRESH: direction = "🔴 SB"
    elif oi_chg < 0 and residual >  THRESH: direction = "🟢 SC"
    elif oi_chg < 0 and residual < -THRESH: direction = "🔴 LU"
    else:
        return "⚪ Δ", flow_type

    suffix = "+" if abs(oi_chg) > 50000 else ("?" if abs(oi_chg) < 10000 else "")
    label  = f"({direction}{suffix})" if flow_type == "MM_HEDGE" else f"{direction}{suffix}"
    return label, flow_type


def add_intent_column(merged: pd.DataFrame, spot_chg: float,
                      iv_chg: float) -> pd.DataFrame:
    """
    Adds 'intent', 'flow_type' columns.
    P5 vectorised: replaces apply(_row, axis=1) with vectorised classify_intent.
    """
    merged = merged.copy()
    # classify_intent is scalar-only due to complex branching; use list comp
    # instead of df.apply — avoids pandas overhead of per-row Series creation
    results = [
        classify_intent(
            float(oi), float(pc), spot_chg,
            float(d), float(v), iv_chg,
            float(vm), float(on))
        for oi, pc, d, v, vm, on in zip(
            merged['oi_chg'].values,
            merged['price_chg'].values,
            merged.get('delta', pd.Series(0.5, index=merged.index)).values,
            merged.get('vega',  pd.Series(0.0, index=merged.index)).values,
            merged['vol_mom'].values,
            merged['oi_now'].values,
        )
    ]
    merged['intent']    = [r[0] for r in results]
    merged['flow_type'] = [r[1] for r in results]
    return merged


def compute_flow_ratio(merged: pd.DataFrame, spot: float) -> dict:
    """
    Fix 3 (Doc1): Flow Ratio = volume / |oi_chg|.
    Returns aggregate flow state.
    """
    near = merged[abs(merged['strike_now'] - spot) <= 500].copy()
    if near.empty:
        near = merged.copy()

    near['flow_ratio']  = near['vol_mom'].abs() / (near['oi_chg'].abs() + 1)
    aggressive_count    = int((near['flow_ratio'] > FLOW_RATIO_AGGRESSIVE).sum())
    passive_count       = int((near['flow_ratio'] < FLOW_RATIO_PASSIVE).sum())
    avg_ratio           = float(near['flow_ratio'].mean())

    if   aggressive_count > 5 and avg_ratio > 5.0: state = "🔥 AGGRESSIVE Option Buying"
    elif aggressive_count > 3:                      state = "📈 Active Buyer Flow"
    elif passive_count > len(near) * 0.6:           state = "🧊 Passive Writing"
    else:                                           state = "⚖️ Mixed Flow"

    return {'state': state, 'aggressive_count': aggressive_count,
            'passive_count': passive_count, 'avg_ratio': round(avg_ratio, 2)}


def add_vol_surge(merged: pd.DataFrame) -> pd.DataFrame:
    """Tags each row with vol surge tier."""
    merged = merged.copy()
    vol_abs = merged['vol_mom'].abs()
    if vol_abs.sum() == 0 or len(vol_abs) < 5:
        merged['vol_surge']    = ''
        merged['vol_oi_ratio'] = 0.0
        return merged
    p75 = vol_abs.quantile(0.75)
    p90 = vol_abs.quantile(0.90)
    merged['vol_surge'] = np.select(
        [vol_abs >= p90, vol_abs >= p75],
        ['🔥 SURGE',     '📈 BUILD'],
        default='')
    merged['vol_oi_ratio'] = np.where(
        merged['oi_now'] > 0,
        (vol_abs / merged['oi_now'].clip(lower=1) * 100).round(1), 0.0)
    return merged


def compute_flow_ratio_v2(merged: pd.DataFrame, spot: float) -> dict:
    """
    Flow ratio v2: uses regime-learned dynamic thresholds.
    Pushes observations to rolling distribution for future use.
    """
    from signals.regime_learning import (push_flow_ratio, flow_ratio_is_aggressive,
                                          flow_ratio_percentile, get_dynamic_flow_threshold)

    near = merged[abs(merged['strike_now'] - spot) <= 500].copy()
    if near.empty:
        near = merged.copy()

    near = near.copy()
    near['flow_ratio']  = near['vol_mom'].abs() / (near['oi_chg'].abs() + 1)
    avg_ratio = float(near['flow_ratio'].mean())
    push_flow_ratio(avg_ratio)

    dyn_thr           = get_dynamic_flow_threshold(fallback=5.0)
    fr_pct            = flow_ratio_percentile(avg_ratio)
    aggressive_count  = int((near['flow_ratio'] >= dyn_thr).sum())
    passive_count     = int((near['flow_ratio'] < 1.0).sum())

    if   fr_pct >= 90 and aggressive_count > 3: state = "🔥 AGGRESSIVE Option Buying"
    elif fr_pct >= 75 or aggressive_count > 3:  state = "📈 Active Buyer Flow"
    elif passive_count > len(near) * 0.6:       state = "🧊 Passive Writing"
    else:                                        state = "⚖️ Mixed Flow"

    return {'state': state, 'aggressive_count': aggressive_count,
            'passive_count': passive_count, 'avg_ratio': round(avg_ratio, 2),
            'flow_ratio_pct': round(fr_pct, 1), 'dyn_threshold': round(dyn_thr, 2)}