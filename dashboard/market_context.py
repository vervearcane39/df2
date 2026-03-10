"""
DASHBOARD LAYER — market_context.py
Responsibility: Build the raw market snapshot dict from all feature/market layers.
No signal logic.  No risk decisions.  Just facts.
"""
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, time as dt_time

from features.greeks        import apply_greeks, atm_iv_approx
from features.flow_features import (build_merged, add_weighted_oi,
                                    add_intent_column, add_vol_surge,
                                    compute_flow_ratio_v2)
from features.volatility    import (compute_iv_regime, compute_realized_vol,
                                    compute_iv_rv_label, compute_25d_skew,
                                    expected_move)
from features.structure     import compute_vwap, compute_market_structure, REGIME_META, get_time_regime
from features.futures       import compute_futures_flow, futures_signal_for_scoring
from features.delta_flow    import compute_delta_flow
from market.gamma           import compute_gex, compute_gamma_flip_signal
from market.dealer_position import (compute_dealer_delta, compute_max_pain_and_wall,
                                    compute_strike_magnetism)
from market.liquidity       import compute_oi_liquidity_profile, signed_futures_volume
from market.liquidity_map   import compute_liquidity_map
from market.hedge_pressure   import compute_hedge_pressure
from market.gamma_convexity  import compute_gamma_convexity
from market.cascade         import compute_hedge_flow, detect_cascade, compute_delta_imbalance
from market.migration       import compute_strike_migration
from market.market_phase    import classify_market_phase            # Upgrade 1: Phase engine
from signals.compression    import detect_compression, detect_volume_shock
from signals.regime_learning import push_oi_flow, push_velocity, oi_flow_is_strong
from signals.skew_dynamics  import compute_skew_dynamics
from signals.stability_filter import apply_stability_filter         # Upgrade 5: Noise filter
from signals.flow_memory      import get_or_create_flow_memory       # Final upgrade: persistence
from data.db_utils          import write_iv_log
from market.event_awareness import get_event_risk, estimate_global_risk
from signals.calibration    import SignalLogger
from dashboard.state        import push_history
from dashboard.cache        import get_or_compute, TTL
import time as _time


def _dte_years() -> float:
    today    = datetime.now()
    days_tue = (1 - today.weekday()) % 7
    if days_tue == 0 and today.time() > dt_time(15, 30):
        days_tue = 7
    expiry = (today + timedelta(days=days_tue)).replace(
        hour=15, minute=30, second=0, microsecond=0)
    return max((expiry - today).total_seconds(), 300) / (365.25 * 24 * 3600)

def _is_expiry() -> bool:
    return datetime.now().weekday() == 1


def build_market_context(raw: dict, lookback_mins: int, db_path: str) -> dict:
    """
    Gathers all observable market facts.
    Returns ctx dict — consumed by signal_engine and risk_engine.
    """
    spot_df           = raw['spot_df']
    options_df        = raw['options_df']
    fast_df           = raw['fast_df']
    token_ages        = raw['token_ages']
    iv_df             = raw['iv_df']
    spot_today_df     = raw['spot_today_df']
    spot_yesterday_df = raw['spot_yesterday_df']
    oi_hist_df        = raw['oi_hist_df']
    health_score      = raw['health_score']
    futures_df        = raw.get('futures_df', pd.DataFrame())

    # ── Spot mechanics ────────────────────────────────────────────────────
    spot       = float(spot_df.iloc[-1]['spot_price'])
    rolling    = spot_df['spot_price'].tail(15)
    velocity   = float(rolling.std()) if len(rolling) > 1 else 1.0
    raw_slope  = (float(rolling.iloc[-1]) - float(rolling.iloc[0])) if len(rolling) > 1 else 0.0
    norm_slope = raw_slope / (velocity + 1)
    # EMA smoothing — prevents single-tick slope flips from cascading into signals
    _slope_ema = st.session_state.get('slope_ema', norm_slope)
    _slope_ema = 0.3 * norm_slope + 0.7 * _slope_ema
    st.session_state['slope_ema'] = _slope_ema
    norm_slope = _slope_ema
    rolling_mean = float(rolling.mean())
    prev_spot  = float(spot_df.iloc[-2]['spot_price']) if len(spot_df) >= 2 else None

    push_history('spot_range_history', float(rolling.max() - rolling.min()), 20)
    push_velocity(velocity)

    # ── Expiry / DTE ──────────────────────────────────────────────────────
    dte        = _dte_years()
    is_expiry  = _is_expiry()
    atm_strike = int(round(spot / 50) * 50)
    regime     = get_time_regime()
    rmeta      = REGIME_META[regime]

    # ── VWAP / Structure ──────────────────────────────────────────────────
    vwap_data = compute_vwap(spot_today_df, spot)
    struct    = compute_market_structure(spot_today_df, spot_yesterday_df, spot)
    acc_ref   = vwap_data['vwap'] if vwap_data['reliable'] else rolling_mean
    acc_dist  = abs(spot - acc_ref)
    dist_thr  = max(velocity * 2, 10)
    spot_trend = "BULLISH" if spot >= acc_ref else "BEARISH"

    # ── Options chain ─────────────────────────────────────────────────────
    merged = build_merged(options_df, lookback_mins, token_ages, spot)
    if merged.empty:
        return {}

    actual_lb  = merged.attrs.get('actual_lb_mins', lookback_mins)
    drift_pct  = (actual_lb - lookback_mins) / max(lookback_mins, 1) * 100
    lb_warning = (
        f"⚠️ LOOKBACK DRIFT: requested {lookback_mins}m, "
        f"actual {actual_lb:.0f}m ({drift_pct:+.0f}%) — engine may have been paused."
    ) if drift_pct > 30 else None

    # ── ATM IV (needed for greeks) ────────────────────────────────────────
    atm_ce   = merged[(merged['strike_now'] == atm_strike) & (merged['type_now'] == 'CE')]
    atm_pe   = merged[(merged['strike_now'] == atm_strike) & (merged['type_now'] == 'PE')]
    atm_ce_p = float(atm_ce.iloc[0]['close_now']) if not atm_ce.empty else 0.0
    atm_pe_p = float(atm_pe.iloc[0]['close_now']) if not atm_pe.empty else 0.0
    straddle  = atm_ce_p + atm_pe_p
    atm_iv    = atm_iv_approx(straddle, spot, dte) if straddle > 0 else 0.0

    # ── Greeks ────────────────────────────────────────────────────────────
    merged = apply_greeks(merged, spot, dte, atm_iv)

    # ── IV regime ─────────────────────────────────────────────────────────
    if atm_iv > 0:
        push_history('iv_history', atm_iv, 20)
        write_iv_log(db_path, atm_iv, straddle)
    iv_reg  = compute_iv_regime(atm_iv, st.session_state.iv_history, iv_df)
    iv_chg  = iv_reg['iv_change_rate'] / 100.0
    rv      = compute_realized_vol(spot_df)
    iv_rv   = compute_iv_rv_label(atm_iv, rv)
    exp_move = expected_move(spot, atm_iv, dte)

    # ── Spot change vs lookback ────────────────────────────────────────────
    past_ts     = merged.attrs.get('past_ts', 0)
    past_snap   = spot_df[spot_df['timestamp'] <= past_ts]
    past_spot_v = float(past_snap.iloc[-1]['spot_price']) if not past_snap.empty else spot
    spot_chg    = spot - past_spot_v

    # ── Flow features ─────────────────────────────────────────────────────
    merged    = add_weighted_oi(merged)
    merged    = add_intent_column(merged, spot_chg, iv_chg)
    flow_info = compute_flow_ratio_v2(merged, spot)
    merged    = add_vol_surge(merged)

    # ── Price-OI classification column (needed by hedge_pressure + hero-zero) ─
    # P5 vectorised: replace apply(classify_oi_flow) with np.select
    # OI_FLOW_LABELS maps (price_dir, oi_dir) → label. Replicate with np.select.
    import numpy as _np
    _pc  = merged['price_chg'].values.astype(float)
    _oi  = merged['oi_chg'].values.astype(float)
    _p_up   = _pc >  0.5;  _p_dn = _pc < -0.5;  _p_fl = ~(_p_up | _p_dn)
    _o_up   = _oi >  500;  _o_dn = _oi < -500;  _o_fl = ~(_o_up | _o_dn)
    merged['oi_flow_type'] = _np.select(
        [_p_up & _o_up, _p_dn & _o_up, _p_up & _o_dn, _p_dn & _o_dn,
         _p_fl & _o_up, _p_up & _o_fl, _p_dn & _o_fl, _p_fl & _o_dn],
        ['LONG_BUILD', 'SHORT_BUILD', 'SHORT_COVER', 'LONG_UNWIND',
         'STALE_OI',   'PRICE_ONLY',  'PRICE_ONLY',  'STALE_OI'],
        default='NEUTRAL')

    w_ce      = float(merged[merged['type_now'] == 'CE']['w_oi_chg'].sum())
    w_pe      = float(merged[merged['type_now'] == 'PE']['w_oi_chg'].sum())
    net_w     = w_pe - w_ce
    push_oi_flow(abs(net_w))

    atm_mom   = float(merged[merged['dist'] < 200]['vol_mom'].abs().sum())
    liq_thresh = max(merged['vol_mom'].abs().sum() * 0.05, 2000)

    TICK_FLOOR = {'OPEN_DISC': 3, 'INST_FLOW': 8, 'VACUUM': 2, 'TREND_EXP': 6, 'EXPIRY_HDGE': 4}
    atm_tick  = float(merged[merged['dist'] < 200]['tick_now'].mean()) if 'tick_now' in merged.columns else 0.0
    tick_ok   = atm_tick >= TICK_FLOOR.get(regime, 5)
    liq_risk  = (atm_mom < liq_thresh) or (not tick_ok and atm_tick > 0)

    # ── GEX — dynamic radius + per-strike IV + root-solver flip ─────────
    # Improvement 2: pass expected_move so radius auto-expands on wide days
    exp_move_pts = expected_move(spot, atm_iv, dte)
    gex_r = get_or_compute(
        'gex',
        lambda: compute_gex(merged, spot, dte, atm_iv,
                            atm_only_radius=400, expected_move=exp_move_pts),
        TTL['gex'], current_spot=spot, velocity=velocity)

    near_gex_wall = abs(spot - gex_r['gex_wall']) <= 75
    gex_wall_pin  = near_gex_wall and gex_r['net_gex'] > 3.0

    # ── Dealer delta — price-OI classification ────────────────────────────
    # Improvement 1: dealer sign derived from price×OI direction, not OI level
    dealer_r = get_or_compute(
        'dealer', lambda: compute_dealer_delta(merged),
        TTL['dealer'], current_spot=spot, velocity=velocity)

    # ── 25d skew ─────────────────────────────────────────────────────────
    skew_25d, iv_25c, iv_25p = compute_25d_skew(merged, spot, dte, atm_iv)

    # ── Max pain + OI wall (cached 30s) ───────────────────────────────────
    now_raw = options_df[options_df['timestamp'] == options_df['timestamp'].max()][['strike','type','oi']].copy()
    max_pain, gwall_oi = get_or_compute(
        'max_pain', lambda: compute_max_pain_and_wall(now_raw, spot),
        TTL['max_pain'], current_spot=spot, velocity=velocity)

    # ── Magnetism (cached 15s) ─────────────────────────────────────────────
    mag_r = get_or_compute(
        'magnet', lambda: compute_strike_magnetism(merged, spot),
        TTL['magnet'], current_spot=spot, velocity=velocity)

    # ── Concentration ────────────────────────────────────────────────────
    abs_w       = merged['w_oi_chg'].abs()
    conc_pct    = 0.0
    dom_strike  = atm_strike
    if abs_w.sum() > 0:
        by_s     = merged.groupby('strike_now')['w_oi_chg'].apply(lambda x: x.abs().sum())
        conc_pct = float(by_s.max() / abs_w.sum() * 100)
        dom_strike = int(by_s.idxmax())

    # ── Migration (cached 30s) ────────────────────────────────────────────
    _mig = get_or_compute(
        'migration', lambda: compute_strike_migration(oi_hist_df, spot),
        TTL['migration'], current_spot=spot, velocity=velocity)
    if isinstance(_mig, tuple):
        mig_strike, mig_dir = _mig
    else:
        mig_strike = _mig.get('strike', spot) if _mig else spot
        mig_dir    = _mig.get('direction', 'NEUTRAL') if _mig else 'NEUTRAL'
    migration_r = {'strike': mig_strike, 'direction': mig_dir}

    # ── Compression (ATR + straddle + range) ──────────────────────────────
    push_history('straddle_history', straddle, 15)
    vol_shock_msg, _ = detect_volume_shock(fast_df, spot)
    comp_r = detect_compression(st.session_state.straddle_history,
                                st.session_state.spot_range_history, spot_df)

    # ── Futures flow — signed VWAP-deviation volume ────────────────────────
    fut_r    = compute_futures_flow(futures_df)
    sig_fut  = signed_futures_volume(futures_df)
    fut_sig  = (sig_fut['norm_signal'] if sig_fut['available']
                else futures_signal_for_scoring(fut_r['futures_signal']))

    # ── Net option delta flow ──────────────────────────────────────────────
    delta_flow_r = compute_delta_flow(merged, spot, radius=500.0)

    # ── Liquidity vacuum ───────────────────────────────────────────────────
    liq_vac_r = compute_oi_liquidity_profile(merged, spot, radius=600)

    # ── Upgrade 3: Full liquidity density map ─────────────────────────────
    liq_map_r = compute_liquidity_map(merged, spot, radius=800)

    # ── Hero-Zero Upgrade 1: Hedge pressure map ───────────────────────────
    hedge_pres_r = compute_hedge_pressure(merged, spot, dte, atm_iv, velocity, radius=600)

    # ── dGamma/dSpot: Dealer gamma convexity (final layer) ────────────────
    gamma_conv_r = compute_gamma_convexity(merged, spot, dte, atm_iv, radius=400)   # P5: perf fix

    # ── Upgrade 1: Market Phase Engine ───────────────────────────────────
    prev_phase   = st.session_state.get('current_market_phase', None)
    cascade_fsm  = st.session_state.get('cascade_fsm_state_cache', 'IDLE')
    # Compute STRONG_OI for phase engine (same logic as signal_engine)
    import numpy as _np2
    _oi_h = list(st.session_state.get('oi_flow_history', []))
    _BASE_OI = 500000 if is_expiry else 700000
    _STRONG_OI_p = (max(float(_np2.percentile(_oi_h, 80)), _BASE_OI * 0.40)
                    if len(_oi_h) >= 10 else float(_BASE_OI))
    phase_r = classify_market_phase(
        is_expiry        = is_expiry,
        velocity         = velocity,
        norm_slope       = norm_slope,
        acceptance_dist  = acc_dist,
        dist_threshold   = dist_thr,
        net_gex          = gex_r['net_gex'],
        gex_mode         = gex_r['gex_mode'],
        net_w_oi         = net_w,
        STRONG_OI        = _STRONG_OI_p,
        stagnation_counter = st.session_state.get('stagnation_counter', 0),
        max_pain         = max_pain,
        spot             = spot,
        straddle         = straddle,
        straddle_history = list(st.session_state.get('straddle_history', [])),
        is_compressed    = comp_r.get('is_compressed', False),
        in_vacuum        = liq_vac_r.get('in_vacuum', False),
        vacuum_severity  = liq_vac_r.get('vacuum_severity', 0.0),
        migration_r      = migration_r,
        cascade_fsm      = cascade_fsm,
        prev_phase       = prev_phase,
        velocity_history = list(st.session_state.get('velocity_history', [])),  # P3/file fix
    )
    st.session_state['current_market_phase'] = phase_r['phase']

    # ── P7: Event awareness (expiry calendar + macro dates) ──────────────
    custom_events = st.session_state.get('custom_events', [])
    event_r = get_event_risk(custom_events=custom_events)
    # Auto-read VIX from DB (engine writes it every minute from live feed)
    india_vix = 0.0
    prev_vix  = 0.0
    try:
        import sqlite3 as _sq
        _vc = _sq.connect(db_path)
        _rows = _vc.execute(
            "SELECT india_vix FROM system_health ORDER BY timestamp DESC LIMIT 2"
        ).fetchall()
        _vc.close()
        if _rows and _rows[0][0]:
            india_vix = float(_rows[0][0])
        if len(_rows) > 1 and _rows[1][0]:
            prev_vix = float(_rows[1][0])
    except Exception:
        india_vix = float(st.session_state.get('india_vix', 0.0))
        prev_vix  = float(st.session_state.get('prev_india_vix', 0.0))
    sgx_gap       = float(st.session_state.get('sgx_gap_pct', 0.0))
    global_risk_r = estimate_global_risk(india_vix, prev_vix, sgx_gap)

    # ── P6: Signal calibration — close out old logged rows ───────────────
    if not st.session_state.get('_signal_logger'):
        st.session_state['_signal_logger'] = SignalLogger(db_path)
    st.session_state['_signal_logger'].close_outcomes(spot)

    # ── Upgrade 5: Stability filter (key binary signals) ──────────────────
    stability_results = apply_stability_filter(st.session_state, {
        'cascade':    (cascade_fsm in ('CONFIRMED', 'FIRING'), 'cascade'),
        'vacuum':     (liq_vac_r.get('in_vacuum', False), 'vacuum'),
        'compression':(comp_r.get('is_compressed', False), 'compression'),
    })

    # ── Final Upgrade: Flow Persistence Engine ────────────────────────────
    # Called AFTER market_context, BEFORE signal_engine.
    # Tracks how long each structural signal has been active.
    # Computes pressure_score (stored energy) and persist_mult (amplifier).
    _fm = get_or_create_flow_memory(st.session_state)
    persistence_r = _fm.update(
        delta_flow_norm = delta_flow_r.get('norm_signal', 0.0),
        net_gex         = gex_r['net_gex'],
        in_vacuum       = liq_vac_r.get('in_vacuum', False),
        vacuum_severity = liq_vac_r.get('vacuum_severity', 0.0),
        is_compressed   = comp_r.get('is_compressed', False),
        cascade_fsm     = cascade_fsm,
        dealer_delta_M  = dealer_r['dealer_delta_M'],
        refresh_secs    = 2.0,   # matches @st.fragment(run_every=2)
    )

    # ── Upgrade 4: Skew dynamics (velocity tracking) ──────────────────────
    skew_25d_r = compute_skew_dynamics(
        skew_25d, iv_25c, iv_25p, timestamp=_time.time())

    # ── Gamma flip — root-solved level ────────────────────────────────────
    gf_msg, gf_crossed = compute_gamma_flip_signal(spot, gex_r['gex_flip_zone'],
                                                    gex_r['net_gex'], prev_spot)

    # ── Hedge flow ────────────────────────────────────────────────────────
    hedge_r = compute_hedge_flow(
        gex_r['net_gex'], spot, raw_slope, dealer_r['dealer_delta_M'], velocity)

    has_atm_surge = not merged[
        (merged['dist'] <= 150) &
        (merged['vol_surge'].str.contains('SURGE', na=False))].empty

    return {
        # Raw
        'merged':       merged,
        'options_df':   options_df,
        'spot_df':      spot_df,          # needed by spot_acceleration in hero_zero
        'spot':         spot,
        'health_score': health_score,
        'straddle':     straddle,
        'atm_iv':       atm_iv,
        'atm_strike':   atm_strike,
        'dte':          dte,
        'is_expiry':    is_expiry,
        'regime':       regime,
        'rmeta':        rmeta,
        # Spot mechanics
        'rolling':      rolling,
        'rolling_mean': rolling_mean,
        'velocity':     velocity,
        'raw_slope':    raw_slope,
        'norm_slope':   norm_slope,
        'spot_trend':   spot_trend,
        'spot_chg':     spot_chg,
        'acc_dist':     acc_dist,
        'dist_thr':     dist_thr,
        'prev_spot':    prev_spot,
        # VWAP
        'vwap_data':    vwap_data,
        'struct':       struct,
        # IV
        'iv_reg':       iv_reg,
        'rv':           rv,
        'iv_rv':        iv_rv,
        'exp_move':     exp_move,
        'skew_25d':     skew_25d,
        'iv_25c':       iv_25c,
        'iv_25p':       iv_25p,
        # Flow
        'net_w':        net_w,
        'w_ce':         w_ce,
        'w_pe':         w_pe,
        'flow_info':    flow_info,
        'atm_mom':      atm_mom,
        'liq_thresh':   liq_thresh,
        'liq_risk':     liq_risk,
        'atm_tick':     atm_tick,
        # GEX
        'gex_r':          gex_r,
        'near_gex_wall':  near_gex_wall,
        'gex_wall_pin':   gex_wall_pin,
        # Dealer
        'dealer_r':       dealer_r,
        # Positioning
        'max_pain':       max_pain,
        'gwall_oi':       gwall_oi,
        'mag_r':          mag_r,
        'conc_pct':       conc_pct,
        'dom_strike':     dom_strike,
        'mig_strike':     mig_strike,
        'mig_dir':        mig_dir,
        # Signals
        'comp_r':         comp_r,
        'vol_shock_msg':  vol_shock_msg,
        'fut_r':          fut_r,
        'fut_sig':        fut_sig,
        'gf_msg':         gf_msg,
        'gf_crossed':     gf_crossed,
        'has_atm_surge':  has_atm_surge,
        'lb_warning':     lb_warning,
        # Improvement 3: Delta flow
        'delta_flow_r':   delta_flow_r,
        # Improvement 4: Liquidity vacuum
        'liq_vac_r':      liq_vac_r,
        # Improvement 5: Hedge flow
        'hedge_r':        hedge_r,
        # Signed futures volume
        'sig_fut_r':      sig_fut,
        # Expected move
        'exp_move_pts':   exp_move_pts,
        # Upgrade 3: Liquidity map
        'liq_map_r':      liq_map_r,
        # Upgrade 4: Skew dynamics
        'skew_dyn_r':     skew_25d_r,
        # Hero-Zero Upgrade 1: Hedge pressure
        'hedge_pres_r':   hedge_pres_r,
        # dGamma/dSpot: Gamma convexity
        'gamma_conv_r':   gamma_conv_r,
        # Upgrade 1: Market Phase Engine
        'phase_r':        phase_r,
        # Upgrade 5: Stability filtered signals
        'stable_cascade': stability_results.get('cascade', False),
        'stable_vacuum':  stability_results.get('vacuum', False),
        'stable_compress':stability_results.get('compression', False),
        # Final upgrade: Flow persistence
        'persistence_r':  persistence_r,
        # P7: Event awareness
        'event_r':        event_r,
        'global_risk_r':  global_risk_r,
    }