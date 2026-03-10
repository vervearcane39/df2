"""
DASHBOARD LAYER — signal_engine.py
Responsibility: Convert market context into scored signals.
No DB access.  No risk decisions.  No UI.
"""
import numpy as np
import pandas as pd
import streamlit as st

from signals.signal_score     import (compute_weighted_signal_score,
                                       compute_layered_quality, compute_directional_probability)
from market.control_dashboard import compute_market_control
from signals.directional_bias import compute_directional_bias
from signals.regime_learning  import get_dynamic_oi_threshold
from risk.trade_filter        import detect_trapped_sellers
from signals.probability_model import compute_breakeven_probability
from signals.stability_filter  import confirmed                    # Upgrade 5
from dashboard.state          import push_history
from features.structure       import REGIME_META
from market.cascade           import detect_cascade, compute_delta_imbalance
from market.dealer_regime     import classify_dealer_regime, build_hedge_scenarios
from market.move_probability  import compute_move_probability
from signals.hero_zero        import (theta_collapse_clock, oi_trap_density,
                                       spot_acceleration, gamma_wall_break,
                                       compute_hero_zero_score)


def build_signals(ctx: dict) -> dict:
    """
    Reads market context, returns all scored signals.
    """
    spot       = ctx['spot']
    net_w      = ctx['net_w']
    regime     = ctx['regime']
    rmeta      = ctx['rmeta']
    gex_r      = ctx['gex_r']
    dealer_r   = ctx['dealer_r']
    flow_info  = ctx['flow_info']
    iv_reg     = ctx['iv_reg']
    vwap_data  = ctx['vwap_data']
    struct     = ctx['struct']
    fut_sig    = ctx['fut_sig']
    atm_iv     = ctx['atm_iv']
    rv         = ctx['rv']
    straddle   = ctx['straddle']
    dte        = ctx['dte']
    merged     = ctx['merged']
    atm_strike = ctx['atm_strike']
    norm_slope = ctx['norm_slope']
    velocity   = ctx['velocity']
    raw_slope  = ctx['raw_slope']
    gf_crossed = ctx['gf_crossed']
    gex_wall_pin = ctx['gex_wall_pin']
    is_expiry  = ctx['is_expiry']
    max_pain   = ctx['max_pain']
    conc_pct   = ctx['conc_pct']
    dom_strike = ctx['dom_strike']
    mag_r      = ctx['mag_r']
    comp_r     = ctx['comp_r']
    health_score = ctx['health_score']

    # ── Dynamic STRONG_OI ────────────────────────────────────────────────
    BASE_OI    = 350_000 if is_expiry else 500_000
    oi_h       = st.session_state.oi_chg_history
    vel_h      = st.session_state.velocity_history
    slp_h      = st.session_state.slope_history
    STRONG_OI  = (max(float(np.percentile(oi_h,  80)), BASE_OI * 0.40) if len(oi_h)  >= 10 else float(BASE_OI))
    HIGH_VOL   = (max(float(np.percentile(vel_h, 75)), 4.0)             if len(vel_h) >= 10 else 10.0)
    SLOPE_TH   = (max(float(np.percentile(slp_h, 70)), 0.20)            if len(slp_h) >= 10 else 0.5)

    push_history('oi_chg_history', abs(net_w))
    push_history('velocity_history', velocity)
    push_history('slope_history', abs(norm_slope))

    # ── Spot trend ────────────────────────────────────────────────────────
    spot_trend = ctx['spot_trend']

    # ── Trapped sellers ───────────────────────────────────────────────────
    trapped_ce, trapped_pe = detect_trapped_sellers(merged, ctx['spot_chg'], atm_strike)

    # ── Probability model ─────────────────────────────────────────────────
    prob_ctx = compute_breakeven_probability(spot, straddle, atm_iv, rv, dte)

    # ── Improvement 5: Cascade + delta imbalance ──────────────────────────
    delta_flow_r  = ctx.get('delta_flow_r', {})
    hedge_r       = ctx.get('hedge_r', {})
    cascade_r     = detect_cascade(
        net_gex         = gex_r['net_gex'],
        hedge_flow_norm = hedge_r.get('hedge_flow_norm', 0.0),
        trapped_ce      = trapped_ce,
        trapped_pe      = trapped_pe,
        futures_signal  = ctx.get('fut_sig', 0.0),
        delta_flow_norm = delta_flow_r.get('norm_signal', 0.0),
        velocity        = ctx.get('velocity', 1.0),
        HIGH_VOL        = HIGH_VOL,
    )
    imbalance_r = compute_delta_imbalance(
        dealer_delta_M  = dealer_r['dealer_delta_M'],
        delta_flow_norm = delta_flow_r.get('norm_signal', 0.0),
        net_gex         = gex_r['net_gex'],
    )

    # ── Upgrade 1: Nonlinear dealer regime ───────────────────────────────
    liq_vac_r    = ctx.get('liq_vac_r', {})
    dealer_reg_r = classify_dealer_regime(
        net_gex          = gex_r['net_gex'],
        dealer_delta_M   = dealer_r['dealer_delta_M'],
        trapped_ce       = trapped_ce,
        trapped_pe       = trapped_pe,
        in_vacuum        = liq_vac_r.get('in_vacuum', False),
        is_cascade       = cascade_r['is_cascade'],
        delta_flow_norm  = delta_flow_r.get('norm_signal', 0.0),
        futures_signal   = ctx.get('fut_sig', 0.0),
    )

    # ── Upgrade 2: Hedge flow scenarios (+50 / -50) ───────────────────────
    hedge_scenarios = build_hedge_scenarios(merged, spot, gex_r['net_gex'])

    # ── Move Probability Engine (main upgrade) ────────────────────────────
    sens_r   = ctx.get('sens_r', {})
    move_r   = compute_move_probability(
        net_gex           = gex_r['net_gex'],
        is_cascade        = cascade_r['is_cascade'],
        cascade_strength  = cascade_r['strength'],
        cascade_type      = cascade_r['cascade_type'],
        in_vacuum         = liq_vac_r.get('in_vacuum', False),
        vacuum_severity   = liq_vac_r.get('vacuum_severity', 0.0),
        next_wall_above   = liq_vac_r.get('next_wall_above'),
        next_wall_below   = liq_vac_r.get('next_wall_below'),
        delta_flow_norm   = delta_flow_r.get('norm_signal', 0.0),
        dealer_delta_M    = dealer_r['dealer_delta_M'],
        dealer_regime     = dealer_reg_r['regime'],
        dealer_regime_mult= dealer_reg_r['multiplier'],
        futures_signal    = ctx.get('fut_sig', 0.0),
        futures_surge     = bool(ctx.get('fut_r', {}).get('vol_surge', False)),
        spot              = spot,
        explosion_zones   = sens_r.get('explosion_zones', []),
        trapped_ce        = trapped_ce,
        trapped_pe        = trapped_pe,
        spot_trend        = spot_trend,
        imbalance_ratio   = imbalance_r['imbalance_ratio'],
        gconv_score       = ctx.get('gamma_conv_r', {}).get('speed_score', 0.0),
        gconv_regime      = ctx.get('gamma_conv_r', {}).get('convexity_regime', 'DAMPENING'),
    )

    # ── Final Upgrade: Persistence amplifies move probability ─────────────
    # move_probability is a snapshot model. Persistence adds temporal context.
    # If pressure has been building for 12+ minutes, raw move_prob gets a boost.
    # Applied as a soft amplification (not multiplicative) to avoid runaway:
    #   persistent pressure_score 60 → adds up to +15 pts to explosion_prob
    _pers_r = ctx.get('persistence_r', {})
    if _pers_r:
        _ps    = float(_pers_r.get('pressure_score', 0.0))
        _pm    = float(_pers_r.get('persist_mult', 1.0))
        # Pressure bonus: only when pressure_score meaningful (> 20)
        _p_bonus = max(0.0, (_ps - 20.0) / 80.0 * 15.0)   # 0 at ps=20, +15 at ps=100
        _raw_mp  = float(move_r['explosion_prob'])
        _new_mp  = min(_raw_mp * min(_pm, 1.5) + _p_bonus, 100.0)  # cap mult at 1.5 here
        move_r = dict(move_r)    # copy before mutating
        move_r['explosion_prob'] = round(_new_mp, 1)
        move_r['pressure_bonus'] = round(_p_bonus, 1)
        move_r['persist_mult_applied'] = round(min(_pm, 1.5), 3)

    # ── Hero-Zero Engine ──────────────────────────────────────────────────
    hedge_pres_r = ctx.get('hedge_pres_r', {})
    spot_df_ctx  = ctx.get('spot_df')   # for acceleration computation

    theta_r  = theta_collapse_clock(is_expiry)
    trap_r   = oi_trap_density(merged, spot, trapped_ce, trapped_pe, net_w)
    accel_r  = spot_acceleration(velocity, spot_df_ctx)
    wall_r   = gamma_wall_break(
        spot, gex_r['gex_wall'], velocity,
        delta_flow_r.get('norm_signal', 0.0), gex_r['net_gex'],
        HIGH_VOL=HIGH_VOL)

    liq_vac_ctx = ctx.get('liq_vac_r', {})
    dir_bias_str = 'NEUTRAL'  # will be filled after dir_bias computation — placeholder here
    hz_r = compute_hero_zero_score(
        theta_r          = theta_r,
        trap_r           = trap_r,
        accel_r          = accel_r,
        wall_r           = wall_r,
        pressure_level   = hedge_pres_r.get('pressure_level', 'LOW'),
        pressure_vel     = hedge_pres_r.get('velocity_pressure', 0.0),
        in_vacuum        = liq_vac_ctx.get('in_vacuum', False),
        vacuum_severity  = liq_vac_ctx.get('vacuum_severity', 0.0),
        net_gex          = gex_r['net_gex'],
        move_prob        = move_r['explosion_prob'],
        delta_flow_norm  = delta_flow_r.get('norm_signal', 0.0),
        is_expiry        = is_expiry,
        spot             = spot,
        next_wall_above  = liq_vac_ctx.get('next_wall_above'),
        next_wall_below  = liq_vac_ctx.get('next_wall_below'),
        direction_bias   = spot_trend,
        gconv_score      = ctx.get('gamma_conv_r', {}).get('speed_score', 0.0),
        gconv_regime     = ctx.get('gamma_conv_r', {}).get('convexity_regime', 'DAMPENING'),
        persistence_r    = ctx.get('persistence_r'),   # Final upgrade: pressure accumulation
    )

    # ── Market control ────────────────────────────────────────────────────
    ctrl_r = compute_market_control(
        dealer_delta_M    = dealer_r['dealer_delta_M'],
        dealer_pressure   = dealer_r.get('pressure_level', 'LOW'),
        net_gex           = gex_r['net_gex'],
        gex_mode          = gex_r.get('gex_mode', ''),
        near_gex_wall     = bool(gex_r.get('near_gex_wall', False)),
        futures_signal    = fut_sig,
        futures_available = bool(ctx.get('futures_available', False)),
        net_w_oi          = net_w,
        STRONG_OI         = ctx.get('STRONG_OI', 500000),
        flow_state        = flow_info['state'],
        trapped_ce        = trapped_ce,
        trapped_pe        = trapped_pe,
        vwap_above        = ctx.get('vwap_above', True),
        spot_trend        = spot_trend,
        iv_pct            = iv_reg.get('iv_pct', 50.0),
        iv_direction      = iv_reg.get('iv_direction', 'NEUTRAL'),
    )

    # ── Get phase context ────────────────────────────────────────────────
    phase_r = ctx.get('phase_r', {})
    phase_mods = phase_r.get('modifiers', {})

    # Cache cascade FSM state for phase engine (read next cycle)
    st.session_state['cascade_fsm_state_cache'] = cascade_r.get('fsm_state', 'IDLE')

    # ── Weighted signal score (phase-aware) ───────────────────────────────
    ws_r = compute_weighted_signal_score(
        dealer_r['dealer_delta_M'], gex_r['net_gex'],
        flow_info['state'], flow_info['avg_ratio'],
        iv_reg['iv_pct'], iv_reg['iv_direction'],
        vwap_data['above_vwap'], struct, fut_sig,
        phase_modifiers=phase_mods)          # ← Upgrade 1 + 3

    # ── Layered quality (phase-capped) ────────────────────────────────────
    lq_r = compute_layered_quality(
        vwap_data['above_vwap'], vwap_data['reliable'],
        ctx['acc_dist'], ctx['dist_thr'], regime, struct,
        net_w, STRONG_OI, flow_info['state'], trapped_ce, trapped_pe,
        dealer_r['dealer_delta_M'], ctrl_r['score'], ctx['has_atm_surge'],
        iv_reg['iv_pct'], iv_reg['iv_direction'],
        gex_r['net_gex'], gex_r['gex_mode'], atm_iv, rv,
        st.session_state.compression_counter, st.session_state.stagnation_counter,
        max_pain, spot, conc_pct, mag_r['magnet_strike'], None, health_score,
        rmeta['q_mod'],
        futures_signal=fut_sig,
        phase_r=phase_r)                     # ← Upgrade 1: quality capped by phase

    push_history('conf_history', lq_r['quality'], 5)
    smoothed_q = int(sum(st.session_state.conf_history) / max(len(st.session_state.conf_history), 1))

    # ── P7: Event risk damp on quality ───────────────────────────────────
    _event_damp = ctx.get('event_r', {}).get('quality_damp', 0.0)
    if _event_damp > 0:
        smoothed_q = int(smoothed_q * (1.0 - _event_damp))

    # ── P6: Log signal snapshot for calibration ───────────────────────────
    # Only logs when move_prob > 40 and max once per minute
    try:
        _logger = st.session_state.get('_signal_logger')
        if _logger and hasattr(_logger, 'log'):
            _move_r = ctx.get('move_r_pre', {})  # pre-signal move_r if available
            _logger.log(
                move_prob = smoothed_q,
                direction = 'UP' if ctx['norm_slope'] > 0 else 'DOWN',
                spot      = ctx['spot'],
                phase     = ctx.get('phase_r', {}).get('phase', 'BALANCED'),
            )
    except Exception:
        pass

    # ── Mode ──────────────────────────────────────────────────────────────
    mode = "⚖️ RANGE"
    if velocity > HIGH_VOL:
        if   norm_slope >  SLOPE_TH: mode = "🚀 TRENDING UP"
        elif norm_slope < -SLOPE_TH: mode = "📉 TRENDING DOWN"
        else:                        mode = "🌪️ VOLATILE CHOP"
    if abs(net_w) > STRONG_OI and velocity < 5 and abs(norm_slope) < 0.2:
        mode = "🧽 ABSORPTION"
    atm_row = merged[merged['strike_now'] == atm_strike]
    if not atm_row.empty:
        ce_a = atm_row[atm_row['type_now'] == 'CE']['oi_chg'].sum()
        pe_a = atm_row[atm_row['type_now'] == 'PE']['oi_chg'].sum()
        if ce_a > 100000 and pe_a > 100000:
            r_ = pe_a / (ce_a + 1)
            mode = "📌 BULL PIN" if r_ > 1.5 else ("📌 BEAR PIN" if r_ < 0.66 else "📌 NEUTRAL PIN")
    # VWAP + gamma wall overlay
    if "TRENDING" in mode and vwap_data['reliable']:
        v = vwap_data
        if "UP"   in mode and v['above_vwap'] and v['vwap_slope'] > 2:     mode += " ✅VWAP"
        elif "UP" in mode and not v['above_vwap']:                          mode += " ⚠️BELOW VWAP"
        elif "DOWN" in mode and not v['above_vwap'] and v['vwap_slope'] < -2: mode += " ✅VWAP"
        elif "DOWN" in mode and v['above_vwap']:                            mode += " ⚠️ABOVE VWAP"
    # Problem 5: gamma wall pinning signal
    if gex_wall_pin and "TRENDING" not in mode:
        mode = "📌 GAMMA PIN @ " + str(gex_r['gex_wall'])

    # ── Stagnation / compression tracking ─────────────────────────────────
    delta_oi = abs(net_w - st.session_state.last_net_oi)
    if delta_oi < 50000 and abs(raw_slope) < 2:
        if comp_r['is_compressing']:
            st.session_state.compression_counter += 1
            st.session_state.stagnation_counter   = 0
        else:
            st.session_state.stagnation_counter  += 1
            st.session_state.compression_counter  = max(0, st.session_state.compression_counter - 1)
    else:
        st.session_state.stagnation_counter  = 0
        if not comp_r['is_compressing']:
            st.session_state.compression_counter = 0
    st.session_state.last_net_oi = net_w

    # ── Narrative ─────────────────────────────────────────────────────────
    dom_actor = "NONE"
    if conc_pct > 30:
        if   net_w < -(STRONG_OI * 0.6): dom_actor = "🐻 CALL WRITERS"
        elif net_w >  (STRONG_OI * 0.6): dom_actor = "🐂 PUT WRITERS"

    seq = "SYNC"
    if   abs(net_w) > STRONG_OI and abs(norm_slope) < 0.2:           seq = "OI LEADS"
    elif abs(norm_slope) > SLOPE_TH and abs(net_w) < STRONG_OI / 2:  seq = "PRICE LEADS"

    phase = "⚪ UNCLEAR"
    if regime == 'EXPIRY_HDGE' and st.session_state.stagnation_counter > 5:
        phase = "🛑 LATE / EXHAUSTION"
    elif seq == "OI LEADS":    phase = "🟢 EARLY / SETUP"
    elif seq == "PRICE LEADS": phase = "🟠 MID / CHASE"

    pain = "NEUTRAL"
    acc_ref = vwap_data['vwap'] if vwap_data['reliable'] else ctx['rolling_mean']
    if   net_w < -STRONG_OI and spot_trend == "BULLISH": pain = "🔥 UPSIDE SQUEEZE"
    elif net_w >  STRONG_OI and spot_trend == "BEARISH": pain = "🩸 DOWNSIDE LIQUIDATION"
    elif "PIN" in mode:                                   pain = "📌 PINNED RANGE"

    # ── Signal strength — regime-multiplied, not just additive ───────────
    # Base score
    sig_str = 50
    if abs(net_w) > STRONG_OI:                    sig_str += 20
    if abs(norm_slope) > SLOPE_TH:                sig_str += 5
    if conc_pct > 40:                             sig_str += 10
    if ctx['has_atm_surge']:                      sig_str += 8
    if st.session_state.compression_counter >= 3: sig_str += 5
    if "SHORT" in gex_r['gex_mode']:              sig_str += 8
    if ctx['fut_r'].get('vol_surge'):             sig_str += 5
    # NOTE: cascade removed from sig_str — it is a derivative of short gamma +
    # vacuum + flow, which are already scored. move_prob carries cascade's effect.
    if abs(delta_flow_r.get('norm_signal', 0)) > 0.4: sig_str += 5
    if imbalance_r['imbalance_ratio'] > 1.5:      sig_str += 5

    # Apply dealer regime multiplier to signal strength (Problem 1 fix)
    # Panic hedging = nonlinear compression toward 100
    regime_mult = dealer_reg_r['multiplier']
    if dealer_reg_r['regime'] == 'PANIC_HEDGING':
        sig_str = int(100 - (100 - sig_str) / regime_mult)
    elif dealer_reg_r['regime'] == 'AMPLIFICATION':
        sig_str = int(min(sig_str * 1.25, 100))
    elif dealer_reg_r['regime'] == 'SUPPRESSION':
        sig_str = int(sig_str * 0.65)   # suppress in long-gamma regime

    # Move probability also boosts sig_str (Problem 3 — sensitivity finally used)
    mp = move_r['explosion_prob']
    if   mp >= 85: sig_str = int(min(sig_str + 12, 100))
    elif mp >= 70: sig_str = int(min(sig_str + 7, 100))
    elif mp >= 50: sig_str = int(min(sig_str + 3, 100))

    # Hero-zero boost: phase-amplified
    hz_phase_boost = phase_mods.get('hero_zero', 1.0)
    if hz_r['ignition']:
        sig_str = int(min(sig_str + int(20 * hz_phase_boost), 100))
    elif hz_r['confidence'] == 'ARMED':
        sig_str = int(min(sig_str + int(12 * hz_phase_boost), 100))
    elif hz_r['confidence'] == 'SETUP':
        sig_str = int(min(sig_str + int(5  * hz_phase_boost), 100))

    # Phase overall confidence damp
    oc = phase_mods.get('overall_confidence', 1.0)
    sig_str = int(np.clip(sig_str * oc, 0, 100))

    # Directional probability (Upgrade 3 — replaces arbitrary scoring) ────
    dir_prob_r = compute_directional_probability(
        net_w, spot_trend, pain, trapped_ce, trapped_pe,
        ctx['skew_25d'], iv_reg['iv_direction'], STRONG_OI,
        gex_r['gex_mode'], vwap_data['above_vwap'],
        max_pain, spot, is_expiry, struct,
        dealer_r['dealer_delta_M'], flow_info['state'], ctrl_r['score'],
        ws_r['weighted_score'], ws_r['confidence'],
        phase_r=phase_r)

    # Keep dir_bias string for backward compat
    dir_bias = dir_prob_r['label']

    return {
        'STRONG_OI': STRONG_OI, 'SLOPE_TH': SLOPE_TH,
        'trapped_ce': trapped_ce, 'trapped_pe': trapped_pe,
        'prob_ctx': prob_ctx,
        'ctrl_r': ctrl_r, 'ws_r': ws_r, 'lq_r': lq_r,
        'smoothed_q': smoothed_q, 'sig_str': sig_str,
        'mode': mode,
        'dom_actor': dom_actor, 'phase': phase, 'pain': pain,
        'dir_bias': dir_bias,
        'dir_prob_r': dir_prob_r,              # Upgrade 3: probabilistic direction
        'spot_trend': spot_trend,
        'acc_ref': acc_ref,
        # Cascade & delta flow
        'cascade_r':       cascade_r,
        'imbalance_r':     imbalance_r,
        'delta_flow_r':    delta_flow_r,
        # Upgrades
        'dealer_reg_r':    dealer_reg_r,
        'hedge_scenarios': hedge_scenarios,
        'move_r':          move_r,
        # Hero-Zero Engine
        'hz_r':            hz_r,
        # Phase engine
        'market_phase_r':  phase_r,
    }
