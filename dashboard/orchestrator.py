"""
DASHBOARD LAYER — orchestrator.py  (thin coordinator)
Responsibility: Call the three engine layers in order and merge outputs.
No math.  No DB.  No UI.  Just orchestration.

Architecture:
    DATA LAYER       → db_loader.load_all()
    MARKET CONTEXT   → market_context.build_market_context()
    SIGNAL ENGINE    → signal_engine.build_signals()
    RISK ENGINE      → risk_engine.build_risk()
    DISPLAY          → panels + drift_dash
"""
import numpy as np
import pandas as pd
import streamlit as st
import time
from features.greeks import RISK_FREE_RATE

from dashboard.market_context   import build_market_context
from dashboard.signal_engine    import build_signals
from dashboard.risk_engine      import build_risk
from market.sensitivity         import compute_spot_sensitivity
from market.control_dashboard   import compute_market_control
from dashboard.state            import push_history
from dashboard.cache            import get_or_compute


def _battle_zone(row, threshold, center):
    oi_ce  = row.get('oi_chg_ce', 0)
    oi_pe  = row.get('oi_chg_pe', 0)
    vol_ce = row.get('vol_mom_ce', 0)
    if abs(oi_ce) > threshold and abs(oi_pe) > threshold:
        if abs(row.name - center) <= 100: return "⚔️ FIGHT"
    if oi_ce > threshold and oi_pe < threshold:  return "🚧 WALL"
    if oi_pe > threshold and oi_ce < threshold:  return "🛡️ SAFE"
    if oi_ce < -threshold and vol_ce > 10000:    return "🔓 BREACH"
    return ""


def run(raw: dict, lookback_mins: int, db_path: str) -> tuple:
    """
    Returns (chain_table: pd.DataFrame, metrics_dict: dict).
    Empty returns signal a data issue — caller should show a warning.
    """
    # ── Layer 1: Market Context ───────────────────────────────────────────
    ctx = build_market_context(raw, lookback_mins, db_path)
    if not ctx:
        return pd.DataFrame(), {}

    # ── Pre-signal: Sensitivity map (needed by move probability) ─────────
    _sens = get_or_compute(
        'sensitivity',
        lambda: compute_spot_sensitivity(
            ctx['merged'], ctx['spot'], ctx['dte'], ctx['atm_iv'],
            delta_s=25.0, radius=max(500, int(ctx.get('exp_move_pts', 0) * 2))),
        ttl=15, current_spot=ctx['spot'], velocity=ctx.get('velocity', 0.0))
    ctx['sens_r'] = _sens   # signal_engine reads this for move probability

    # ── Layer 2: Signals ─────────────────────────────────────────────────
    sig = build_signals(ctx)

    # ── Layer 3: Risk ────────────────────────────────────────────────────
    risk = build_risk(ctx, sig, db_path)

    # ── Build chain table ─────────────────────────────────────────────────
    merged = ctx['merged']
    norm_slope = ctx['norm_slope']

    # P5 vectorised: replace apply(_signal) with np.select on arrays
    _is_ce = (merged['type_now'].values == 'CE')
    _pc    = merged['price_chg'].values
    merged['signal'] = np.select(
        [_is_ce  & (_pc < 0) & (norm_slope >  0.5),   # CE crush
         _is_ce  & (_pc > 0) & (norm_slope < -0.5),   # CE pump
         ~_is_ce & (_pc < 0) & (norm_slope < -0.5),   # PE crush
         ~_is_ce & (_pc > 0) & (norm_slope >  0.5)],  # PE pump
        ['⚠️ CRUSH', '🚀 PUMP', '⚠️ CRUSH', '🚀 PUMP'],
        default='-')
    sig_oi = max(50000, float(merged['oi_chg'].abs().quantile(0.90)))

    atm_strike = ctx['atm_strike']
    mag_pct    = ctx['mag_r']['pct_dict']

    final = merged[['strike_now', 'type_now', 'close_now', 'oi_chg',
                     'vol_mom', 'signal', 'intent', 'flow_type',
                     'delta', 'vol_surge', 'vol_oi_ratio']].copy()
    final.rename(columns={'strike_now': 'strike', 'type_now': 'type'}, inplace=True)

    ce = final[final['type'] == 'CE'].set_index('strike')
    pe = final[final['type'] == 'PE'].set_index('strike')
    ce.columns = [f"{c}_ce" for c in ce.columns]
    pe.columns = [f"{c}_pe" for c in pe.columns]
    chain = pd.merge(ce, pe, left_index=True, right_index=True, how='outer').fillna(0)
    # P5 vectorised: replace apply(_battle_zone) with np.select on chain columns
    _oce = chain['oi_chg_ce'].values.astype(float)
    _ope = chain['oi_chg_pe'].values.astype(float)
    _vce = chain['vol_mom_ce'].values.astype(float) if 'vol_mom_ce' in chain.columns else np.zeros(len(chain))
    _str = chain.index.values.astype(float)
    chain['zone'] = np.select(
        [
            (np.abs(_oce) > sig_oi) & (np.abs(_ope) > sig_oi) & (np.abs(_str - atm_strike) <= 100),
            (_oce > sig_oi) & (_ope < sig_oi),
            (_ope > sig_oi) & (_oce < sig_oi),
            (_oce < -sig_oi) & (_vce > 10000),
        ],
        ['⚔️ FIGHT', '🚧 WALL', '🛡️ SAFE', '🔓 BREACH'],
        default='')
    chain['magnet'] = chain.index.map(
        lambda s: f"{mag_pct.get(s, 0.0):.1f}%" if mag_pct.get(s, 0.0) > 5 else "")
    chain = chain.sort_index()

    # ── Merge everything into one flat metrics dict ───────────────────────
    lq_r    = sig['lq_r']
    ws_r    = sig['ws_r']
    ctrl_r  = sig['ctrl_r']
    gex_r   = ctx['gex_r']
    dealer_r = ctx['dealer_r']
    iv_reg  = ctx['iv_reg']
    vwap_d  = ctx['vwap_data']
    struct  = ctx['struct']
    flow_i  = ctx['flow_info']
    prob_ctx = sig['prob_ctx']

    # Sensitivity map already computed before build_signals
    sens_r = ctx['sens_r']

    # ── GEX History snapshot every ~30s ───────────────────────────────────
    gex_hist = st.session_state.gex_history
    now_ts   = int(time.time())
    if not gex_hist or (now_ts - gex_hist[-1]['ts']) >= 30:
        push_history('gex_history', {
            'ts':           now_ts,
            'spot':         ctx['spot'],
            'gex_by_strike': dict(gex_r['gex_by_strike']),
            'net_gex':      gex_r['net_gex'],
            'gex_wall':     gex_r['gex_wall'],
        }, max_len=60)   # 60 snapshots × 30s = 30 min history

    # ── Upgrade 3: Unified Market Control Dashboard ────────────────────────
    ctrl_full = compute_market_control(
        dealer_delta_M   = dealer_r['dealer_delta_M'],
        dealer_pressure  = dealer_r['pressure'],
        net_gex          = gex_r['net_gex'],
        gex_mode         = gex_r['gex_mode'],
        near_gex_wall    = ctx['near_gex_wall'],
        futures_signal   = ctx['fut_sig'],
        futures_available = ctx['fut_r']['available'],
        net_w_oi         = ctx['net_w'],
        STRONG_OI        = sig['STRONG_OI'],
        flow_state       = flow_i['state'],
        trapped_ce       = sig['trapped_ce'],
        trapped_pe       = sig['trapped_pe'],
        vwap_above       = vwap_d['above_vwap'],
        spot_trend       = sig['spot_trend'],
        iv_pct           = iv_reg['iv_pct'],
        iv_direction     = iv_reg['iv_direction'],
    )

    m = {
        # Core
        'spot':           ctx['spot'],
        'health_score':   ctx['health_score'],
        'net_w':          ctx['net_w'],
        'atm_strike':     atm_strike,
        'is_expiry':      ctx['is_expiry'],
        # Permission
        'trade_allowed':  risk['trade_allowed'],
        'reason':         risk['reason'],
        'is_cool':        risk['is_cool'],
        'size_fac':       risk['size_fac'],
        # Quality layers
        'quality':        sig['smoothed_q'],
        'signal_strength': sig['sig_str'],
        'scores':         lq_r.get('scores', lq_r),
        # Weighted score
        'weighted_score':    ws_r['weighted_score'],
        'confidence':        ws_r['confidence'],
        'confidence_pct':    round(ws_r['confidence'] * 100, 1),
        'score_components':  ws_r.get('component_scores', ws_r.get('components', {})),
        'score_label':       ws_r['label'],
        'dyn_flow_threshold': ws_r.get('dyn_flow_threshold', 5.0),
        'flow_ratio_pct':    ws_r.get('flow_ratio_pct', 50.0),
        # Regime
        'regime':         ctx['regime'],
        'regime_label':   ctx['rmeta']['label'],
        'regime_desc':    ctx['rmeta']['desc'],
        # Narrative
        'phase':          sig['phase'],
        'pain':           sig['pain'],
        'dominant_actor': sig['dom_actor'],
        'mode':           sig['mode'],
        # IV
        'atm_iv':         ctx['atm_iv'],
        'iv_pct':         iv_reg['iv_pct'],
        'iv_label':       iv_reg['iv_label'],
        'iv_direction':   iv_reg['iv_direction'],
        'iv_change_rate': iv_reg['iv_change_rate'],
        'rv':             ctx['rv'],
        'iv_rv_label':    ctx['iv_rv'],
        'skew_25d':       ctx['skew_25d'],
        'iv_25c':         ctx['iv_25c'],
        'iv_25p':         ctx['iv_25p'],
        'exp_move':       ctx['exp_move'],
        'dte_days':       round(ctx['dte'] * 365, 2),
        'straddle':       ctx['straddle'],
        # Probability model
        'prob_win_pct':   prob_ctx['prob_win_pct'],
        'prob_summary':   prob_ctx['summary'],
        'straddle_overpriced': prob_ctx['overpriced'],
        'iv_edge':        prob_ctx['iv_edge'],
        # VWAP
        'vwap':           vwap_d['vwap'],
        'vwap_upper':     vwap_d['upper_1sd'],
        'vwap_lower':     vwap_d['lower_1sd'],
        'vwap_slope':     vwap_d['vwap_slope'],
        'vwap_dist':      vwap_d['dist'],
        'vwap_above':     vwap_d['above_vwap'],
        'vwap_reliable':  vwap_d['reliable'],
        'struct':         struct,
        # GEX
        'net_gex':        gex_r['net_gex'],
        'gex_mode':       gex_r['gex_mode'],
        'gex_wall':       gex_r['gex_wall'],
        'gex_flip':       gex_r['gex_flip_zone'],
        'gamma_wall_oi':  ctx['gwall_oi'],
        'strong_oi':      sig['STRONG_OI'],
        'gex_by_strike':  gex_r['gex_by_strike'],  # for heatmap
        'gex_wall_pin':   ctx['gex_wall_pin'],
        'near_gex_wall':  ctx['near_gex_wall'],
        # Dealer (skew-weighted)
        'dealer_delta_M': dealer_r['dealer_delta_M'],
        'dealer_pressure': dealer_r['pressure'],
        'dealer_skew_note': dealer_r['skew_note'],
        'dealer_ce_oi':   dealer_r.get('ce_oi_total', 0),
        'dealer_pe_oi':   dealer_r.get('pe_oi_total', 0),
        # Flow
        'flow_state':     flow_i['state'],
        'avg_flow_ratio': flow_i['avg_ratio'],
        'aggressive_count': flow_i['aggressive_count'],
        # Futures
        'futures_state':  ctx['fut_r']['futures_state'],
        'futures_signal': ctx['fut_r']['futures_signal'],
        'futures_delta_M': ctx['fut_r']['futures_delta'],
        'futures_surge':  ctx['fut_r']['vol_surge'],
        'futures_available': ctx['fut_r']['available'],
        # Magnetism / structure
        'magnet_strike':  ctx['mag_r']['magnet_strike'],
        'magnet_dist':    ctx['mag_r']['dist'],
        'max_pain':       ctx['max_pain'],
        'conc_pct':       ctx['conc_pct'],
        'dominant_strike': ctx['dom_strike'],
        # Migration
        'migration_strike': ctx['mig_strike'],
        'migration_direction': ctx['mig_dir'],
        # Market control (simple, from signal_engine)
        'ctrl_score':     ctrl_r['score'],
        'ctrl_label':     ctrl_r.get('quadrant', ctrl_r.get('summary_line', '—')),
        # Upgrade 3: Unified market control dashboard
        'mctrl_score':     ctrl_full['score'],
        'mctrl_bar':       ctrl_full['bar'],
        'mctrl_quadrant':  ctrl_full['quadrant'],
        'mctrl_components': ctrl_full['components'],
        'mctrl_summary':   ctrl_full['summary_line'],
        # Upgrade 1: Spot sensitivity map
        'sens_by_strike':     sens_r['by_strike'],
        'sens_df':            sens_r['sensitivity_df'],
        'sens_max_strike':    sens_r['max_sens_strike'],
        'sens_chain_total':   sens_r['chain_total'],
        'sens_explosion_zones': sens_r['explosion_zones'],
        # Upgrade 2: GEX history (in session_state, not m — accessed by panel directly)
        'gex_history_len':    len(st.session_state.gex_history),
        # Directional bias
        'directional_bias': sig['dir_bias'],
        'invalidation':   risk['invalidation'],
        # Alerts
        'trap_alert':     risk['trap_alert'],
        'trap_confirmed': risk['trap_confirmed'],
        'straddle_warn':  risk['straddle_warn'],
        'gamma_flip_msg': ctx['gf_msg'],
        'gamma_flip_crossed': ctx['gf_crossed'],
        'vol_shock':      ctx['vol_shock_msg'],
        'compression_alert': ctx['comp_r']['alert'],
        'is_compressed':     ctx['comp_r'].get('is_compressed', False),
        'trapped_ce':     sig['trapped_ce'],
        'trapped_pe':     sig['trapped_pe'],
        'exit_warn':      risk['exit_warn'],
        'liq_risk':       ctx['liq_risk'],
        'tick_rate':      ctx['atm_tick'],
        'lookback_warning': ctx['lb_warning'],
        # ── 5 Institutional Improvements ──────────────────────────────────
        # 1. Price-OI classification (flows via dealer_r)
        'dealer_flow_breakdown': dealer_r.get('flow_breakdown', {}),
        # 2. True gamma flip (root-solved)
        'gex_flip_exact':    ctx['gex_r']['gex_flip_zone'],
        'gex_dyn_radius':    ctx['gex_r'].get('dynamic_radius', 400),
        # 3. Delta flow
        'delta_flow_norm':   sig['delta_flow_r'].get('norm_signal', 0.0),
        'delta_flow_state':  sig['delta_flow_r'].get('state', '—'),
        'delta_bull':        sig['delta_flow_r'].get('bull_delta', 0.0),
        'delta_bear':        sig['delta_flow_r'].get('bear_delta', 0.0),
        'delta_imbalance':   sig['delta_flow_r'].get('imbalance_pct', 0.0),
        # 4. Liquidity vacuum
        'in_vacuum':         ctx['liq_vac_r']['in_vacuum'],
        'vacuum_zones':      ctx['liq_vac_r']['vacuum_zones'],
        'vacuum_severity':   ctx['liq_vac_r']['vacuum_severity'],
        'next_wall_above':   ctx['liq_vac_r']['next_wall_above'],
        'next_wall_below':   ctx['liq_vac_r']['next_wall_below'],
        'vacuum_alert':      ctx['liq_vac_r']['gap_alert'],
        # 5. Cascade detector (state machine)
        'is_cascade':             sig['cascade_r']['is_cascade'],
        'cascade_type':           sig['cascade_r']['cascade_type'],
        'cascade_strength':       sig['cascade_r']['strength'],
        'cascade_alert':          sig['cascade_r']['alert'],
        'cascade_fsm_state':      sig['cascade_r']['fsm_state'],
        'cascade_frames_in':      sig['cascade_r']['frames_in'],
        'cascade_n_conditions':   sig['cascade_r']['n_conditions'],
        # Hedge flow
        'hedge_flow_norm':   ctx['hedge_r'].get('hedge_flow_norm', 0.0),
        'hedge_flow_state':  ctx['hedge_r'].get('flow_state', '—'),
        # Delta imbalance
        'imbalance_ratio':   sig['imbalance_r']['imbalance_ratio'],
        'imbalance_state':   sig['imbalance_r']['state'],
        # ── This session upgrades ─────────────────────────────────────────
        # Upgrade 1: Nonlinear dealer regime
        'dealer_regime':     sig['dealer_reg_r']['regime'],
        'dealer_regime_label': sig['dealer_reg_r']['label'],
        'dealer_regime_mult':  sig['dealer_reg_r']['multiplier'],
        'dealer_panic_score':  sig['dealer_reg_r']['panic_score'],
        'dealer_conditions':   sig['dealer_reg_r']['conditions_met'],
        # Upgrade 2: Hedge flow scenarios
        'hedge_up50':        sig['hedge_scenarios']['up_50'],
        'hedge_dn50':        sig['hedge_scenarios']['dn_50'],
        'hedge_dom_dir':     sig['hedge_scenarios']['dominant_cascade_dir'],
        'hedge_asymmetry':   sig['hedge_scenarios']['asymmetry'],
        # Move Probability Engine (main)
        'move_prob':         sig['move_r']['explosion_prob'],
        'move_direction':    sig['move_r']['direction'],
        'move_target':       sig['move_r']['target'],
        'move_drivers':      sig['move_r']['drivers'],
        'move_confidence':   sig['move_r']['confidence'],
        'move_interp':       sig['move_r']['interpretation'],
        'move_driver_scores': sig['move_r']['driver_scores'],
        'move_regime_mult':  sig['move_r']['regime_mult'],
        'move_confirm_n':    sig['move_r']['confirming_n'],
        # Liquidity density map
        'liq_profile':       ctx['liq_map_r']['profile'],
        'liq_acc_zones':     ctx['liq_map_r']['acceleration_zones'],
        'liq_dec_zones':     ctx['liq_map_r']['deceleration_zones'],
        'liq_map_df':        ctx['liq_map_r']['visual_data'],
        'liq_acc_above':     ctx['liq_map_r']['nearest_acc_above'],
        'liq_acc_below':     ctx['liq_map_r']['nearest_acc_below'],
        # Skew dynamics
        'skew_velocity':     ctx['skew_dyn_r']['skew_velocity'],
        'skew_z':            ctx['skew_dyn_r']['skew_z_score'],
        'skew_alert':        ctx['skew_dyn_r']['skew_alert'],
        'skew_signal':       ctx['skew_dyn_r']['skew_signal'],
        'skew_state':        ctx['skew_dyn_r']['skew_state'],
        # ── Hero-Zero Engine ──────────────────────────────────────────────
        'hz_score':          sig['hz_r']['hz_score'],
        'hz_ignition':       sig['hz_r']['ignition'],
        'hz_confidence':     sig['hz_r']['confidence'],
        'hz_direction':      sig['hz_r']['direction'],
        'hz_target':         sig['hz_r']['target'],
        'hz_interpretation': sig['hz_r']['interpretation'],
        'hz_conditions':     sig['hz_r']['active_conditions'],
        'hz_conditions_met': sig['hz_r']['conditions_met'],
        'hz_cond_labels':    sig['hz_r']['cond_labels'],
        'hz_core_conditions':sig['hz_r']['core_conditions'],
        'hz_components':     sig['hz_r']['components'],
        'hz_theta_mult':     sig['hz_r']['theta_mult'],
        'hz_theta_label':    sig['hz_r']['theta_label'],
        'hz_theta_bucket':   sig['hz_r']['theta_bucket'],
        # Sub-detector detail (for deep drill-down)
        'hz_wall_r':         sig['hz_r']['wall_r'],
        'hz_trap_r':         sig['hz_r']['trap_r'],
        'hz_accel_r':        sig['hz_r']['accel_r'],
        'hz_theta_r':        sig['hz_r']['theta_r'],
        # Hedge pressure map
        'hp_pressure_pt':    ctx['hedge_pres_r']['pressure_per_pt'],
        'hp_pressure_signed':ctx['hedge_pres_r']['pressure_signed'],
        'hp_vel_pressure':   ctx['hedge_pres_r']['velocity_pressure'],
        'hp_level':          ctx['hedge_pres_r']['pressure_level'],
        'hp_hero_strikes':   ctx['hedge_pres_r']['hero_zone_strikes'],
        'hp_curve_df':       ctx['hedge_pres_r']['pressure_curve_df'],
        # dGamma/dSpot — Dealer Gamma Convexity
        'gconv_speed':       ctx['gamma_conv_r']['chain_speed'],
        'gconv_regime':      ctx['gamma_conv_r']['convexity_regime'],
        'gconv_score':       ctx['gamma_conv_r']['speed_score'],
        'gconv_dom_dir':     ctx['gamma_conv_r']['dominant_direction'],
        'gconv_up':          ctx['gamma_conv_r']['up_convexity'],
        'gconv_dn':          ctx['gamma_conv_r']['dn_convexity'],
        'gconv_note':        ctx['gamma_conv_r']['acceleration_note'],
        'gconv_exp_strikes': ctx['gamma_conv_r']['explosive_strikes'],
        'gconv_by_strike':   ctx['gamma_conv_r']['speed_by_strike'],
        # ── Upgrade 1: Market Phase Engine ───────────────────────────────
        'market_phase':      ctx['phase_r']['phase'],
        'market_phase_note': ctx['phase_r']['note'],
        'market_phase_conf': ctx['phase_r']['confidence'],
        'market_phase_reason':ctx['phase_r']['reason'],
        'phase_trade_ok':    ctx['phase_r']['trade_allowed_by_phase'],
        'phase_quality_cap': ctx['phase_r']['quality_cap'],
        'phase_r':           ctx['phase_r'],
        # ── Upgrade 3: Directional Probability ───────────────────────────
        'p_up':              sig['dir_prob_r']['p_up'],
        'p_dn':              sig['dir_prob_r']['p_dn'],
        'dir_direction':     sig['dir_prob_r']['direction'],
        'dir_edge':          sig['dir_prob_r']['edge'],
        'dir_label':         sig['dir_prob_r']['label'],
        # ── Upgrade 5: Stability-filtered signals ─────────────────────────
        'stable_cascade':    ctx.get('stable_cascade', False),
        'stable_vacuum':     ctx.get('stable_vacuum',  False),
        'stable_compress':   ctx.get('stable_compress',False),
        # ── P7: Event Awareness ───────────────────────────────────────────
        'event_risk_level':  ctx['event_r']['event_risk_level'],
        'events_today':      ctx['event_r']['events_today'],
        'events_soon':       ctx['event_r']['events_soon'],
        'event_note':        ctx['event_r']['note'],
        'event_quality_damp':ctx['event_r']['quality_damp'],
        'expiry_type':       ctx['event_r']['expiry_type'],
        'global_risk':       ctx['global_risk_r']['global_risk'],
        'vix_regime':        ctx['global_risk_r']['vix_regime'],
        'global_risk_note':  ctx['global_risk_r']['note'],
        # ── P1: Dealer/GEX Uncertainty Ranges ────────────────────────────
        'gex_low':           ctx['gex_r']['gex_low'],
        'gex_high':          ctx['gex_r']['gex_high'],
        'gex_uncertainty_note': ctx['gex_r']['gex_uncertainty_note'],
        'dealer_delta_low':  ctx['dealer_r']['dealer_delta_low'],
        'dealer_delta_high': ctx['dealer_r']['dealer_delta_high'],
        'dealer_note':       ctx['dealer_r']['dealer_note'],
        # ── P3: Move probability interaction multiplier ───────────────────
        'interaction_mult':  sig['move_r'].get('interaction_mult', 1.0),
        # ── Final Upgrade: Flow Persistence ──────────────────────────────
        'pressure_score':    ctx.get('persistence_r', {}).get('pressure_score', 0.0),
        'persist_mult':      ctx.get('persistence_r', {}).get('persist_mult', 1.0),
        'buildup_phase':     ctx.get('persistence_r', {}).get('buildup_phase', 'DORMANT'),
        'hz_persist_mult':   sig.get('hz_r', {}).get('persist_mult', 1.0),
        'delta_dur_min':     ctx.get('persistence_r', {}).get('delta_dur_min', 0.0),
        'gamma_dur_min':     ctx.get('persistence_r', {}).get('gamma_dur_min', 0.0),
        'vacuum_dur_min':    ctx.get('persistence_r', {}).get('vacuum_dur_min', 0.0),
        'compress_dur_min':  ctx.get('persistence_r', {}).get('compress_dur_min', 0.0),
        'ps_delta':          ctx.get('persistence_r', {}).get('ps_delta', 0.0),
        'ps_gamma':          ctx.get('persistence_r', {}).get('ps_gamma', 0.0),
        'ps_vacuum':         ctx.get('persistence_r', {}).get('ps_vacuum', 0.0),
        'ps_compress':       ctx.get('persistence_r', {}).get('ps_compress', 0.0),
        'delta_direction_p': ctx.get('persistence_r', {}).get('delta_direction', 'NEUTRAL'),
        'delta_accel_p':     ctx.get('persistence_r', {}).get('delta_accel', 0.0),
        'move_pressure_bonus': sig['move_r'].get('pressure_bonus', 0.0),
    }

    return chain, m