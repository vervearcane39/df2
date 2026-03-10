"""
DASHBOARD LAYER — risk_engine.py
Responsibility: Trade permission, sizing, exit alerts, invalidation.
No market math.  No signal scoring.  Pure risk decisions.
"""
import time
import logging
import numpy as np
import streamlit as st

from risk.trade_filter  import (detect_trap_v3, evaluate_permission,
                                 compute_invalidation, compute_sizing)
from dashboard.state    import push_history


def build_risk(ctx: dict, sig: dict, db_path: str) -> dict:
    """
    Reads market context + signal outputs → produces permission + sizing.
    Also fires logging and manages cooldown state.
    """
    spot       = ctx['spot']
    health     = ctx['health_score']
    gex_r      = ctx['gex_r']
    iv_reg     = ctx['iv_reg']
    liq_risk   = ctx['liq_risk']
    comp_r     = ctx['comp_r']
    gf_crossed = ctx['gf_crossed']
    gf_msg     = ctx['gf_msg']
    vwap_data  = ctx['vwap_data']
    dealer_r   = ctx['dealer_r']
    mag_r      = ctx['mag_r']
    max_pain   = ctx['max_pain']
    is_expiry  = ctx['is_expiry']
    regime     = ctx['regime']
    rmeta      = ctx['rmeta']
    atm_strike = ctx['atm_strike']
    norm_slope = ctx['norm_slope']
    atm_mom    = ctx['atm_mom']
    liq_thresh = ctx['liq_thresh']

    smoothed_q = sig['smoothed_q']
    phase      = sig['phase']
    mode       = sig['mode']
    lq_r       = sig['lq_r']
    ws_r       = sig['ws_r']
    ctrl_r     = sig['ctrl_r']
    net_w      = ctx['net_w']
    STRONG_OI  = sig['STRONG_OI']
    SLOPE_TH   = sig['SLOPE_TH']
    spot_trend = sig['spot_trend']
    trapped_ce = sig['trapped_ce']
    trapped_pe = sig['trapped_pe']
    prob_ctx   = sig['prob_ctx']

    # ── Trap detection ────────────────────────────────────────────────────
    trap_alert, trap_confirmed = detect_trap_v3(
        net_w, spot_trend, norm_slope, atm_mom, liq_thresh,
        STRONG_OI, SLOPE_TH, mode, regime)

    # Gamma wall pinning trap override
    if ctx['gex_wall_pin']:
        if "TRENDING" in mode:
            trap_alert     = f"⚠️ TRENDING INTO GAMMA WALL @ {gex_r['gex_wall']} — pinning risk"
            trap_confirmed = False

    # Expected-move / straddle overpriced filter (Problem 6)
    straddle_warn = None
    if prob_ctx.get('overpriced'):
        straddle_warn = (
            f"⚠️ STRADDLE OVERPRICED: {ctx['straddle']:.0f} > "
            f"ExpMove {prob_ctx['expected_move']:.0f} × 1.2 — avoid buying"
        )

    # ── Permission ────────────────────────────────────────────────────────
    perm = evaluate_permission(
        smoothed_q, health, st.session_state.cooldown_until,
        liq_risk, trap_alert, gf_crossed,
        gex_r['gex_mode'], iv_reg['iv_pct'], iv_reg['iv_direction'], regime)

    # ── Upgrade 1: Phase gate — override permission during bad phases ──────
    phase_r      = ctx.get('phase_r', {})
    market_phase = phase_r.get('phase', 'BALANCED')
    phase_note   = phase_r.get('note', '')
    stable_cascade = ctx.get('stable_cascade', False)

    if perm['allowed']:
        if not phase_r.get('trade_allowed_by_phase', True):
            perm = {'allowed': False,
                    'reason': f"PHASE: {market_phase} — {phase_note.split('—')[1].strip() if '—' in phase_note else phase_note}",
                    'is_cool': False}
        elif market_phase == 'OPEN_DISCOVERY':
            perm = {'allowed': False,
                    'reason': '🌅 OPEN DISCOVERY — wait until 9:45',
                    'is_cool': False}

    # ── P7: Event risk gate ───────────────────────────────────────────────
    event_r      = ctx.get('event_r', {})
    event_level  = event_r.get('event_risk_level', 'NONE')
    global_risk  = ctx.get('global_risk_r', {}).get('global_risk', 'CALM')

    # EXTREME risk day (monthly expiry + RBI same day): warn but don't block
    # HIGH risk day (macro event today): apply quality damp already in quality score
    # RISK_OFF global: additional warning
    if perm['allowed'] and global_risk == 'RISK_OFF':
        # Don't block, but mark as elevated risk (affects sizing later)
        perm['global_risk_warn'] = True

    # Block on cascade (opposite direction to cascade = bad entry)
    cascade_r   = sig.get('cascade_r', {})
    imbalance_r = sig.get('imbalance_r', {})
    liq_vac_r   = ctx.get('liq_vac_r', {})
    delta_flow_r = sig.get('delta_flow_r', {})

    if perm['allowed'] and liq_vac_r.get('in_vacuum'):
        # Upgrade 5: use stability-filtered vacuum, not raw tick
        if not ctx.get('stable_vacuum', liq_vac_r.get('in_vacuum')):
            pass   # not yet confirmed — don't block on single-tick vacuum
        elif not cascade_r.get('is_cascade'):
            perm = {'allowed': False, 'reason': 'LIQUIDITY VACUUM — no cascade confirm', 'is_cool': False}

    # Probability-based override: block if P(win) < 40% AND overpriced
    if perm['allowed'] and not prob_ctx['buy_signal'] and prob_ctx['overpriced']:
        perm = {'allowed': False, 'reason': 'OVERPRICED + LOW WIN PROB', 'is_cool': False}

    # ── Cooldown management ───────────────────────────────────────────────
    was_in_trade = st.session_state.prev_trade_state
    if was_in_trade and not perm['allowed']:
        st.session_state.cooldown_until = time.time() + 20

    # ── Logging on entry ─────────────────────────────────────────────────
    if perm['allowed'] and not was_in_trade:
        dir_prob = sig.get('dir_prob_r', {})
        logging.info(
            f"ENTRY Q:{smoothed_q}(S:{lq_r['scores']['s']} F:{lq_r['scores']['f']} "
            f"V:{lq_r['scores']['v']} P:{lq_r['scores']['p']}) "
            f"PHASE:{market_phase} "
            f"P(up):{dir_prob.get('p_up',0)*100:.0f}% P(dn):{dir_prob.get('p_dn',0)*100:.0f}% "
            f"IV:{ctx['atm_iv']:.1%}({iv_reg['iv_pct']:.0f}%) "
            f"GEX:{gex_r['gex_mode'][:15]} "
            f"δFLOW:{delta_flow_r.get('norm_signal',0):+.2f} "
            f"CASCADE:{cascade_r.get('cascade_type','—')}[{cascade_r.get('fsm_state','—')}] "
            f"P(win):{prob_ctx['prob_win_pct']}%"
        )
    st.session_state.prev_trade_state = perm['allowed']

    # ── Sizing ────────────────────────────────────────────────────────────
    from risk.trade_filter import compute_sizing
    size_fac = compute_sizing(
        perm['allowed'], ctx['conc_pct'], phase, regime, rmeta['s_mod'],
        iv_reg['iv_pct'], iv_reg['iv_direction'],
        gex_r['gex_mode'], dealer_r['dealer_delta_M'])

    # Reduce size near gamma wall (pinning risk lowers payoff)
    if ctx['near_gex_wall']:
        size_fac = max(size_fac - 0.25, 0.5)

    # ── Exit alerts ───────────────────────────────────────────────────────
    exit_warn = None
    if perm['allowed'] or was_in_trade:
        prev = st.session_state.prev_mode
        if   "TRENDING" in prev and "CHOP" in mode:           exit_warn = "⚠️ TREND STALLED"
        elif "PIN" in prev and "PIN" not in mode:             exit_warn = "⚠️ PIN BROKEN"
        elif "CRUSHING" in iv_reg['iv_direction'] and iv_reg['iv_pct'] > 60:
                                                               exit_warn = "⚠️ IV CRUSH"
        elif gf_crossed:                                      exit_warn = "⚠️ GAMMA FLIP — regime change"
        elif ctx['fut_r'].get('vol_surge') and not perm['allowed']:
                                                               exit_warn = "⚠️ FUTURES SURGE — re-evaluate"
    st.session_state.prev_mode = mode

    # ── Invalidation ─────────────────────────────────────────────────────
    invalidation = compute_invalidation(
        mode, phase, net_w, vwap_data['vwap'], vwap_data['reliable'],
        ctx['rolling_mean'], atm_strike)

    return {
        'trap_alert':      trap_alert,
        'trap_confirmed':  trap_confirmed,
        'straddle_warn':   straddle_warn,
        'perm':            perm,
        'trade_allowed':   perm['allowed'],
        'reason':          perm['reason'],
        'is_cool':         perm['is_cool'],
        'size_fac':        size_fac,
        'exit_warn':       exit_warn,
        'invalidation':    invalidation,
    }
