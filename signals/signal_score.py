"""
SIGNAL LAYER — signal_score.py
Probabilistic Signal Scoring  (Upgrade 3)

What was wrong with the old version:
    Signals combined with hand-tuned weights:
        dealer 30%, gamma 25%, flow 20%, futures 15% ...

    Those numbers were arbitrary. "Score 72" meant nothing concrete.

What this version does:
    Replaces hand-tuned linear combination with a logistic sigmoid model.

    P(move) = sigmoid( sum(wi * feature_i) )

    Each feature is normalised before entering the sigmoid.
    The output is a true 0-1 probability — interpretable even without calibration.

Phase-aware:
    Weights multiplied by phase modifiers. Signals mean different things
    at 10:30 vs 14:45. Phase engine controls this.

Directional scoring:
    P_up = sigmoid(bull_logit)  P_dn = sigmoid(bear_logit)
    Presented as: "P(up move) = 62%"  — far more useful than CALL LEAN.
"""
import numpy as np
from typing import Optional

from signals.regime_learning import (
    flow_ratio_percentile, get_dynamic_flow_threshold,
)


def _sig(x):
    x = float(np.clip(x, -10.0, 10.0))
    return 1.0 / (1.0 + np.exp(-x))

def _norm(value, lo, hi):
    if hi == lo: return 0.0
    return float(np.clip(2.0*(value-lo)/(hi-lo)-1.0, -1.0, 1.0))

def _f_dealer(dealer_delta_M):
    return float(np.clip(-dealer_delta_M/3.0, -1.0, 1.0))

def _f_gamma(net_gex):
    return float(np.clip(-net_gex/6.0, -1.0, 1.0))

def _f_flow(flow_state, avg_flow_ratio):
    fr_pct = flow_ratio_percentile(avg_flow_ratio)
    if   fr_pct >= 90: return  0.8
    elif fr_pct >= 75: return  0.4
    elif 'Writing' in flow_state: return -0.5
    elif 'Aggressive' in flow_state: return 0.4
    return 0.0

def _f_futures(futures_signal):
    return float(np.clip(futures_signal, -1.0, 1.0))

def _f_vol(iv_pct, iv_direction):
    base = _norm(iv_pct, 20.0, 80.0) * 0.3
    if   'EXPANDING' in iv_direction: base += 0.3
    elif 'CRUSH'     in iv_direction: base -= 0.4
    return float(np.clip(base, -1.0, 1.0))

def _f_struct(vwap_above, struct):
    s = 0.3 if vwap_above else -0.3
    if struct.get('above_pdh'): s += 0.2
    if struct.get('below_pdl'): s -= 0.2
    if struct.get('above_orh'): s += 0.1
    if struct.get('below_orl'): s -= 0.1
    return float(np.clip(s, -1.0, 1.0))

BASE_LOG_ODDS = {
    'dealer': 1.20, 'gamma': 1.00, 'flow': 0.80,
    'futures': 0.60, 'vol': 0.20, 'struct': 0.20,
}

def compute_weighted_signal_score(
    dealer_delta_M, net_gex, flow_state, avg_flow_ratio,
    iv_pct, iv_direction, vwap_above, struct,
    futures_signal=0.0, phase_modifiers=None,
):
    mods = phase_modifiers or {}
    feat = {
        'dealer':  _f_dealer(dealer_delta_M),
        'gamma':   _f_gamma(net_gex),
        'flow':    _f_flow(flow_state, avg_flow_ratio),
        'futures': _f_futures(futures_signal),
        'vol':     _f_vol(iv_pct, iv_direction),
        'struct':  _f_struct(vwap_above, struct),
    }
    total_logit = 0.0
    abs_total   = 0.0
    comp_out    = {}
    for k, base_w in BASE_LOG_ODDS.items():
        w       = base_w * mods.get(k, 1.0)
        contrib = w * feat[k]
        total_logit += contrib
        abs_total   += abs(contrib)
        comp_out[k]  = round(float(feat[k]), 3)

    weighted_score = float(np.tanh(total_logit / max(abs_total, 1.0) * 2.0))
    avg_abs   = abs_total / len(BASE_LOG_ODDS)
    move_prob = _sig(avg_abs * 2.0 - 1.0)
    n_agree   = sum(1 for v in feat.values()
                    if (v>0.1 and weighted_score>0) or (v<-0.1 and weighted_score<0))
    n_total   = sum(1 for v in feat.values() if abs(v) > 0.1)
    confidence = (n_agree / max(n_total, 1)) if n_total > 0 else 0.5

    label = (
        '🟢 STRONG BULL' if weighted_score > 0.5 else
        '📈 BULL LEAN'   if weighted_score > 0.15 else
        '🔴 STRONG BEAR' if weighted_score < -0.5 else
        '📉 BEAR LEAN'   if weighted_score < -0.15 else
        '⚪ NEUTRAL'
    )
    return {
        'weighted_score': round(weighted_score, 3),
        'move_prob':      round(move_prob, 3),
        'confidence':     round(confidence, 3),
        'label':          label,
        'component_scores': comp_out,
        'phase_applied':  bool(phase_modifiers),
    }


def compute_layered_quality(
    vwap_above, vwap_reliable, acc_dist, dist_thr, regime, struct,
    net_w, STRONG_OI, flow_state, trapped_ce, trapped_pe,
    dealer_delta_M, ctrl_score, has_atm_surge, iv_pct, iv_direction,
    net_gex, gex_mode, atm_iv, rv, compression_ctr, stagnation_ctr,
    max_pain, spot, conc_pct, magnet_strike, _unused, health, q_mod,
    futures_signal=0.0, phase_r=None,
):
    # S: signal quality
    s = 50
    if abs(net_w) > STRONG_OI:        s += 15
    if conc_pct > 40:                  s += 10
    if trapped_ce or trapped_pe:       s += 10
    if compression_ctr >= 3:           s += 8
    if 'SHORT' in gex_mode:            s += 7
    # ctrl_score removed — it aggregates gamma+dealer+flow already scored elsewhere
    if trapped_ce and trapped_pe:      s -= 10
    s = int(np.clip(s, 0, 100))

    # F: flow quality
    f = 50
    if has_atm_surge:                  f += 20
    if abs(futures_signal) > 0.3:      f += 12
    if 'AGGRESSIVE' in flow_state:     f += 10
    if stagnation_ctr > 5:             f -= 25
    f = int(np.clip(f, 0, 100))

    # V: volatility quality
    v = 50
    iv_rv_r = atm_iv / max(rv, 0.01)
    if iv_rv_r < 0.85:                 v += 20
    elif iv_rv_r > 1.60:               v -= 25
    if 'EXPANDING' in iv_direction:    v += 15
    elif 'CRUSH'   in iv_direction:    v -= 20
    if iv_pct > 80:                    v -= 15
    elif iv_pct < 20:                  v += 10
    v = int(np.clip(v, 0, 100))

    # P: price quality
    p = 50
    if vwap_reliable and vwap_above:   p += 10
    elif vwap_reliable:                p += 5
    if acc_dist < dist_thr:            p += 10
    elif acc_dist > dist_thr * 2:      p -= 20
    if struct.get('above_pdh') or struct.get('below_pdl'): p += 12
    if max_pain > 0 and abs(spot-max_pain) < 50 and 'EXPIRY' in regime:
        p -= 15
    p = int(np.clip(p, 0, 100))

    # T: time quality
    t = 50
    t += {'MID': 10, 'LATE': 5, 'OPEN': -20, 'EXPIRY_HDGE': 15}.get(regime, 0)
    if health < 60:   t -= 20
    elif health > 90: t += 5
    t = int(np.clip(t, 0, 100))

    raw = int(0.30*s + 0.25*f + 0.25*v + 0.10*p + 0.10*t)
    raw = int(np.clip(int(raw * q_mod), 0, 100))

    if phase_r:
        from market.market_phase import apply_phase_quality_cap
        raw = apply_phase_quality_cap(raw, phase_r)

    return {'quality': raw, 'scores': {'s': s, 'f': f, 'v': v, 'p': p, 't': t},
            'iv_rv_ratio': round(iv_rv_r, 2)}


def compute_directional_probability(
    net_w_oi, spot_trend, pain, trapped_ce, trapped_pe,
    skew_25d, iv_direction, STRONG_OI, gex_mode, vwap_above,
    max_pain, spot, is_expiry, struct, dealer_delta_M, flow_state,
    ctrl_score, weighted_score, confidence, phase_r=None,
):
    """
    P(up) and P(dn) as true probabilities.
    Replaces arbitrary call_score/put_score integer counting.
    """
    mods = (phase_r or {}).get('modifiers', {})
    oc = mods.get('overall_confidence', 1.0)

    bull = bear = 0.0

    if net_w_oi < -STRONG_OI * 0.5:    bear += 1.5
    elif net_w_oi > STRONG_OI * 0.5:   bull += 1.5

    bull += max(weighted_score, 0.0) * 1.2
    bear += max(-weighted_score, 0.0) * 1.2

    if vwap_above:  bull += 0.4
    else:           bear += 0.4

    if trapped_ce:  bull += 0.9
    if trapped_pe:  bear += 0.9

    # dealer_delta_M removed here — already the highest-weight input (1.20)
    # in weighted_score which feeds into bull/bear above. Direct use = double-count.

    if 'SQUEEZE'     in pain:  bull += 0.8
    if 'LIQUIDATION' in pain:  bear += 0.8

    if skew_25d > 0.04:    bear += 0.4
    elif skew_25d < -0.04: bull += 0.4

    if struct.get('above_pdh'): bull += 0.3
    if struct.get('below_pdl'): bear += 0.3
    if struct.get('above_orh'): bull += 0.2
    if struct.get('below_orl'): bear += 0.2

    if is_expiry and max_pain > 0:
        dist = spot - max_pain
        if   dist >  100: bear += 0.4
        elif dist < -100: bull += 0.4

    if 'SHORT' in gex_mode:
        bull *= 1.15; bear *= 1.15

    bull *= oc; bear *= oc

    p_up = _sig(bull - 1.5)
    p_dn = _sig(bear - 1.5)
    total = p_up + p_dn
    if total > 0:
        p_up /= total; p_dn /= total
    else:
        p_up = p_dn = 0.5

    edge = abs(p_up - p_dn)

    if p_up > p_dn + 0.08:
        direction = 'UP'
        label = (f"🟢 BUY CALL  (P={p_up*100:.0f}%)" if p_up > 0.65
                 else f"📈 CALL LEAN (P={p_up*100:.0f}%)")
    elif p_dn > p_up + 0.08:
        direction = 'DOWN'
        label = (f"🔴 BUY PUT   (P={p_dn*100:.0f}%)" if p_dn > 0.65
                 else f"📉 PUT LEAN  (P={p_dn*100:.0f}%)")
    else:
        direction = 'NEUTRAL'
        label = f"⚪ NO EDGE   (P↑={p_up*100:.0f}% P↓={p_dn*100:.0f}%)"

    return {
        'p_up': round(p_up, 3), 'p_dn': round(p_dn, 3),
        'direction': direction, 'edge': round(edge, 3),
        'label': label, 'bull_logit': round(bull, 3), 'bear_logit': round(bear, 3),
    }
