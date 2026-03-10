"""
MARKET STATE LAYER — cascade.py
Improvement 5 — Dealer Gamma Hedging Cascade Detector

The biggest intraday moves occur when dealers are forced to hedge
in the same direction as the market move, creating a feedback loop:

    Short gamma + spot moves up →
    Dealer delta goes negative (needs to BUY) →
    Buying pushes spot higher →
    New delta imbalance → more buying →
    CASCADE

Formula (Doc 15):
    HedgeFlow = Gamma × SpotChange × DealerPosition

    DealerPosition: inferred from net_gex sign
        net_gex < 0 → dealer net short gamma → hedge amplifies moves
        net_gex > 0 → dealer net long gamma  → hedge dampens moves

    SpotChange: recent velocity (slope from last N minutes)

HedgeFlow interpretation:
    Large positive → dealers forced to BUY aggressively → squeeze risk
    Large negative → dealers forced to SELL aggressively → flush risk
    Near zero      → no cascade risk regardless of move direction

Cascade conditions (all must be true):
    1. Short gamma (net_gex < -2.0)
    2. Directional futures flow (dealers not the only buyers/sellers)
    3. Trapped sellers in CE or PE (confirming squeeze potential)
    4. |HedgeFlow| > threshold

Also detects delta imbalance — the precursor condition.
"""
import numpy as np
import math
from typing import Optional


# ── Hedge Flow Computation ────────────────────────────────────────────────────

def compute_hedge_flow(net_gex: float, spot: float, raw_slope: float,
                       dealer_delta_M: float,
                       velocity: float) -> dict:
    """
    HedgeFlow = net_gamma_exposure × spot_velocity × dealer_short_factor

    net_gamma_exposure: net_gex (in M units from gamma.py)
    spot_velocity:       raw_slope (points/min, unsigned for scaling)
    dealer_short_factor: -sign(net_gex) — short gamma amplifies, long dampens

    Sign of HedgeFlow:
        Positive = dealer hedging flow is BULLISH (buying pressure)
        Negative = dealer hedging flow is BEARISH (selling pressure)

    Scaling: normalise by spot to get a dimensionless intensity.
    """
    try:
        # Dealer short factor: short gamma amplifies, long gamma dampens
        gamma_factor  = -float(net_gex)   # negative net_gex = dealer short = amplifies
        spot_move     = float(raw_slope)   # points/min

        # Hedge flow raw: gamma × spot_move (proportional to futures needed to hedge)
        hedge_raw     = gamma_factor * spot_move * (spot / 1000.0)
        intensity     = abs(hedge_raw) / max(velocity + 0.1, 0.1)

        # Normalise to [-5, +5] for display
        norm = float(np.clip(hedge_raw, -5.0, 5.0))

        if   norm >  2.0: flow_state = "🔥 STRONG BULLISH hedge cascade"
        elif norm >  0.8: flow_state = "⬆️ MILD BULLISH hedging"
        elif norm < -2.0: flow_state = "💧 STRONG BEARISH hedge cascade"
        elif norm < -0.8: flow_state = "⬇️ MILD BEARISH hedging"
        else:             flow_state = "⚖️ Neutral hedge flow"

        return {
            'hedge_flow_raw':   round(hedge_raw, 3),
            'hedge_flow_norm':  round(norm, 3),
            'flow_state':       flow_state,
            'intensity':        round(intensity, 3),
        }
    except Exception:
        return {'hedge_flow_raw': 0.0, 'hedge_flow_norm': 0.0,
                'flow_state': '⚖️ Neutral', 'intensity': 0.0}


# ── Cascade State Machine (Problem 4 fix) ────────────────────────────────────
#
# The old implementation triggered on a single frame. This is fragile.
# A real cascade is a process, not an event. The state machine requires
# conditions to persist across multiple observations before confirming.
#
# States:
#   IDLE → WARMING (≥2 conditions) → BUILDING (≥3 conditions, 2+ frames)
#         → CONFIRMED (all conditions, 2+ consecutive frames)
#         → FIRING (confirmed 3+ frames)
#
# Timeout: if conditions drop below 2 for >3 frames, reset to IDLE.
# This eliminates false triggers from single-frame noise.

_CASCADE_STATE = {
    'state':       'IDLE',          # IDLE | WARMING | BUILDING | CONFIRMED | FIRING
    'frames_in':   0,               # consecutive frames in current state
    'frames_idle': 0,               # consecutive frames below threshold
    'last_type':   None,            # 'BULL' | 'BEAR' | None
    'history':     [],              # rolling condition counts (last 5)
}

_CASCADE_MIN_FRAMES = {
    'WARMING':   1,   # just needs to enter
    'BUILDING':  2,   # 2 consecutive frames with ≥3 conditions
    'CONFIRMED': 2,   # 2 consecutive frames with all conditions
    'FIRING':    3,   # 3 consecutive frames confirmed
}

_IDLE_RESET_FRAMES = 3   # frames below threshold before resetting


def _count_cascade_conditions(
    is_short_gamma: bool, trapped_ce: bool, trapped_pe: bool,
    is_bull_flow: bool, is_bear_flow: bool,
    hedge_bull: bool, hedge_bear: bool, is_volatile: bool,
) -> tuple:
    """Return (bull_count, bear_count, cascade_type, total_conditions)."""
    bull = sum([is_short_gamma, trapped_ce, is_bull_flow, hedge_bull, is_volatile])
    bear = sum([is_short_gamma, trapped_pe, is_bear_flow, hedge_bear, is_volatile])

    if bull > bear and bull >= 2:
        return bull, bear, 'BULL', bull
    elif bear > bull and bear >= 2:
        return bull, bear, 'BEAR', bear
    elif bull == bear and bull >= 2:
        return bull, bear, 'BULL', bull   # tie → prefer bull
    return 0, 0, None, 0


def detect_cascade(
    net_gex:         float,
    hedge_flow_norm: float,
    trapped_ce:      bool,
    trapped_pe:      bool,
    futures_signal:  float,
    delta_flow_norm: float,
    velocity:        float,
    HIGH_VOL:        float,
) -> dict:
    """
    State machine cascade detector.

    Eliminates false triggers: conditions must persist across multiple
    consecutive observations before cascade is confirmed.

    States and thresholds:
        IDLE       → need ≥2 conditions for WARMING
        WARMING    → need ≥3 conditions, 1 frame
        BUILDING   → need ≥3 conditions, 2 frames in state
        CONFIRMED  → all conditions, 2 frames
        FIRING     → confirmed 3+ frames (highest conviction)
    """
    global _CASCADE_STATE

    is_short_gamma = net_gex < -2.0
    is_volatile    = velocity > HIGH_VOL
    is_bull_flow   = delta_flow_norm > 0.3 and futures_signal > 0.3
    is_bear_flow   = delta_flow_norm < -0.3 and futures_signal < -0.3
    hedge_bull     = hedge_flow_norm > 0.8
    hedge_bear     = hedge_flow_norm < -0.8

    bull_c, bear_c, c_type, n_conds = _count_cascade_conditions(
        is_short_gamma, trapped_ce, trapped_pe,
        is_bull_flow, is_bear_flow, hedge_bull, hedge_bear, is_volatile)

    # All conditions for a full cascade
    if c_type == 'BULL':
        all_conds = is_short_gamma and trapped_ce and is_bull_flow and hedge_bull
    elif c_type == 'BEAR':
        all_conds = is_short_gamma and trapped_pe and is_bear_flow and hedge_bear
    else:
        all_conds = False

    # Update history
    s = _CASCADE_STATE
    s['history'].append(n_conds)
    if len(s['history']) > 5:
        s['history'].pop(0)

    # State transitions
    prev_state = s['state']

    if n_conds < 2:
        s['frames_idle'] += 1
        if s['frames_idle'] >= _IDLE_RESET_FRAMES:
            s['state'] = 'IDLE'
            s['frames_in'] = 0
            s['last_type'] = None
    else:
        s['frames_idle'] = 0
        s['last_type'] = c_type

        if s['state'] == 'IDLE':
            s['state'] = 'WARMING'
            s['frames_in'] = 1

        elif s['state'] == 'WARMING':
            s['frames_in'] += 1
            if n_conds >= 3:
                s['state'] = 'BUILDING'
                s['frames_in'] = 1

        elif s['state'] == 'BUILDING':
            if n_conds >= 3:
                s['frames_in'] += 1
                if all_conds and s['frames_in'] >= _CASCADE_MIN_FRAMES['BUILDING']:
                    s['state'] = 'CONFIRMED'
                    s['frames_in'] = 1
            else:
                s['state'] = 'WARMING'
                s['frames_in'] = 1

        elif s['state'] == 'CONFIRMED':
            if all_conds:
                s['frames_in'] += 1
                if s['frames_in'] >= _CASCADE_MIN_FRAMES['CONFIRMED']:
                    s['state'] = 'FIRING'
            else:
                s['state'] = 'BUILDING'
                s['frames_in'] = 1

        elif s['state'] == 'FIRING':
            if not all_conds:
                s['state'] = 'CONFIRMED' if n_conds >= 3 else 'BUILDING'
                s['frames_in'] = 1
            else:
                s['frames_in'] += 1

    # Output
    fsm_state   = s['state']
    is_cascade  = fsm_state in ('CONFIRMED', 'FIRING')
    strength    = {'IDLE': 0, 'WARMING': 1, 'BUILDING': 2,
                   'CONFIRMED': 3, 'FIRING': 3}.get(fsm_state, 0)

    # Alert only on confirmed / firing
    alert = None
    final_type = s['last_type']
    if fsm_state == 'FIRING' and final_type:
        alert = (f"🚨 {'BULL' if final_type=='BULL' else 'BEAR'} CASCADE FIRING "
                 f"[{s['frames_in']} frames] — "
                 f"γ:{net_gex:.1f} δ:{delta_flow_norm:+.2f} hedge:{hedge_flow_norm:+.2f}")
    elif fsm_state == 'CONFIRMED' and final_type:
        alert = (f"🚨 {'BULL' if final_type=='BULL' else 'BEAR'} CASCADE CONFIRMED "
                 f"— short γ + trapped + flow aligned")
    elif fsm_state == 'BUILDING':
        partial = []
        if is_short_gamma:              partial.append("short γ")
        if trapped_ce or trapped_pe:    partial.append("trapped")
        if is_bull_flow or is_bear_flow: partial.append("flow")
        if is_volatile:                 partial.append("velocity")
        alert = f"⚠️ CASCADE BUILDING [{','.join(partial)}] ({s['frames_in']} frames)"

    return {
        'is_cascade':   is_cascade,
        'cascade_type': final_type,
        'strength':     strength,
        'fsm_state':    fsm_state,
        'frames_in':    s['frames_in'],
        'n_conditions': n_conds,
        'alert':        alert,
    }


# ── Delta Imbalance (precursor) ───────────────────────────────────────────────

def compute_delta_imbalance(dealer_delta_M: float, delta_flow_norm: float,
                             net_gex: float) -> dict:
    """
    Delta imbalance = demand > liquidity.
    Precursor to large moves: when delta demand exceeds dealer capacity.

    Dealer capacity proxy: proportional to |net_gex| (more gamma = more liquidity)
    Delta demand: from delta_flow_norm (normalised option flow)

    Imbalance ratio > 1.5 = demand exceeds supply → move likely
    """
    capacity  = max(abs(net_gex) * 0.5, 0.5)
    demand    = abs(delta_flow_norm) + abs(dealer_delta_M) * 0.2
    ratio     = demand / capacity

    if   ratio > 2.0:  state = "⚡ EXTREME IMBALANCE — move imminent"
    elif ratio > 1.5:  state = "⚠️ HIGH IMBALANCE — elevated move risk"
    elif ratio > 1.0:  state = "📊 MODERATE IMBALANCE"
    else:              state = "✅ BALANCED delta supply/demand"

    return {
        'imbalance_ratio': round(ratio, 2),
        'state':           state,
        'demand':          round(demand, 3),
        'capacity':        round(capacity, 3),
    }
