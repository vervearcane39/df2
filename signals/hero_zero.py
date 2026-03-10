"""
SIGNAL LAYER — hero_zero.py
The Hero-Zero Detection Engine.

Hero-zero trades are the specific events where options go from ₹20 → ₹200
in 15–30 minutes. These happen only when a precise set of structural conditions
align. This module detects them.

The five sub-detectors (each maps to a named doc requirement):

1. theta_collapse_clock()    — Time-decay multiplier. Last 90min = 1.5×, last 30min = 2.2×
2. oi_trap_density()         — % of chain OI that is trapped. >25% = explosive
3. spot_acceleration()       — Velocity increasing = phase change = explosion incoming
4. gamma_wall_break()        — Price crossing GEX wall = dealer hedge flip = feedback loop
5. (hedge pressure used from market/hedge_pressure.py)

Hero-zero score:
    Each component outputs 0–1.
    Combined with weights and theta multiplier.
    Threshold at 0.70 = hero-zero setup confirmed.

The output is a single HERO-ZERO SCORE (0–100) with:
    direction:   UP | DOWN
    ignition:    True if score ≥ 70
    target:      expected move target from next OI wall
    confidence:  WATCH | SETUP | ARMED | IGNITION
    components:  breakdown of each sub-score
"""
import numpy as np
import math
from datetime import datetime, time as dt_time
from typing import Optional

# P4: Adaptive ignition threshold — will be loaded from DB each cycle
# Fallback values used until enough historical data is collected
_HZ_ARMED_DEFAULT     = 65
_HZ_IGNITION_DEFAULT  = 80


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 1 — Theta Collapse Clock
# ═══════════════════════════════════════════════════════════════════════════════

# Session: 9:15 → 15:30 = 375 minutes
_SESSION_START = dt_time(9, 15)
_SESSION_END   = dt_time(15, 30)
_SESSION_MINS  = 375.0

# Theta multiplier schedule (time remaining → multiplier)
# These are calibrated to observed NIFTY weekly expiry theta curves
_THETA_SCHEDULE = [
    (90, 1.5),   # >90 min remaining → 1.5×
    (60, 1.8),   # 60–90 min remaining
    (30, 2.2),   # 30–60 min remaining
    (15, 3.0),   # 15–30 min remaining
    (0,  4.0),   # <15 min remaining
]


def theta_collapse_clock(is_expiry: bool) -> dict:
    """
    Computes the theta time-weight multiplier for the current moment.

    Non-expiry: multiplier = 1.0 (no theta acceleration)
    Expiry day: multiplier scales from 0.7 (morning) → 4.0 (final 15 min)

    Returns:
        multiplier:    float, applied to hero-zero score
        time_bucket:   label (MORNING/MIDDAY/LATE/LAST_90/LAST_30/FINAL)
        mins_to_close: minutes until 15:30
        theta_score:   0–1 normalised urgency (0 = morning, 1 = final 15min)
        label:         human-readable string
    """
    now      = datetime.now().time()
    now_mins = now.hour * 60 + now.minute
    end_mins = 15 * 60 + 30
    start_mins = 9 * 60 + 15

    mins_to_close   = max(end_mins - now_mins, 0)
    mins_from_start = max(now_mins - start_mins, 0)
    total_session   = _SESSION_MINS

    if not is_expiry:
        return {
            'multiplier': 1.0, 'time_bucket': 'NON_EXPIRY',
            'mins_to_close': mins_to_close, 'theta_score': 0.1,
            'label': '⏰ Non-expiry day',
        }

    # Find multiplier
    mult = 0.7   # before market open or early morning
    bucket = 'MORNING'
    for mins_thresh, m in _THETA_SCHEDULE:
        if mins_to_close > mins_thresh:
            mult = m
            break
    else:
        mult = 4.0

    # Time bucket labels
    if   mins_to_close > 150: bucket, mult = 'MORNING',  0.7
    elif mins_to_close > 90:  bucket, mult = 'MIDDAY',   1.0
    elif mins_to_close > 60:  bucket, mult = 'LAST_90',  1.5
    elif mins_to_close > 30:  bucket, mult = 'LAST_60',  1.8
    elif mins_to_close > 15:  bucket, mult = 'LAST_30',  2.2
    else:                     bucket, mult = 'FINAL_15', 3.0

    # Theta score: 0 (morning) → 1 (final minutes), nonlinear
    theta_score = float(np.clip(1.0 - mins_to_close / 150.0, 0.0, 1.0) ** 1.5)

    icon_map = {
        'MORNING': '🌅', 'MIDDAY': '☀️', 'LAST_90': '🔥',
        'LAST_60': '🔥🔥', 'LAST_30': '💀', 'FINAL_15': '💥',
    }
    label = f"{icon_map.get(bucket,'⏰')} {bucket} — Θ×{mult:.1f} ({mins_to_close}min to close)"

    return {
        'multiplier':    mult,
        'time_bucket':   bucket,
        'mins_to_close': mins_to_close,
        'theta_score':   round(theta_score, 3),
        'label':         label,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 2 — OI Trap Density
# ═══════════════════════════════════════════════════════════════════════════════

def oi_trap_density(merged, spot: float, trapped_ce: bool, trapped_pe: bool,
                    net_oi_chg: float) -> dict:
    """
    Measures what fraction of chain OI is trapped (at risk of forced exit).

    Trapped OI = OI at strikes where sellers are underwater.
    Method:
        CE trapped: strike < spot (call sellers in pain if spot > strike)
        PE trapped: strike > spot (put sellers in pain if spot < strike)
        Weight by OI concentration and distance from spot.

    trap_density = sum(trapped_OI) / total_chain_OI

    Returns:
        trap_density_pct:  0–100
        trap_level:        'WEAK' | 'MODERATE' | 'STRONG' | 'EXPLOSIVE'
        trapped_oi_M:      absolute trapped OI (M contracts)
        total_oi_M:        total chain OI (M contracts)
        dominant_side:     'CE' | 'PE' | 'BOTH' | 'NONE'
        trap_score:        0–1 for hero-zero scoring
    """
    if merged is None or (hasattr(merged, 'empty') and merged.empty):
        return {'trap_density_pct': 0.0, 'trap_level': 'WEAK',
                'trapped_oi_M': 0.0, 'total_oi_M': 0.0,
                'dominant_side': 'NONE', 'trap_score': 0.0}
    try:
        import pandas as pd

        total_oi = float(merged['oi_now'].sum())
        if total_oi < 1000:
            return {'trap_density_pct': 0.0, 'trap_level': 'WEAK',
                    'trapped_oi_M': 0.0, 'total_oi_M': 0.0,
                    'dominant_side': 'NONE', 'trap_score': 0.0}

        # Trapped CE: strike < spot AND significant OI (sellers underwater)
        trapped_ce_oi = float(
            merged[(merged['type_now'] == 'CE') & (merged['strike_now'] < spot)]['oi_now'].sum()
        )
        # Trapped PE: strike > spot AND significant OI
        trapped_pe_oi = float(
            merged[(merged['type_now'] == 'PE') & (merged['strike_now'] > spot)]['oi_now'].sum()
        )

        # Weight: only count if signal confirms trap (trapped_ce/pe flags from risk layer)
        ce_factor = 1.0 if trapped_ce else 0.5
        pe_factor = 1.0 if trapped_pe else 0.5
        effective_trapped = (trapped_ce_oi * ce_factor) + (trapped_pe_oi * pe_factor)

        density_pct = min(effective_trapped / total_oi * 100.0, 100.0)

        if   density_pct >= 30: level = 'EXPLOSIVE'
        elif density_pct >= 20: level = 'STRONG'
        elif density_pct >= 10: level = 'MODERATE'
        else:                   level = 'WEAK'

        # Dominant side
        if   trapped_ce_oi > trapped_pe_oi * 1.5: dom = 'CE'
        elif trapped_pe_oi > trapped_ce_oi * 1.5: dom = 'PE'
        elif trapped_ce_oi > 0 and trapped_pe_oi > 0: dom = 'BOTH'
        else: dom = 'NONE'

        # Normalised score for hero-zero
        trap_score = float(np.clip(density_pct / 30.0, 0.0, 1.0))

        return {
            'trap_density_pct': round(density_pct, 1),
            'trap_level':       level,
            'trapped_oi_M':     round(effective_trapped / 1e6, 2),
            'total_oi_M':       round(total_oi / 1e6, 2),
            'dominant_side':    dom,
            'trap_score':       round(trap_score, 3),
        }
    except Exception:
        return {'trap_density_pct': 0.0, 'trap_level': 'WEAK',
                'trapped_oi_M': 0.0, 'total_oi_M': 0.0,
                'dominant_side': 'NONE', 'trap_score': 0.0}


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 3 — Spot Acceleration Detector
# ═══════════════════════════════════════════════════════════════════════════════

_velocity_history: list = []


def push_velocity_hz(velocity: float):
    """Maintain rolling velocity buffer for acceleration computation."""
    _velocity_history.append(float(velocity))
    if len(_velocity_history) > 10:
        _velocity_history.pop(0)


def spot_acceleration(velocity: float, spot_df) -> dict:
    """
    Computes spot price acceleration = d(velocity)/dt.

    Hero-zero pattern: slow → faster → explosion.
    Acceleration detects the phase change before the explosion.

    Method:
        velocity_now = current std of last 5 spot candles
        velocity_3m_ago = std from 3–8 candles ago
        acceleration = (velocity_now - velocity_3m_ago)

    acceleration_score = clip(acceleration / avg_velocity, −1, +1)
    where +1 = strongly accelerating (explosion imminent)

    Returns:
        acceleration:       raw pts/min² change
        accel_score:        −1 to +1
        accel_level:        'DECELERATING' | 'FLAT' | 'BUILDING' | 'SURGING'
        phase:              'SLOW→FASTER' | 'FASTER→EXPLOSION' | 'STABLE' | 'SLOWING'
    """
    push_velocity_hz(velocity)
    hist = _velocity_history

    try:
        import pandas as pd
        # Compute velocity from spot_df directly (more accurate than session state)
        prices = spot_df['spot_price'].values.astype(float) if spot_df is not None and len(spot_df) > 8 else []

        if len(prices) >= 8:
            v_now = float(pd.Series(prices[-5:]).std())
            v_old = float(pd.Series(prices[-8:-3]).std())
        elif len(hist) >= 4:
            v_now = float(np.mean(hist[-2:]))
            v_old = float(np.mean(hist[-5:-2])) if len(hist) >= 5 else v_now
        else:
            v_now, v_old = velocity, velocity

        accel = v_now - v_old
        avg_v = max(float(np.mean(hist)) if hist else velocity, 0.5)
        accel_score = float(np.clip(accel / avg_v, -1.0, 1.0))

        if   accel_score >  0.5: level = 'SURGING'
        elif accel_score >  0.2: level = 'BUILDING'
        elif accel_score < -0.3: level = 'DECELERATING'
        else:                    level = 'FLAT'

        # Phase detection
        if   v_now > v_old * 1.5 and v_old < 3: phase = '🚀 SLOW→FASTER (explosion setup)'
        elif v_now > v_old * 2.0:                phase = '💥 FASTER→EXPLOSION'
        elif v_now < v_old * 0.6:                phase = '🧊 SLOWING'
        else:                                    phase = '〰️ STABLE'

        return {
            'acceleration':   round(accel, 3),
            'velocity_now':   round(v_now, 3),
            'velocity_old':   round(v_old, 3),
            'accel_score':    round(accel_score, 3),
            'accel_level':    level,
            'phase':          phase,
        }
    except Exception:
        return {
            'acceleration': 0.0, 'velocity_now': velocity, 'velocity_old': velocity,
            'accel_score': 0.0, 'accel_level': 'FLAT', 'phase': '〰️ STABLE',
        }


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 4 — Gamma Wall Break Detector
# ═══════════════════════════════════════════════════════════════════════════════

_prev_wall_side: Optional[str] = None   # 'ABOVE' | 'BELOW' | None


def gamma_wall_break(spot: float, gex_wall: int, velocity: float,
                     delta_flow_norm: float, net_gex: float,
                     HIGH_VOL: float = 8.0) -> dict:
    """
    Detects when spot breaks through a GEX wall — the ignition point.

    When price breaks a gamma wall, dealer hedging flips direction:
        Previously selling to suppress → now forced to buy
    This creates a feedback loop: the exact start of hero-zero moves.

    Logic:
        near_wall = abs(spot - gex_wall) <= 25
        crossing  = spot crossed wall level in this tick
        confirmed = near_wall AND velocity > threshold AND delta_flow confirms

    Break types:
        APPROACHING: within 25pts, not yet broken
        BREAKING:    velocity > HIGH_VOL, at the wall, with directional flow
        BROKEN:      already past wall, sustained move away

    Returns:
        wall_break:     True if confirmed break
        break_type:     'APPROACHING' | 'BREAKING' | 'BROKEN' | None
        break_direction:'UP' | 'DOWN' | None
        wall_distance:  pts from gex_wall
        break_score:    0–1 for hero-zero scoring
        alert:          human-readable string
    """
    global _prev_wall_side

    dist = abs(spot - gex_wall)
    side = 'ABOVE' if spot > gex_wall else 'BELOW'

    # Detect crossing
    crossed = (_prev_wall_side is not None and _prev_wall_side != side)
    _prev_wall_side = side

    # Near-wall condition
    near = dist <= 25
    at_wall = dist <= 50

    # Directional confirmation
    direction = 'UP' if spot > gex_wall else 'DOWN'
    flow_confirms = (
        (direction == 'UP'   and delta_flow_norm > 0.2) or
        (direction == 'DOWN' and delta_flow_norm < -0.2)
    )
    vel_confirms = velocity > HIGH_VOL * 0.7

    # Short gamma amplifies breaks
    short_gamma = net_gex < -1.5

    # Alert logic
    wall_break  = False
    break_type  = None
    break_score = 0.0
    alert       = None

    if crossed and flow_confirms:
        wall_break  = True
        break_type  = 'BROKEN'
        break_score = 1.0
        alert = (f"🚨 GAMMA WALL {'BROKEN ↑' if direction=='UP' else 'BROKEN ↓'} "
                 f"at {gex_wall} — dealers hedging flips {'BUY' if direction=='UP' else 'SELL'}"
                 + (" | SHORT γ AMPLIFYING" if short_gamma else ""))

    elif near and vel_confirms and flow_confirms:
        wall_break  = False
        break_type  = 'BREAKING'
        break_score = 0.80
        alert = (f"⚠️ GAMMA WALL BREAKING — spot {dist:.0f}pts from {gex_wall}, "
                 f"velocity {velocity:.1f}, flow {delta_flow_norm:+.2f}")

    elif at_wall and vel_confirms:
        break_type  = 'APPROACHING'
        break_score = 0.45
        alert = f"🔶 APPROACHING GAMMA WALL {gex_wall} ({dist:.0f}pts) — watch for break"

    elif near:
        break_type  = 'APPROACHING'
        break_score = 0.25

    return {
        'wall_break':       wall_break,
        'break_type':       break_type,
        'break_direction':  direction if break_type else None,
        'wall_distance':    round(dist, 1),
        'break_score':      round(break_score, 3),
        'alert':            alert,
        'short_gamma':      short_gamma,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# HERO-ZERO COMPOSITE SCORER
# ═══════════════════════════════════════════════════════════════════════════════

# Component weights (sum = 1.0)
# Theta clock is a multiplier, not an additive component
HZ_WEIGHTS = {
    'wall_break':    0.25,   # ignition trigger — highest weight
    'trap_density':  0.22,   # trapped sellers = explosive fuel
    'hedge_pressure':0.20,   # dealer forced-buy/sell rate
    'acceleration':  0.18,   # momentum phase change
    'vacuum':        0.15,   # no friction = fast travel
}


def compute_hero_zero_score(
    # Sub-detector outputs
    theta_r:    dict,
    trap_r:     dict,
    accel_r:    dict,
    wall_r:     dict,
    # From existing modules
    pressure_level: str,
    pressure_vel:   float,
    in_vacuum:  bool,
    vacuum_severity: float,
    # Context
    net_gex:    float,
    move_prob:  float,
    delta_flow_norm: float,
    is_expiry:  bool,
    spot:       float,
    next_wall_above: Optional[int],
    next_wall_below: Optional[int],
    direction_bias:  str,
    # dGamma/dSpot — final layer
    gconv_score:    float = 0.0,   # 0–1
    gconv_regime:   str   = 'DAMPENING',
    # Flow persistence — state-aware intelligence (Final Upgrade)
    persistence_r:  dict  = None,
) -> dict:
    """
    Combines all hero-zero components into a single 0–100 score.
    dGamma/dSpot feeds in as a direct amplifier: when convexity is EXPLOSIVE,
    the gamma_bonus is doubled and the base score is lifted.

    persistence_r feeds in as a duration-aware multiplier:
        compression lasting 18 min → ×1.60
        delta flow persistent 12 min → ×1.20
        Combined → ×1.92 total before theta

    This converts snapshot score → pressure-accumulation-aware score.
    """
    # ── Component scores (0–1) ─────────────────────────────────────────────
    wall_score   = wall_r['break_score']
    trap_score   = trap_r['trap_score']
    accel_score  = max(accel_r['accel_score'], 0.0)

    p_map = {'LOW': 0.1, 'MEDIUM': 0.4, 'HIGH': 0.7, 'EXTREME': 1.0}
    pressure_score = p_map.get(pressure_level, 0.1)
    pressure_score = min(pressure_score + pressure_vel * 0.03, 1.0)

    vacuum_score = 1.0 if in_vacuum else min(vacuum_severity * 2.0, 0.8)

    base = (
        HZ_WEIGHTS['wall_break']    * wall_score    +
        HZ_WEIGHTS['trap_density']  * trap_score    +
        HZ_WEIGHTS['hedge_pressure']* pressure_score+
        HZ_WEIGHTS['acceleration']  * accel_score   +
        HZ_WEIGHTS['vacuum']        * vacuum_score
    )

    mp_bonus = float(np.clip((move_prob - 50) / 200.0, 0.0, 0.15))

    # Short gamma bonus — doubled when convexity is EXPLOSIVE
    # (dGamma/dSpot tells us gamma is actively accelerating, not just negative)
    if   gconv_regime == 'EXPLOSIVE' and net_gex < -1.5: gamma_bonus = 0.16
    elif gconv_regime == 'EXPLOSIVE':                     gamma_bonus = 0.10
    elif net_gex < -3.0:                                  gamma_bonus = 0.08
    elif net_gex < -1.5:                                  gamma_bonus = 0.04
    else:                                                  gamma_bonus = 0.0

    # dGamma/dSpot convexity bonus
    gconv_bonus = gconv_score * 0.06

    raw_score = (base + mp_bonus + gamma_bonus + gconv_bonus) * 100.0

    # ── Upgrade 4: Convexity amplification ───────────────────────────────
    if gconv_regime == 'EXPLOSIVE':
        raw_score = raw_score * 1.25
    elif gconv_regime == 'LINEAR':
        raw_score = raw_score * 1.08

    # ── FINAL UPGRADE: Persistence multiplier ────────────────────────────
    # Applied AFTER convexity boost but BEFORE theta (so theta compounds it).
    # This is the "pressure vessel" detection:
    #   raw snapshot score = 45
    #   compression lasted 22 min → persist_mult = 1.60
    #   convexity EXPLOSIVE        → raw × 1.25
    #   combined before theta:       45 × 1.60 × 1.25 = 90
    #   theta in LAST_30:            ×2.3
    #   final hz_score:              ~97 IGNITION
    #
    # Without persistence: same 22-min compression = 56 after theta.
    # This is the architectural difference.
    _persist_mult = 1.0
    if persistence_r is not None:
        _persist_mult = float(persistence_r.get('persist_mult', 1.0))
        # Additive pressure score bonus (up to +10 pts)
        _pressure_bonus = float(persistence_r.get('pressure_score', 0.0)) * 0.10
        raw_score = raw_score * _persist_mult + _pressure_bonus

    raw_score = float(np.clip(raw_score, 0.0, 95.0))  # cap at 95 before theta

    theta_mult = theta_r['multiplier']

    if raw_score > 20:
        hz_score = float(100.0 - (100.0 - raw_score) / theta_mult)
    else:
        hz_score = raw_score * (theta_mult * 0.5)

    hz_score = float(np.clip(hz_score, 0.0, 100.0))

    hz_score = float(np.clip(hz_score, 0.0, 100.0))

    # ── Direction ──────────────────────────────────────────────────────────
    if   wall_r['break_direction'] == 'UP':   direction = 'UP'
    elif wall_r['break_direction'] == 'DOWN':  direction = 'DOWN'
    elif delta_flow_norm > 0.25:              direction = 'UP'
    elif delta_flow_norm < -0.25:             direction = 'DOWN'
    elif direction_bias in ('BULLISH', 'STRONG_BULL', 'MILD_BULL'): direction = 'UP'
    elif direction_bias in ('BEARISH', 'STRONG_BEAR', 'MILD_BEAR'): direction = 'DOWN'
    else:                                      direction = 'NEUTRAL'

    # ── Target — Vacuum-extended target selection  (Fix for Problem 4) ────
    # In a liquidity vacuum, the first OI wall is NOT the target.
    # The market will TELEPORT through it because there is no friction.
    # In vacuum: skip the nearest wall, target the NEXT wall beyond it.
    # This matches what actually happens in hero-zero moves.
    if direction == 'UP':
        if in_vacuum and vacuum_severity > 0.4 and next_wall_above is not None:
            # Add expected vacuum distance (roughly 1.5× the gap to first wall)
            gap = next_wall_above - spot
            target = int(next_wall_above + gap * 0.5)   # extended target
        else:
            target = next_wall_above
    elif direction == 'DOWN':
        if in_vacuum and vacuum_severity > 0.4 and next_wall_below is not None:
            gap = spot - next_wall_below
            target = int(next_wall_below - gap * 0.5)
        else:
            target = next_wall_below
    else:
        target = None

    # Vacuum also boosts the overall hz_score when confirmed (P4)
    if in_vacuum and vacuum_severity > 0.5:
        hz_score = float(min(hz_score + vacuum_severity * 8.0, 100.0))

    # ── Confidence tiers (P4: adaptive threshold) ─────────────────────────
    # Load adaptive thresholds. Falls back to defaults until data is available.
    # Import here to avoid circular import at module load time.
    try:
        from signals.calibration import get_adaptive_hz_threshold
        _thresholds = get_adaptive_hz_threshold('market_data.db')
        _armed_t    = _thresholds['armed_threshold']
        _ignition_t = _thresholds['ignition_threshold']
    except Exception:
        _armed_t    = _HZ_ARMED_DEFAULT
        _ignition_t = _HZ_IGNITION_DEFAULT

    if   hz_score >= _ignition_t and is_expiry: confidence = 'IGNITION'
    elif hz_score >= _armed_t:                  confidence = 'ARMED'
    elif hz_score >= (_armed_t - 15):           confidence = 'SETUP'
    elif hz_score >= 30:                        confidence = 'WATCH'
    else:                                       confidence = 'DORMANT'

    ignition = (confidence == 'IGNITION') or (hz_score >= _armed_t and wall_r['wall_break'])

    # ── 5 Core Conditions — the only ones that actually predict explosions ──
    # Based on: Short Gamma + Hedge Pressure + Vacuum + Flow + Compression
    # Each is independently meaningful. Score = 0/5 to 5/5.
    cond_short_gamma   = net_gex < 0.0
    cond_hedge_pressure= pressure_level in ('HIGH', 'EXTREME')
    cond_vacuum        = in_vacuum
    cond_directional   = abs(delta_flow_norm) > 0.3
    cond_compression   = (accel_r['accel_level'] in ('BUILDING', 'SURGING') and
                          wall_r['break_type'] in ('BREAKING', 'BROKEN', 'INTACT'))

    core_conditions = {
        'short_gamma':    cond_short_gamma,
        'hedge_pressure': cond_hedge_pressure,
        'vacuum':         cond_vacuum,
        'directional':    cond_directional,
        'compression':    cond_compression,
    }
    conditions_met = sum(core_conditions.values())

    # Condition labels for display
    cond_labels = {
        'short_gamma':    f"{'✅' if cond_short_gamma   else '⬜'} Short Gamma ({net_gex:.1f}M)",
        'hedge_pressure': f"{'✅' if cond_hedge_pressure else '⬜'} Hedge Pressure ({pressure_level})",
        'vacuum':         f"{'✅' if cond_vacuum         else '⬜'} Liquidity Vacuum",
        'directional':    f"{'✅' if cond_directional    else '⬜'} Directional Flow ({delta_flow_norm:+.2f})",
        'compression':    f"{'✅' if cond_compression    else '⬜'} Compression Break ({accel_r['accel_level']})",
    }

    # ── Active conditions for display (legacy — additional context) ──────
    active = []
    if wall_r['break_type'] in ('BREAKING', 'BROKEN'):  active.append(f"✓ Gamma wall {wall_r['break_type'].lower()}")
    if trap_r['trap_density_pct'] >= 20:                 active.append(f"✓ Trap density {trap_r['trap_density_pct']:.0f}%")
    if pressure_level in ('HIGH', 'EXTREME'):            active.append(f"✓ Hedge pressure {pressure_level.lower()}")
    if accel_r['accel_level'] in ('BUILDING', 'SURGING'): active.append(f"✓ Acceleration {accel_r['accel_level'].lower()}")
    if in_vacuum:                                         active.append("✓ Liquidity vacuum")
    if theta_r['time_bucket'] in ('LAST_30', 'FINAL_15'): active.append(f"✓ {theta_r['label']}")
    if net_gex < -2.0:                                   active.append(f"✓ Short γ {net_gex:.1f}M")

    # ── Interpretation ──────────────────────────────────────────────────────
    interp_map = {
        'IGNITION': f"🔥 HERO-ZERO IGNITION — {direction} to {target or '?'}",
        'ARMED':    f"💣 ARMED — {len(active)} conditions. {'Break imminent' if wall_r['break_type'] == 'BREAKING' else 'Waiting for trigger'}",
        'SETUP':    f"⚡ SETUP FORMING — {len(active)} conditions active",
        'WATCH':    f"👁️ WATCH — {len(active)} conditions",
        'DORMANT':  "💤 DORMANT — no hero-zero conditions",
    }

    return {
        'hz_score':          round(hz_score, 1),
        'ignition':          ignition,
        'confidence':        confidence,
        'direction':         direction,
        'target':            target,
        'interpretation':    interp_map[confidence],
        'conditions_met':    conditions_met,
        'core_conditions':   core_conditions,
        'cond_labels':       cond_labels,
        'active_conditions': active,
        'components': {
            'wall_break':     round(wall_score, 3),
            'trap_density':   round(trap_score, 3),
            'hedge_pressure': round(pressure_score, 3),
            'acceleration':   round(accel_score, 3),
            'vacuum':         round(vacuum_score, 3),
        },
        'persist_mult':      round(_persist_mult, 3),   # final upgrade
        'theta_mult':        theta_mult,
        'theta_bucket':      theta_r['time_bucket'],
        'armed_threshold':   _armed_t,
        'ignition_threshold':_ignition_t,
        'theta_label':       theta_r['label'],
        'wall_r':            wall_r,
        'trap_r':            trap_r,
        'accel_r':           accel_r,
        'theta_r':           theta_r,
    }