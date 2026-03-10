"""
MARKET STATE LAYER — move_probability.py
The Move Probability Engine.

This is the core upgrade described in the document.
It unifies all signals into a single probability:
    "What is the probability that a large move happens in the next 5–30 minutes?"

Design principle:
    NOT a linear sum of scores.
    Uses a NONLINEAR model where:
        - each driver has a base contribution
        - regime interactions create multiplicative explosions
        - confidence from multiple confirmations compounds non-linearly

Formula (simplified):
    base_prob = Σ(driver_weight × driver_score) / total_max
    regime_mult = dealer_regime_multiplier
    confirmation_bonus = (N_confirming ** 1.5) / max_bonus   ← nonlinear
    final_prob = clip(base_prob × regime_mult + confirmation_bonus, 0, 100)

Output:
    explosion_prob: 0–100 probability of a large move
    direction:      'UP' | 'DOWN' | 'NEUTRAL'
    target_zone:    nearest OI wall in the expected direction
    drivers:        list of active drivers with their contributions
    interpretation: human-readable sentence
    confidence:     'LOW' | 'MEDIUM' | 'HIGH' | 'EXTREME'
"""
import math
import numpy as np
from typing import Optional, List, Dict


# ── Driver weights (sum to 100) ───────────────────────────────────────────────
# Based on historical importance in options market microstructure:
# Gamma regime is the most important structural factor (controls dealer hedging)
# Cascade is high-conviction (requires multiple conditions simultaneously)
# Vacuum enables fast moves (no friction)
# Delta flow shows real-money direction
# Dealer regime is the overall envelope

# P3 Fix (2025-03): Cascade removed as independent driver.
# Cascade = (short gamma) + (vacuum) + (directional flow) all firing together.
# Treating it as a separate 18-weight signal was counting those 3 signals twice.
# It now acts as a CONFIRMATION MULTIPLIER (×1.25 max) applied to the base score.
# gamma_convexity reduced from 11→6: semi-derivative of gamma, not independent.
# Weights redistributed to the 6 truly independent structural signals.
DRIVER_WEIGHTS = {
    'gamma_regime':    28,   # core structural factor — dealer amplify vs dampen
    'vacuum':          20,   # liquidity friction — independent of gamma direction
    'delta_flow':      18,   # net real-money demand — observable, independent
    'dealer_pressure': 15,   # forced hedging magnitude — observable, independent
    'futures_flow':    10,   # institutional directional positioning
    'gamma_convexity':  6,   # dGamma/dSpot — acceleration modifier (semi-derivative)
    'sensitivity':      3,   # explosion zone proximity — low weight, proximity bonus
}
# Total = 100  |  cascade removed — see _cascade_multiplier() below


def _gamma_score(net_gex: float) -> float:
    """Short gamma = higher move probability. Long gamma = lower."""
    if   net_gex < -5.0: return 1.0
    elif net_gex < -3.0: return 0.85
    elif net_gex < -1.5: return 0.65
    elif net_gex < 0:    return 0.40
    elif net_gex < 2.0:  return 0.20   # long gamma = mild suppression
    elif net_gex < 4.0:  return 0.10
    else:                return 0.05   # strong pinning → very low move prob


def _cascade_multiplier(is_cascade: bool, strength: int) -> float:
    """
    Cascade is NOT an independent signal — it is (short_gamma + vacuum + flow)
    all confirming simultaneously. So it should MULTIPLY the base score, not add.
    Max ×1.25 prevents runaway while still rewarding full confirmation.
    """
    if is_cascade:         return 1.25
    elif strength >= 3:    return 1.15
    elif strength >= 2:    return 1.08
    elif strength >= 1:    return 1.03
    return 1.0


def _vacuum_score(in_vacuum: bool, severity: float) -> float:
    if in_vacuum:               return 1.0
    elif severity > 0.50:       return 0.70
    elif severity > 0.35:       return 0.45
    elif severity > 0.20:       return 0.20
    return 0.0


def _delta_flow_score(norm_signal: float) -> float:
    return min(abs(norm_signal) * 1.4, 1.0)


def _dealer_pressure_score(dealer_delta_M: float, regime: str) -> float:
    base = min(abs(dealer_delta_M) / 3.0, 1.0)   # saturates at 3M
    if regime == 'PANIC_HEDGING':   return 1.0
    elif regime == 'AMPLIFICATION': return min(base * 1.5, 1.0)
    elif regime == 'SUPPRESSION':   return base * 0.3   # suppressor damps
    return base


def _futures_score(futures_signal: float, vol_surge: bool) -> float:
    base = min(abs(futures_signal), 1.0)
    return min(base + (0.25 if vol_surge else 0.0), 1.0)


def _sensitivity_score(spot: float, explosion_zones: list) -> float:
    """Higher score when spot is near a gamma explosion zone."""
    if not explosion_zones:
        return 0.0
    nearest = min(abs(spot - z) for z in explosion_zones)
    if   nearest < 25:  return 1.0
    elif nearest < 75:  return 0.65
    elif nearest < 150: return 0.35
    return 0.0


def compute_move_probability(
    # Gamma
    net_gex:          float,
    # Cascade
    is_cascade:       bool,
    cascade_strength: int,
    cascade_type:     Optional[str],
    # Vacuum
    in_vacuum:        bool,
    vacuum_severity:  float,
    next_wall_above:  Optional[int],
    next_wall_below:  Optional[int],
    # Delta flow
    delta_flow_norm:  float,
    # Dealer
    dealer_delta_M:   float,
    dealer_regime:    str,
    dealer_regime_mult: float,
    # Futures
    futures_signal:   float,
    futures_surge:    bool,
    # Sensitivity
    spot:             float,
    explosion_zones:  list,
    # Context
    trapped_ce:       bool,
    trapped_pe:       bool,
    spot_trend:       str,
    imbalance_ratio:  float,
    # dGamma/dSpot
    gconv_score:      float = 0.0,   # 0–1 from compute_gamma_convexity
    gconv_regime:     str   = 'DAMPENING',
) -> dict:
    """
    Unified Move Probability Engine.

    Returns a 0–100 probability that a large move occurs in the next 5–30 min,
    along with directional bias and target zone.
    """
    # ── 1. Driver scores ──────────────────────────────────────────────────
    # gamma_convexity score: EXPLOSIVE regime gets full score, LINEAR gets 0.5
    gc_score = gconv_score if gconv_regime == 'EXPLOSIVE' else (
               gconv_score * 0.5 if gconv_regime == 'LINEAR' else 0.0)

    scores = {
        'gamma_regime':    _gamma_score(net_gex),
        'vacuum':          _vacuum_score(in_vacuum, vacuum_severity),
        'delta_flow':      _delta_flow_score(delta_flow_norm),
        'dealer_pressure': _dealer_pressure_score(dealer_delta_M, dealer_regime),
        'gamma_convexity': gc_score,
        'futures_flow':    _futures_score(futures_signal, futures_surge),
        'sensitivity':     _sensitivity_score(spot, explosion_zones),
    }

    # ── 2. Weighted base probability ──────────────────────────────────────
    base = sum(scores[k] * DRIVER_WEIGHTS[k] for k in scores)

    # Cascade multiplier: confirms that gamma + vacuum + flow are all aligned.
    # Applied to base BEFORE interaction multipliers so they compound correctly.
    casc_mult = _cascade_multiplier(is_cascade, cascade_strength)
    base = base * casc_mult

    # ── 3. MULTIPLICATIVE INTERACTIONS (Fix for Problem 3) ───────────────
    #
    # The critique: "Real interactions like gamma + vacuum should MULTIPLY,
    # not add. A short gamma market in a vacuum is not gamma_score + vacuum_score.
    # It is gamma_score × vacuum_score — exponentially more dangerous."
    #
    # Implementation: compute interaction multipliers for key correlated pairs.
    # Applied AFTER the base sum so they amplify the total rather than replacing it.
    #
    # Interaction pairs and their logic:
    #   gamma × vacuum:         Short gamma + no OI friction = uncontrolled dealer hedging
    #   gamma × convexity:      Short gamma + accelerating gamma = feedback loop
    #   cascade × vacuum:       Cascade in a vacuum = fastest possible move
    #   dealer_pressure × cascade: Dealer panic + cascade confirmation = explosive

    interaction_mult = 1.0

    gs = scores['gamma_regime']
    vs = scores['vacuum']
    gc = scores['gamma_convexity']
    dp = scores['dealer_pressure']
    # cascade is now a multiplier (casc_mult), not a score — derive a 0–1 proxy
    cs = (casc_mult - 1.0) / 0.25   # 0 when no cascade, 1.0 when full cascade (×1.25)

    # gamma × vacuum: both > 0.5 = multiplicative boost
    if gs > 0.5 and vs > 0.5:
        interaction_mult *= (1.0 + gs * vs * 0.40)

    # gamma × convexity: dealer gamma accelerating in short-gamma env
    if gs > 0.5 and gc > 0.5:
        interaction_mult *= (1.0 + gs * gc * 0.30)

    # cascade × vacuum: cascade inside a liquidity vacuum = fastest move
    if cs > 0.7 and vs > 0.5:
        interaction_mult *= (1.0 + cs * vs * 0.35)

    # triple: gamma + vacuum + cascade = maximum explosive scenario
    if gs > 0.6 and vs > 0.5 and cs > 0.6:
        interaction_mult *= 1.15

    # Suppression interaction: long gamma + low vacuum = dampening
    if gs < 0.2 and vs < 0.2:
        interaction_mult *= 0.70

    # Cap interaction at 2.5× to prevent runaway
    interaction_mult = float(min(interaction_mult, 2.5))

    base_interacted = base * interaction_mult
    # ── 3. Regime multiplier (nonlinear amplification) ────────────────────
    if dealer_regime == 'PANIC_HEDGING':
        prob_raw = 100.0 - (100.0 - base_interacted) * (1.0 / dealer_regime_mult)
    elif dealer_regime == 'AMPLIFICATION':
        prob_raw = base_interacted * dealer_regime_mult * 0.75
    elif dealer_regime == 'SUPPRESSION':
        prob_raw = base_interacted * dealer_regime_mult
    else:
        prob_raw = base_interacted

    # ── 4. Confirmation bonus (nonlinear — multiple confirming = exponential) ──
    # Confirmation bonus: genuinely independent signals agreeing.
    # Only count signals that are in DRIVER_WEIGHTS (no cascade double-count).
    # Cap at 3 independent confirmations — beyond that is likely correlated.
    confirming = [k for k, v in scores.items() if v > 0.5]
    n = min(len(confirming), 3)   # cap at 3 to limit correlation inflation
    confirm_bonus = min((n ** 1.5) * 2.0, 10.0) if n >= 2 else 0.0
    # Extra bonus if cascade confirms (it represents all 3 core signals aligning)
    if is_cascade:
        confirm_bonus = min(confirm_bonus + 5.0, 12.0)
    prob_raw += confirm_bonus

    # ── 5. Additional structural bonuses ──────────────────────────────────
    if trapped_ce and delta_flow_norm > 0.3:   prob_raw += 5.0
    if trapped_pe and delta_flow_norm < -0.3:  prob_raw += 5.0
    if imbalance_ratio > 2.0:                  prob_raw += 5.0

    prob = float(np.clip(prob_raw, 0.0, 100.0))

    # ── 6. Direction ──────────────────────────────────────────────────────
    bull_score = 0.0
    bear_score = 0.0

    # Delta flow
    if delta_flow_norm > 0.1:  bull_score += abs(delta_flow_norm) * 3
    elif delta_flow_norm < -0.1: bear_score += abs(delta_flow_norm) * 3
    # Futures
    if futures_signal > 0.1:   bull_score += futures_signal * 2
    elif futures_signal < -0.1: bear_score += abs(futures_signal) * 2
    # Trapped sellers
    if trapped_ce:  bull_score += 2
    if trapped_pe:  bear_score += 2
    # Dealer delta (negative = bullish squeeze)
    if dealer_delta_M < -1.0:  bull_score += 1.5
    elif dealer_delta_M > 1.0: bear_score += 1.5
    # Cascade type
    if cascade_type == 'BULL': bull_score += 3
    elif cascade_type == 'BEAR': bear_score += 3
    # Spot trend
    if spot_trend == 'BULLISH': bull_score += 0.5
    elif spot_trend == 'BEARISH': bear_score += 0.5

    margin = abs(bull_score - bear_score)
    if   bull_score > bear_score and margin > 1.0: direction = 'UP'
    elif bear_score > bull_score and margin > 1.0: direction = 'DOWN'
    else:                                           direction = 'NEUTRAL'

    # ── 7. Target zone ────────────────────────────────────────────────────
    if direction == 'UP':
        target = next_wall_above
    elif direction == 'DOWN':
        target = next_wall_below
    else:
        target = None

    # ── 8. Active drivers for display ─────────────────────────────────────
    active_drivers = []
    driver_labels = {
        'gamma_regime':    "Short gamma regime",
        'vacuum':          "Liquidity vacuum",
        'delta_flow':      f"δ-flow {'bull' if delta_flow_norm > 0 else 'bear'} ({delta_flow_norm:+.2f})",
        'dealer_pressure': f"Dealer {dealer_regime.lower().replace('_',' ')}",
        'gamma_convexity': f"dγ/dS {gconv_regime.lower()} — hedging accelerating",
        'futures_flow':    "Futures surge" if futures_surge else "Futures directional",
        'sensitivity':     "Near explosion zone",
    }
    for k, v in sorted(scores.items(), key=lambda x: -x[1]):
        if v > 0.45:
            active_drivers.append(f"{'✓' if v > 0.7 else '~'} {driver_labels[k]}")
    if casc_mult > 1.0:
        lbl = "✓ CASCADE CONFIRMED" if is_cascade else f"~ Cascade building (str={cascade_strength})"
        active_drivers.insert(0, lbl)

    # ── 9. Confidence label ───────────────────────────────────────────────
    if   prob >= 85: confidence = 'EXTREME'
    elif prob >= 70: confidence = 'HIGH'
    elif prob >= 50: confidence = 'MEDIUM'
    elif prob >= 30: confidence = 'LOW'
    else:            confidence = 'DORMANT'

    # ── 10. Interpretation ────────────────────────────────────────────────
    interp_map = {
        'EXTREME': f"🔥 EXPLOSION RISK — {direction} {('to ' + str(target)) if target else ''}",
        'HIGH':    f"⚡ STRONG MOVE likely {direction} {('to ' + str(target)) if target else ''}",
        'MEDIUM':  f"📈 TREND possible — watch {direction} setup",
        'LOW':     "📊 Small moves — fragmented signals",
        'DORMANT': "💤 Dead market — no structural edge",
    }
    interpretation = interp_map[confidence]

    return {
        'explosion_prob':    round(prob, 1),
        'cascade_mult':      round(casc_mult, 3),
        'direction':         direction,
        'target':            target,
        'drivers':           active_drivers,
        'driver_scores':     scores,
        'confidence':        confidence,
        'interpretation':    interpretation,
        'bull_score':        round(bull_score, 2),
        'bear_score':        round(bear_score, 2),
        'confirming_n':      n,
        'confirm_bonus':     round(confirm_bonus, 1),
        'regime_mult':       dealer_regime_mult,
        'interaction_mult':  round(interaction_mult, 3),   # P3 fix: shows amplification
    }