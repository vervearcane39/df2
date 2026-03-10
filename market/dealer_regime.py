"""
MARKET STATE LAYER — dealer_regime.py
Replaces linear dealer scoring with nonlinear regime detection.

Doc Problem 1: linear +1/−1 scoring is wrong because
    short_gamma + trapped_sellers is NOT additive — it is explosive.
    The product of these conditions creates a feedback loop, not a sum.

Four dealer regimes with nonlinear interaction multipliers:

    SUPPRESSION:    dealers net long gamma → fade every move
                    Multiplier: 0.5× on all directional signals

    NEUTRAL:        no dominant hedging pressure
                    Multiplier: 1.0×

    AMPLIFICATION:  dealers net short gamma, unidirectional flow
                    Multiplier: 1.8× on primary signals

    PANIC_HEDGING:  short gamma + trapped sellers + vacuum + cascade
                    Multiplier: 3.0× — explosive nonlinear regime

The multiplier is applied to the directional conviction score in
signal_engine.py, replacing the simple linear addition.

Also computes expected hedge demand at ±50pt spot moves
(Upgrade 2 — Dealer Hedging Flow Simulator).
"""
import numpy as np
from typing import Tuple


# ── Regime classification ─────────────────────────────────────────────────────

REGIME_MULTIPLIERS = {
    'SUPPRESSION':    0.5,
    'NEUTRAL':        1.0,
    'AMPLIFICATION':  1.8,
    'PANIC_HEDGING':  3.0,
}

REGIME_LABELS = {
    'SUPPRESSION':   "📌 DEALER SUPPRESSION — fading moves",
    'NEUTRAL':       "⚖️  DEALER NEUTRAL",
    'AMPLIFICATION': "🔥 DEALER AMPLIFICATION — trending",
    'PANIC_HEDGING': "🚨 DEALER PANIC HEDGING — explosive",
}


def classify_dealer_regime(
    net_gex:        float,
    dealer_delta_M: float,
    trapped_ce:     bool,
    trapped_pe:     bool,
    in_vacuum:      bool,
    is_cascade:     bool,
    delta_flow_norm: float,
    futures_signal:  float,
) -> dict:
    """
    Nonlinear regime classification.

    Rules:
    1. PANIC_HEDGING requires all four: short gamma + trapped + vacuum + cascade
    2. AMPLIFICATION: short gamma AND two of (trapped/vacuum/directional_flow)
    3. SUPPRESSION:   long gamma (dealers absorb moves)
    4. NEUTRAL:       everything else

    Returns:
        regime:         one of the four names above
        multiplier:     float 0.5–3.0
        label:          display string
        conditions_met: list of conditions that triggered this regime
        panic_score:    0–4 count of panic conditions (for partial warnings)
    """
    is_short_gamma  = net_gex < -2.0
    is_long_gamma   = net_gex > 3.0
    is_trapped      = trapped_ce or trapped_pe
    is_directional  = abs(delta_flow_norm) > 0.3 or abs(futures_signal) > 0.3
    is_high_dd      = abs(dealer_delta_M) > 1.5

    conditions = []
    if is_short_gamma:  conditions.append("short_gamma")
    if is_trapped:      conditions.append("trapped_sellers")
    if in_vacuum:       conditions.append("liquidity_vacuum")
    if is_cascade:      conditions.append("cascade_confirmed")
    if is_directional:  conditions.append("directional_flow")
    if is_high_dd:      conditions.append("high_dealer_delta")

    panic_score = sum([is_short_gamma, is_trapped, in_vacuum, is_cascade])

    # Regime selection (priority order: PANIC > AMPLIFICATION > SUPPRESSION > NEUTRAL)
    if panic_score >= 4:
        regime = 'PANIC_HEDGING'
    elif panic_score >= 2 and is_short_gamma and is_directional:
        regime = 'AMPLIFICATION'
    elif is_long_gamma:
        regime = 'SUPPRESSION'
    else:
        regime = 'NEUTRAL'

    return {
        'regime':         regime,
        'multiplier':     REGIME_MULTIPLIERS[regime],
        'label':          REGIME_LABELS[regime],
        'conditions_met': conditions,
        'panic_score':    panic_score,
    }


# ── Hedge flow simulator (Upgrade 2) ─────────────────────────────────────────

def simulate_hedge_demand(
    merged_rows: list,   # list of (strike, otype, oi_now, delta, gamma, dealer_sign)
    spot:        float,
    delta_spot:  float,   # e.g. +50 or -50
    net_gex:     float,
) -> dict:
    """
    Simulates how much futures dealers must buy/sell if spot moves by delta_spot.

    For each option:
        New_delta(S + dS) ≈ delta + gamma × dS
        Hedge_change = (new_delta - old_delta) × OI × lot × dealer_sign

    dealer_sign: +1 = dealer long, -1 = dealer short
    (derived from price-OI flow classification stored in merged)

    Returns:
        hedge_futures_M:   total futures buying (+) or selling (−) in M
        net_direction:     'BUY' | 'SELL' | 'NEUTRAL'
        amplifying:        True if hedge direction matches spot move direction
        hedge_per_100pt:   scaled to 100pt move for comparison
    """
    from features.greeks import NIFTY_LOT_SIZE
    total_hedge = 0.0
    for strike, otype, oi_now, delta, gamma, dealer_sign in merged_rows:
        if oi_now < 100:
            continue
        delta_change = gamma * delta_spot
        # Dealers need to re-hedge: buy/sell futures proportional to delta change
        # dealer_sign: if dealers are short options (−1), they must hedge in opposite direction
        hedge = -dealer_sign * delta_change * oi_now * NIFTY_LOT_SIZE
        total_hedge += hedge

    hedge_M = total_hedge / 1e7   # convert to M futures equivalent

    # Amplifying if hedge direction == move direction
    amplifying = (hedge_M > 0 and delta_spot > 0) or (hedge_M < 0 and delta_spot < 0)
    direction  = 'BUY' if hedge_M > 0.5 else ('SELL' if hedge_M < -0.5 else 'NEUTRAL')

    return {
        'hedge_futures_M':  round(hedge_M, 2),
        'net_direction':    direction,
        'amplifying':       amplifying,
        'hedge_per_100pt':  round(hedge_M / max(abs(delta_spot), 1) * 100, 2),
    }


def build_hedge_scenarios(merged, spot: float, net_gex: float) -> dict:
    """
    Run hedge simulator for ±50pt moves.
    Returns both scenarios for display and decision use.
    """
    rows = []
    for _, row in merged.iterrows():
        strike = int(row.get('strike_now', 0))
        otype  = str(row.get('type_now', 'CE'))
        oi     = float(row.get('oi_now', 0))
        delta  = abs(float(row.get('delta', 0.5)))
        gamma  = abs(float(row.get('gamma', 0.0)))
        # Dealer sign from flow: LONG_BUILD → dealer short → sign = -1
        flow   = str(row.get('oi_flow_type', 'NEUTRAL'))
        dsign  = -1.0 if flow in ('LONG_BUILD', 'SHORT_COVER') else 1.0
        rows.append((strike, otype, oi, delta, gamma, dsign))

    up_50  = simulate_hedge_demand(rows, spot, +50, net_gex)
    dn_50  = simulate_hedge_demand(rows, spot, -50, net_gex)

    # Which direction triggers bigger cascade?
    dominant = 'UP' if abs(up_50['hedge_futures_M']) > abs(dn_50['hedge_futures_M']) else 'DOWN'

    return {
        'up_50':   up_50,
        'dn_50':   dn_50,
        'dominant_cascade_dir': dominant,
        'asymmetry': round(
            abs(up_50['hedge_futures_M']) - abs(dn_50['hedge_futures_M']), 2),
    }
