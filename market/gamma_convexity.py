"""
MARKET STATE LAYER — gamma_convexity.py
dGamma/dSpot — The "Speed" Greek — Dealer Gamma Hedging Pressure

This is the final missing layer.

──────────────────────────────────────────────────────────────────────────────
What hedge_pressure.py already computes:
    dHedge/dSpot = Σ( OI × gamma × lot × dealer_sign )

    → "If spot moves 1pt, dealers must trade X futures"
    → This is the SIZE of the hedging requirement

What this module adds:
    dGamma/dSpot = Σ( OI × speed × lot × dealer_sign )

    where speed = d(gamma)/d(spot) for each option

    → "As spot moves, the hedging requirement itself is ACCELERATING"
    → This is the RATE OF CHANGE of that requirement
    → This is what makes moves explosive rather than linear

──────────────────────────────────────────────────────────────────────────────
Why this matters for hero-zero trades:

    Normal move (low dGamma/dSpot):
        spot +10 → dealers buy 50 lots
        spot +20 → dealers buy 50 lots
        spot +30 → dealers buy 50 lots     ← linear, manageable

    Explosive move (high dGamma/dSpot):
        spot +10 → dealers buy 50 lots
        spot +20 → dealers buy 120 lots    ← gamma growing
        spot +30 → dealers buy 280 lots    ← gamma accelerating
        spot +40 → dealers buy 600 lots    ← feedback loop, unmanageable

    The second scenario is what causes ₹20 → ₹200 in 15 minutes.
    dGamma/dSpot detects which scenario you are in BEFORE the move starts.

──────────────────────────────────────────────────────────────────────────────
The Speed Greek (Third Derivative):

    Option price:  V(S)
    Delta:         dV/dS
    Gamma:         d²V/dS²
    Speed:         d³V/dS³  =  dGamma/dSpot

Closed-form for a vanilla option:
    speed = -gamma × [ d1/(S × σ × √T) + 1/S ]

    where d1 = (ln(S/K) + (r + σ²/2) × T) / (σ × √T)
          gamma = N'(d1) / (S × σ × √T)

Speed is negative for OTM options (gamma decreasing as spot moves away)
Speed is positive for options approaching ATM (gamma increasing as spot approaches)

The SIGN of speed at the current spot level tells you:
    Positive dGamma/dSpot (chain level): 
        gamma increasing → hedging ACCELERATES on moves → explosive
    Negative dGamma/dSpot (chain level):
        gamma decreasing → hedging DECELERATES → self-limiting

──────────────────────────────────────────────────────────────────────────────
Outputs:
    chain_speed:        total dGamma/dSpot across chain (signed, M/pt²)
    speed_by_strike:    per-strike contribution map
    convexity_regime:   EXPLOSIVE | LINEAR | DAMPENING
    explosive_radius:   pt range around spot where speed > threshold
    acceleration_alert: triggered when chain_speed flips sign or surges
    speed_score:        0–1, used by hero_zero and move_probability engines
"""
import numpy as np
import math
import pandas as pd
from typing import Optional

from features.greeks import NIFTY_LOT_SIZE, RISK_FREE_RATE


# ── Core: Speed Greek (d³V/dS³) ──────────────────────────────────────────────

def bs_speed(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """
    Speed = d(gamma)/d(spot) for a vanilla option.

    Formula:
        speed = -gamma × [ d1 / (S × σ × √T) + 1/S ]

    This is the same for both CE and PE (speed does not depend on option type).
    Negative speed = gamma decreasing as spot moves (OTM direction)
    Positive speed = gamma increasing as spot moves (ATM direction, approaching)

    Returns 0 on bad inputs rather than raising.
    """
    try:
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return 0.0
        sqrtT  = math.sqrt(T)
        d1     = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT)
        npd1   = math.exp(-0.5 * d1 ** 2) / math.sqrt(2 * math.pi)   # N'(d1)
        gamma  = npd1 / (S * sigma * sqrtT)
        speed  = -gamma * (d1 / (S * sigma * sqrtT) + 1.0 / S)
        return float(speed)
    except (ZeroDivisionError, ValueError, OverflowError):
        return 0.0


# ── Chain-Level dGamma/dSpot ──────────────────────────────────────────────────

def compute_gamma_convexity(
    merged:   pd.DataFrame,
    spot:     float,
    dte:      float,
    atm_iv:   float,
    radius:   int = 500,
) -> dict:
    """
    Computes the total chain-level dGamma/dSpot and classifies the
    convexity regime.

    Method:
        For each strike within radius:
            speed_i      = bs_speed(spot, strike_i, dte, r, iv_i)
            dealer_sign  = from oi_flow_type column (-1 dealer short, +1 long)
            contribution = -dealer_sign × speed_i × OI_i × lot_size

        The negative dealer_sign is because:
            Dealer SHORT option → they own the negative of the option's speed
            → when speed is negative (OTM), their gamma is INCREASING as spot moves toward them
            → this creates the feedback loop

        chain_speed = Σ(contributions) in units of M futures / pt²

    Regime classification:
        chain_speed > +0.010 M/pt²  → EXPLOSIVE  (gamma accelerating, feedback loop)
        chain_speed > +0.003 M/pt²  → LINEAR      (modest acceleration)
        chain_speed ≤  0.003 M/pt²  → DAMPENING   (gamma decelerating, self-limiting)

    Returns:
        chain_speed:       float (M futures/pt²), signed
        chain_speed_abs:   absolute value
        convexity_regime:  'EXPLOSIVE' | 'LINEAR' | 'DAMPENING'
        speed_by_strike:   {strike: speed_contribution_M}
        speed_score:       0–1, saturates at 0.03 M/pt²
        explosive_strikes: strikes where |contribution| > 20% of chain total
        acceleration_note: human-readable interpretation
        up_convexity:      chain_speed on hypothetical spot+25 (directional bias)
        dn_convexity:      chain_speed on hypothetical spot-25
        dominant_direction:'UP' | 'DOWN' | 'SYMMETRIC'
    """
    empty = {
        'chain_speed': 0.0, 'chain_speed_abs': 0.0,
        'convexity_regime': 'DAMPENING', 'speed_by_strike': {},
        'speed_score': 0.0, 'explosive_strikes': [],
        'acceleration_note': '—', 'up_convexity': 0.0,
        'dn_convexity': 0.0, 'dominant_direction': 'SYMMETRIC',
    }

    if merged is None or (hasattr(merged, 'empty') and merged.empty) or dte <= 0:
        return empty

    try:
        iv_safe  = max(float(atm_iv), 0.05)
        near     = merged[abs(merged['strike_now'] - spot) <= radius].copy()
        if near.empty:
            return empty

        by_strike: dict = {}
        chain_total = 0.0

        for _, row in near.iterrows():
            strike  = int(row.get('strike_now', 0))
            oi_now  = max(float(row.get('oi_now', 0)), 0.0)
            if oi_now < 100:
                continue

            # Per-strike IV: linear skew approximation
            moneyness = (strike - spot) / max(spot, 1.0)
            iv_s      = max(iv_safe * (1.0 + 0.5 * abs(moneyness)), 0.05)

            spd = bs_speed(spot, strike, dte, RISK_FREE_RATE, iv_s)

            # Dealer sign from price-OI flow
            flow   = str(row.get('oi_flow_type', 'NEUTRAL'))
            dsign  = -1.0 if flow in ('LONG_BUILD', 'SHORT_COVER') else 1.0

            # Contribution: dealers short options → they carry -speed
            # When they need to re-hedge: dHedge/dSpot changes at rate = speed
            contribution = -dsign * spd * oi_now * NIFTY_LOT_SIZE

            if strike not in by_strike:
                by_strike[strike] = 0.0
            by_strike[strike] += contribution
            chain_total        += contribution

        chain_M  = chain_total / 1e7   # convert to M futures / pt²
        chain_abs = abs(chain_M)

        # Regime
        if   chain_M > 0.010:  regime = 'EXPLOSIVE'
        elif chain_M > 0.003:  regime = 'LINEAR'
        else:                   regime = 'DAMPENING'

        # Speed score (saturates at 0.030 M/pt²)
        speed_score = float(np.clip(chain_abs / 0.030, 0.0, 1.0))

        # Explosive strikes (>15% of chain total)
        total_abs   = sum(abs(v) for v in by_strike.values())
        exp_strikes = [s for s, v in by_strike.items()
                       if total_abs > 0 and abs(v) / total_abs > 0.15]

        # Directional convexity: evaluate chain_speed at spot±25
        # A positive up_convexity > dn_convexity means upside moves will accelerate more
        up_conv = _chain_speed_at(near, spot + 25, dte, iv_safe) / 1e7
        dn_conv = _chain_speed_at(near, spot - 25, dte, iv_safe) / 1e7

        if   up_conv > dn_conv + 0.002: dom_dir = 'UP'
        elif dn_conv > up_conv + 0.002: dom_dir = 'DOWN'
        else:                            dom_dir = 'SYMMETRIC'

        # Human readable
        note_map = {
            'EXPLOSIVE': (
                f"🔥 EXPLOSIVE CONVEXITY — each 1pt move forces {chain_M*1000:.1f}k more futures/pt. "
                f"Dominant: {dom_dir}. Hero-zero risk is HIGH."),
            'LINEAR': (
                f"📈 LINEAR HEDGING — steady dealer flow. "
                f"Moves will be directional but not self-reinforcing."),
            'DAMPENING': (
                f"🧊 DAMPENING CONVEXITY — dealer gamma shrinks as price moves. "
                f"Moves are self-limiting. Range environment."),
        }
        note = note_map[regime]

        return {
            'chain_speed':       round(chain_M, 6),
            'chain_speed_abs':   round(chain_abs, 6),
            'convexity_regime':  regime,
            'speed_by_strike':   {s: round(v/1e7, 6) for s, v in by_strike.items()},
            'speed_score':       round(speed_score, 3),
            'explosive_strikes': exp_strikes,
            'acceleration_note': note,
            'up_convexity':      round(up_conv, 6),
            'dn_convexity':      round(dn_conv, 6),
            'dominant_direction': dom_dir,
        }

    except Exception:
        return empty


def _chain_speed_at(near: pd.DataFrame, hypo_spot: float,
                    dte: float, iv_safe: float) -> float:
    """Evaluate total chain speed at a hypothetical spot level."""
    total = 0.0
    for _, row in near.iterrows():
        strike = int(row.get('strike_now', 0))
        oi_now = max(float(row.get('oi_now', 0)), 0.0)
        if oi_now < 100:
            continue
        moneyness = (strike - hypo_spot) / max(hypo_spot, 1.0)
        iv_s      = max(iv_safe * (1.0 + 0.5 * abs(moneyness)), 0.05)
        spd       = bs_speed(hypo_spot, strike, dte, RISK_FREE_RATE, iv_s)
        flow      = str(row.get('oi_flow_type', 'NEUTRAL'))
        dsign     = -1.0 if flow in ('LONG_BUILD', 'SHORT_COVER') else 1.0
        total    += -dsign * spd * oi_now * NIFTY_LOT_SIZE
    return total
