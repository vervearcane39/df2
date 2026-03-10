"""
SIGNAL LAYER — skew_dynamics.py
Upgrade 4 — IV Skew Dynamics (velocity tracking)

Your system already computes 25Δ skew.
But skew CHANGE VELOCITY often predicts moves before price moves.

Key insight from market microstructure:
    Skew rising fast → institutional downside hedging demand
    Skew collapsing fast → short covering (big upside move incoming)
    25Δ Put IV > 25Δ Call IV and rising → systematic protection buying

This module tracks:
    skew_velocity:  points/minute change in (put_IV − call_IV)
    skew_z_score:   how extreme current skew is vs recent history
    skew_alert:     triggered when velocity > threshold
    skew_signal:    normalized −1 to +1 directional signal
"""
import numpy as np
from collections import deque
from typing import Optional


_skew_history: deque = deque(maxlen=30)   # 30 observations (~30 min)
_skew_ts_history: deque = deque(maxlen=30)


def push_skew(skew_value: float, timestamp: float):
    _skew_history.append(float(skew_value))
    _skew_ts_history.append(float(timestamp))


def compute_skew_dynamics(skew_25d: float, iv_25c: float, iv_25p: float,
                           timestamp: float) -> dict:
    """
    Computes skew velocity and detects direction-predicting skew moves.

    skew_25d = iv_25p − iv_25c (positive = put skew = downside fear)

    Returns:
        skew_velocity:    points/min change (positive = skew rising = bearish)
        skew_z_score:     z-score vs recent 30-observation history
        skew_alert:       human-readable alert or None
        skew_signal:      −1 to +1 (negative = upside lean, positive = downside lean)
        skew_state:       descriptive label
    """
    push_skew(skew_25d, timestamp)

    history = list(_skew_history)
    if len(history) < 5:
        return {
            'skew_velocity': 0.0, 'skew_z_score': 0.0,
            'skew_alert': None, 'skew_signal': 0.0,
            'skew_state': '⚪ Insufficient history',
            'skew_25d': round(skew_25d, 4),
            'iv_25c': round(iv_25c, 4), 'iv_25p': round(iv_25p, 4),
        }

    # Velocity: change over last 5 observations
    recent  = history[-5:]
    vel     = (recent[-1] - recent[0]) / max(len(recent) - 1, 1)

    # Z-score
    mu      = np.mean(history)
    sigma   = np.std(history) + 1e-6
    z       = (skew_25d - mu) / sigma

    # Normalised signal: negative z = skew falling = short covering = bullish
    signal  = float(np.clip(-z * 0.5, -1.0, 1.0))

    # Alert
    alert = None
    VEL_THRESH = 0.002   # ~0.2% IV per minute is significant
    if   vel > VEL_THRESH and z > 1.5:
        alert = f"🐻 SKEW RISING FAST (+{vel*1000:.1f}‰/min) — downside hedging detected"
    elif vel < -VEL_THRESH and z < -1.5:
        alert = f"🚀 SKEW COLLAPSING (−{abs(vel)*1000:.1f}‰/min) — short covering / upside likely"
    elif abs(z) > 2.5:
        alert = f"⚠️ EXTREME SKEW (z={z:.1f}) — {'downside' if z > 0 else 'upside'} positioning"

    # State label
    if   vel > VEL_THRESH and skew_25d > 0:   state = "📉 PUT SKEW RISING — downside hedging"
    elif vel < -VEL_THRESH and skew_25d > 0:  state = "📈 PUT SKEW COLLAPSING — covering"
    elif vel < -VEL_THRESH and skew_25d < 0:  state = "🔥 CALL SKEW RISING — upside demand"
    elif abs(vel) < VEL_THRESH * 0.5:         state = "⚖️ Skew stable"
    else:                                      state = f"〰️ Skew drifting ({vel*1000:+.1f}‰/min)"

    return {
        'skew_velocity':  round(vel, 6),
        'skew_z_score':   round(z, 2),
        'skew_alert':     alert,
        'skew_signal':    round(signal, 3),
        'skew_state':     state,
        'skew_25d':       round(skew_25d, 4),
        'iv_25c':         round(iv_25c, 4),
        'iv_25p':         round(iv_25p, 4),
    }
