"""
SIGNAL LAYER — stability_filter.py
Temporal Signal Confirmation  (Upgrade 5)

The core problem:
    Many indicators flip too fast.
    skew_velocity, flow_ratio, velocity_acceleration can all react to
    single-tick noise and flip the signal.

    Without stability filtering this creates dashboard chatter:
    BULLISH → BEARISH → BULLISH in 3 consecutive 2-second refreshes.
    That is useless and dangerous.

Solution — Temporal Confirmation Buffer:
    A signal is only considered CONFIRMED when it has persisted for
    N consecutive observations.

    If it drops below threshold before N frames: reset to PENDING.
    Only CONFIRMED signals contribute to scores.

Problem 5 fixes:
    - MAX_HISTORY is now strictly enforced via list slicing (not conditional)
    - min_frames is dynamic based on refresh frequency
    - StabilityBuffer is a plain class (not dataclass) to avoid Streamlit
      session state serialisation issues that caused state corruption
    - Added reset_all() helper to cleanly clear all buffers on page reload
    - Added bounded_size property for memory audit
"""
import numpy as np
from typing import Any, List, Optional


# ── Default confirmation requirements per signal type ─────────────────────────
DEFAULT_MIN_FRAMES = {
    'cascade':          3,
    'vacuum':           2,
    'compression':      3,
    'trap':             2,
    'hero_zero_armed':  2,
    'hero_zero_ignition': 2,
    'skew_velocity':    3,
    'flow_ratio':       2,
    'acceleration':     2,
    'move_prob':        2,
    'phase':            3,
    'direction_up':     3,
    'direction_dn':     3,
}

MAX_HISTORY = 8    # strictly capped — was 10 but smaller = less memory


def min_frames_for_refresh(refresh_secs: float, base_frames: int = 3) -> int:
    """
    Dynamic min_frames based on dashboard refresh rate.

    Critique: "min_frames should vary with refresh frequency."
    If refresh = 2s, 3 frames = 6 seconds confirmation.
    If refresh = 10s, 3 frames = 30 seconds confirmation.

    We want confirmation to represent ~10-15 seconds of real time.
    Target: 12 seconds of real-world confirmation.

    Example:
        refresh=2s  → min_frames=6   (12s)
        refresh=5s  → min_frames=3   (15s)
        refresh=10s → min_frames=2   (20s, slightly loose but ok)
    """
    target_secs = 12.0
    frames = max(int(round(target_secs / max(refresh_secs, 1.0))), 2)
    return min(frames, 8)   # hard cap at 8 regardless


class StabilityBuffer:
    """
    Single-signal temporal confirmation buffer.

    Plain class (not dataclass) to avoid Streamlit session_state
    serialisation edge cases that can corrupt state on hot-reload.

    States:
        INACTIVE:  not enough history yet
        PENDING:   signal currently active but not confirmed
        CONFIRMED: signal has persisted for min_frames
        COOLING:   signal was confirmed, now dropped — cooldown period

    Memory: fixed at MAX_HISTORY entries. Strictly enforced.
    """

    def __init__(self, name: str, min_frames: int = 2, cool_frames: int = 2):
        self.name               = name
        self.min_frames         = int(min_frames)
        self.cool_frames        = int(cool_frames)
        # Runtime state — plain list, no default_factory issues
        self.history:           List[Any] = []
        self.state:             str = 'INACTIVE'
        self.consecutive_active:    int = 0
        self.consecutive_inactive:  int = 0
        self.last_confirmed_value:  Any = None

    @property
    def bounded_size(self) -> int:
        """Returns current history length. Should always be ≤ MAX_HISTORY."""
        return len(self.history)

    def update(self, value: Any, threshold: Any = True) -> bool:
        """
        Update buffer with new observation. Returns True if CONFIRMED.
        """
        # Strictly bounded append (P5 fix: use slice not conditional)
        self.history.append(value)
        self.history = self.history[-MAX_HISTORY:]   # always bounded

        # Activation check
        if isinstance(value, bool):
            active = value
        elif isinstance(value, str):
            active = (value == threshold)
        elif isinstance(value, (int, float)):
            active = float(value) >= float(threshold) if isinstance(threshold, (int, float)) else bool(value)
        else:
            active = bool(value)

        # Counter update
        if active:
            self.consecutive_active   += 1
            self.consecutive_inactive  = 0
        else:
            self.consecutive_inactive += 1
            self.consecutive_active    = 0

        # State machine
        if self.state == 'INACTIVE':
            if active:
                self.state = 'PENDING'
                self.consecutive_active = 1

        elif self.state == 'PENDING':
            if self.consecutive_active >= self.min_frames:
                self.state = 'CONFIRMED'
                self.last_confirmed_value = value
            elif not active:
                self.state = 'INACTIVE'
                self.consecutive_active = 0

        elif self.state == 'CONFIRMED':
            if not active:
                self.state = 'COOLING'
                self.consecutive_inactive = 1
            else:
                self.last_confirmed_value = value

        elif self.state == 'COOLING':
            if active:
                self.state = 'CONFIRMED'
                self.last_confirmed_value = value
            elif self.consecutive_inactive >= self.cool_frames:
                self.state = 'INACTIVE'
                self.consecutive_active = 0

        return self.state == 'CONFIRMED'

    def is_confirmed(self) -> bool:
        return self.state == 'CONFIRMED'

    def smoothed_value(self) -> float:
        """Rolling mean of last min_frames observations."""
        if not self.history:
            return 0.0
        recent = self.history[-self.min_frames:]
        try:
            return float(np.mean([float(v) for v in recent]))
        except (TypeError, ValueError):
            return 0.0

    def reset(self):
        self.history = []
        self.state = 'INACTIVE'
        self.consecutive_active = 0
        self.consecutive_inactive = 0
        self.last_confirmed_value = None


# ── Session state helpers ─────────────────────────────────────────────────────

def get_or_create_buffer(session_state, key: str,
                         signal_type: str = None,
                         refresh_secs: float = 2.0) -> StabilityBuffer:
    """
    Gets or creates a StabilityBuffer in Streamlit session state.

    P5 fix: checks that existing buffer is a StabilityBuffer instance.
    If session state was corrupted/reloaded, recreates it cleanly.
    """
    existing = session_state.get(key)
    if not isinstance(existing, StabilityBuffer):
        base_frames = DEFAULT_MIN_FRAMES.get(signal_type or key, 2)
        min_f = min_frames_for_refresh(refresh_secs, base_frames)
        session_state[key] = StabilityBuffer(name=key, min_frames=min_f)
    return session_state[key]


def confirmed(session_state, key: str, value: Any,
              threshold: Any = True, signal_type: str = None,
              refresh_secs: float = 2.0) -> bool:
    """One-liner: update buffer and return confirmed state."""
    buf = get_or_create_buffer(session_state, key, signal_type, refresh_secs)
    return buf.update(value, threshold)


def confirmed_value(session_state, key: str, raw_value: float,
                    threshold: float, signal_type: str = None,
                    refresh_secs: float = 2.0) -> float:
    """Returns smoothed value if confirmed, else 0.0."""
    buf = get_or_create_buffer(session_state, key, signal_type, refresh_secs)
    is_conf = buf.update(raw_value, threshold)
    return buf.smoothed_value() if is_conf else 0.0


def reset_all_buffers(session_state):
    """
    P5 fix: cleanly reset all stability buffers.
    Call this on page reload or when lookback changes significantly.
    """
    keys_to_reset = [k for k in session_state if k.startswith('sb_')]
    for k in keys_to_reset:
        if isinstance(session_state[k], StabilityBuffer):
            session_state[k].reset()


def get_buffer_audit(session_state) -> dict:
    """
    Returns memory audit of all stability buffers.
    Used to detect runaway memory growth (P5 fix).
    """
    result = {}
    for k, v in session_state.items():
        if isinstance(v, StabilityBuffer):
            result[k] = {
                'state': v.state,
                'size':  v.bounded_size,
                'min_frames': v.min_frames,
            }
    return result


def apply_stability_filter(session_state, signals: dict,
                           refresh_secs: float = 2.0) -> dict:
    """
    Applies stability filtering to a batch of signals.

    Input dict format:
        {
            'cascade':       (bool_value, 'cascade'),
            'vacuum':        (bool_value, 'vacuum'),
            'skew_velocity': (float_value, 'skew_velocity', threshold),
        }
    """
    result = {}
    for name, args in signals.items():
        key = f'sb_{name}'
        if len(args) == 2:
            value, stype = args
            result[name] = confirmed(session_state, key, value,
                                     threshold=True, signal_type=stype,
                                     refresh_secs=refresh_secs)
        elif len(args) == 3:
            value, stype, threshold = args
            result[name] = confirmed(session_state, key, value,
                                     threshold=threshold, signal_type=stype,
                                     refresh_secs=refresh_secs)
        else:
            result[name] = bool(args[0])
    return result


