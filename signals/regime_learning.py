"""
SIGNAL LAYER — regime_learning.py
Responsibility: Rolling percentile thresholds — replace static magic numbers.

Problem 7 Fix — Regime Learning:
    Static thresholds (flow_ratio > 5, STRONG_OI = 500k) are arbitrary.
    Historical distribution gives proper context.
    top-10% flow ratio is more meaningful than a fixed number.

Design:
    RollingDistribution stores a fixed-length window of observations.
    percentile_rank(value) returns 0–100 indicating how extreme this value is.
    threshold_at(pct) returns the value at the given historical percentile.
"""
import numpy as np
from collections import deque
from typing import Optional


class RollingDistribution:
    """Thread-safe rolling percentile calculator."""
    __slots__ = ('_buf', '_maxlen')

    def __init__(self, maxlen: int = 1000):
        self._buf    = deque(maxlen=maxlen)
        self._maxlen = maxlen

    def push(self, value: float):
        self._buf.append(float(value))

    def percentile_rank(self, value: float) -> float:
        """What percentile is this value in the stored distribution? 0–100."""
        if len(self._buf) < 10:
            return 50.0
        arr = np.array(self._buf)
        return float((arr < value).mean() * 100.0)

    def threshold_at(self, pct: float, fallback: float = 0.0) -> float:
        """Value at pct-th percentile. Returns fallback if insufficient data."""
        if len(self._buf) < 10:
            return fallback
        return float(np.percentile(self._buf, pct))

    def __len__(self):
        return len(self._buf)


# ── Module-level distributions (one per session) ─────────────────────────────

_dists: dict = {}

def _get(name: str, maxlen: int = 1000) -> RollingDistribution:
    if name not in _dists:
        _dists[name] = RollingDistribution(maxlen=maxlen)
    return _dists[name]


def push_flow_ratio(value: float):
    _get('flow_ratio').push(abs(value))

def push_oi_flow(value: float):
    _get('oi_flow').push(abs(value))

def push_velocity(value: float):
    _get('velocity').push(abs(value))

def push_net_gex(value: float):
    _get('net_gex').push(abs(value))


def flow_ratio_is_aggressive(ratio: float, fallback_threshold: float = 5.0) -> bool:
    """
    True if ratio is in the top 10% of historical flow ratios.
    Falls back to static threshold until enough history (≥50 observations).
    """
    d = _get('flow_ratio')
    if len(d) >= 50:
        return ratio >= d.threshold_at(90, fallback=fallback_threshold)
    return ratio >= fallback_threshold


def oi_flow_is_strong(oi_val: float, fallback: float = 500_000) -> bool:
    """True if OI value is in the top 20% of observed OI flows."""
    d = _get('oi_flow')
    if len(d) >= 50:
        return oi_val >= d.threshold_at(80, fallback=fallback)
    return oi_val >= fallback


def flow_ratio_percentile(ratio: float) -> float:
    """0–100 rank of this flow ratio in historical distribution."""
    return _get('flow_ratio').percentile_rank(ratio)


def oi_percentile(oi_val: float) -> float:
    return _get('oi_flow').percentile_rank(oi_val)


def velocity_percentile(vel: float) -> float:
    return _get('velocity').percentile_rank(vel)


def get_dynamic_flow_threshold(fallback: float = 5.0) -> float:
    """Top-10% flow ratio threshold from history."""
    return _get('flow_ratio').threshold_at(90, fallback=fallback)


def get_dynamic_oi_threshold(fallback: float = 500_000) -> float:
    """Top-20% OI flow threshold from history."""
    return _get('oi_flow').threshold_at(80, fallback=fallback)
