"""
DASHBOARD LAYER — cache.py
Responsibility: TTL-based computation cache for heavy operations.

Performance Fix: GEX, dealer delta, and migration are expensive to
recompute every 2 seconds.  Cache them with per-metric TTLs.

Design:
    - _CacheEntry holds (result, timestamp)
    - get_or_compute(key, fn, ttl) returns cached result if fresh, else calls fn
    - Thread-safe via simple dict (single-threaded Streamlit is fine)
    - Cache is module-level (persists across fragment re-runs)
    - Spot change > threshold forces cache invalidation (prevents stale walls)
"""
import time
from typing import Any, Callable, Dict, Optional

_CACHE: Dict[str, tuple] = {}   # key → (value, ts, spot_at_write)

# TTL seconds per operation type
TTL = {
    'gex':            10,   # GEX: expensive BS gamma loops, 10s stale is fine
    'dealer':         10,   # Dealer delta: similar cost
    'migration':      30,   # Migration: 30s — changes slowly
    'magnet':         15,   # Strike magnetism
    'max_pain':       30,   # Max pain: very stable intraday
    'gamma_conv':     8,    # P5: dGamma/dSpot — limit radius+400 AND cache 8s
    'liquidity_map':  15,   # Liquidity map: OI walls stable over 15s
    'sensitivity':    20,   # Explosion zones: slow-changing
}
# Spot move that forces immediate refresh (catching breakouts)
SPOT_INVALIDATION_THRESHOLD = 25.0   # fallback — overridden by velocity below

def _dynamic_threshold(current_spot: float, velocity: float = 0.0) -> float:
    """
    Doc 9 fix: threshold = max(15, velocity × 2)
    Nifty can move 20–30pts in seconds during news events.
    Static 25pt threshold causes stale cache during fast moves.
    velocity = recent spot std (pts/min from market_context).
    """
    return max(15.0, velocity * 2.0)


def get_or_compute(key: str, fn, ttl: int,
                   current_spot: float = 0.0,
                   velocity: float = 0.0):
    """
    Return cached value if (a) age < ttl AND (b) spot hasn't moved > dynamic threshold.
    """
    now = time.time()
    thr = _dynamic_threshold(current_spot, velocity)
    if key in _CACHE:
        value, ts, cached_spot = _CACHE[key]
        age        = now - ts
        spot_moved = abs(current_spot - cached_spot) if current_spot and cached_spot else 0.0
        if age < ttl and spot_moved < thr:
            return value
    value = fn()
    _CACHE[key] = (value, now, current_spot)
    return value


def invalidate(key: str):
    _CACHE.pop(key, None)


def invalidate_all():
    _CACHE.clear()


def cache_age(key: str) -> float:
    """Seconds since last cache write.  Returns inf if not cached."""
    if key not in _CACHE:
        return float('inf')
    return time.time() - _CACHE[key][1]
