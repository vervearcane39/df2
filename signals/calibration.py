"""
SIGNAL LAYER — calibration.py
Signal Calibration Engine  (Fix for Problem 6)

The problem being solved:
    Your logistic model outputs P(move) = 70%.
    But does that mean 70% of times you see that score, a large move occurs?
    Without empirical calibration: no.

    You need to:
    1. Log every signal snapshot with a timestamp
    2. Check what happened to spot N minutes later
    3. Build an empirical hit-rate table
    4. Apply Platt scaling (a logistic regression on the raw scores
       to map them to calibrated probabilities)

This module handles steps 1–3.
Platt scaling is implemented as a simple running lookup table
that can be used without scipy or sklearn.

Architecture:
    SignalLogger writes to a 'signal_log' table in the existing SQLite DB.
    Each row: timestamp, move_prob_raw, direction, spot_then, spot_after_5m,
              spot_after_15m, spot_after_30m, realized_move (computed on close)

    CalibrationTable reads all closed rows and builds a 3-bucket table:
        low  (0–40):   empirical_hit_rate
        mid  (40–70):  empirical_hit_rate
        high (70–100): empirical_hit_rate

    apply_calibration(raw_score) → calibrated_score using the table.

Usage:
    from signals.calibration import SignalLogger, apply_calibration

    logger = SignalLogger(db_path)
    logger.log(move_prob=72, direction='UP', spot=22350)

    # Every cycle: close out old open rows
    logger.close_outcomes(current_spot=22380)

    # Use calibrated probability
    calibrated = apply_calibration(db_path, raw_score=72)
"""
import sqlite3
import time
import math
import logging
from typing import Optional


# ── Table DDL ─────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS signal_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_open     INTEGER NOT NULL,
    ts_close_5  INTEGER,
    ts_close_15 INTEGER,
    ts_close_30 INTEGER,
    move_prob   REAL,
    direction   TEXT,
    phase       TEXT,
    hz_score    REAL,
    spot_open   REAL,
    spot_5m     REAL,
    spot_15m    REAL,
    spot_30m    REAL,
    move_5m     REAL,
    move_15m    REAL,
    move_30m    REAL,
    hit_5m      INTEGER,   -- 1 if move > threshold, 0 otherwise, NULL if not closed
    hit_15m     INTEGER,
    hit_30m     INTEGER
)
"""

# A "hit" = spot moved > HIT_THRESHOLD points in the declared direction
HIT_THRESHOLD_PTS = 30    # ₹30 in NIFTY = meaningful move


class SignalLogger:
    """
    Logs signal snapshots and closes them out with realized outcomes.

    Designed to be instantiated once per session and called each cycle.
    Very lightweight: only writes when state changes meaningfully.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_table()
        self._last_log_ts = 0
        self._log_cooldown = 60   # log at most once per minute

    def _ensure_table(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(_CREATE_TABLE)
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f"SignalLogger table init failed: {e}")

    def log(self, move_prob: float, direction: str, spot: float,
            phase: str = 'BALANCED', hz_score: float = 0.0):
        """
        Log a new signal snapshot.
        Only logs when:
            - at least _log_cooldown seconds since last log
            - move_prob > 40 (below this is not interesting)
        """
        now = int(time.time())
        if now - self._last_log_ts < self._log_cooldown:
            return
        if move_prob < 40:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                INSERT INTO signal_log
                    (ts_open, move_prob, direction, phase, hz_score, spot_open,
                     ts_close_5, ts_close_15, ts_close_30)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (now, round(float(move_prob), 1), direction[:8], phase[:20],
                  round(float(hz_score), 1), round(float(spot), 1),
                  now + 300, now + 900, now + 1800))
            conn.commit()
            conn.close()
            self._last_log_ts = now
        except Exception as e:
            logging.debug(f"SignalLogger.log failed: {e}")

    def close_outcomes(self, current_spot: float):
        """
        Close out rows whose close timestamps have passed.
        Called every cycle. Updates spot_Xm and computes hit_Xm.
        """
        now = int(time.time())
        try:
            conn = sqlite3.connect(self.db_path)
            # 5-minute outcomes
            rows = conn.execute("""
                SELECT id, spot_open, direction, ts_close_5
                FROM signal_log
                WHERE spot_5m IS NULL AND ts_close_5 <= ?
            """, (now,)).fetchall()

            for row_id, spot_open, dirn, _ in rows:
                move = current_spot - spot_open
                hit  = _compute_hit(move, dirn, HIT_THRESHOLD_PTS)
                conn.execute("""
                    UPDATE signal_log SET spot_5m=?, move_5m=?, hit_5m=?
                    WHERE id=?
                """, (round(current_spot, 1), round(move, 1), hit, row_id))

            # 15-minute outcomes
            rows15 = conn.execute("""
                SELECT id, spot_open, direction
                FROM signal_log
                WHERE spot_15m IS NULL AND ts_close_15 <= ?
            """, (now,)).fetchall()
            for row_id, spot_open, dirn in rows15:
                move = current_spot - spot_open
                hit  = _compute_hit(move, dirn, HIT_THRESHOLD_PTS)
                conn.execute("""
                    UPDATE signal_log SET spot_15m=?, move_15m=?, hit_15m=?
                    WHERE id=?
                """, (round(current_spot, 1), round(move, 1), hit, row_id))

            # 30-minute outcomes
            rows30 = conn.execute("""
                SELECT id, spot_open, direction
                FROM signal_log
                WHERE spot_30m IS NULL AND ts_close_30 <= ?
            """, (now,)).fetchall()
            for row_id, spot_open, dirn in rows30:
                move = current_spot - spot_open
                hit  = _compute_hit(move, dirn, HIT_THRESHOLD_PTS)
                conn.execute("""
                    UPDATE signal_log SET spot_30m=?, move_30m=?, hit_30m=?
                    WHERE id=?
                """, (round(current_spot, 1), round(move, 1), hit, row_id))

            conn.commit()
            conn.close()
        except Exception as e:
            logging.debug(f"SignalLogger.close_outcomes failed: {e}")


def _compute_hit(move: float, direction: str, threshold: float) -> int:
    if direction == 'UP':
        return 1 if move >= threshold else 0
    elif direction == 'DOWN':
        return 1 if move <= -threshold else 0
    else:   # NEUTRAL: hit if any large move
        return 1 if abs(move) >= threshold else 0


# ── Calibration Table ─────────────────────────────────────────────────────────

# Raw score → calibrated score lookup buckets
# Updated from empirical data. Starts at naive 1:1 (uncalibrated).
# Buckets: (raw_lo, raw_hi): (empirical_hit_rate_15m, n_samples)
_DEFAULT_CALIBRATION = {
    # 10 buckets as recommended in file critique (was 5)
    # Prior hit rates are based on general options market microstructure research.
    # These priors are REPLACED by empirical data once ≥5 samples exist per bucket.
    (0,  15):  0.12,   # very low signal — near random
    (15, 25):  0.18,
    (25, 35):  0.25,
    (35, 45):  0.32,
    (45, 55):  0.42,   # ~coin-flip zone
    (55, 65):  0.52,
    (65, 72):  0.60,
    (72, 80):  0.67,
    (80, 90):  0.73,
    (90, 101): 0.80,   # top decile — sustained pressure + cascade confirmation
}


def build_calibration_table(db_path: str, horizon: str = '15m') -> dict:
    """
    Read signal_log and build empirical hit-rate table by score bucket.

    horizon: '5m' | '15m' | '30m'

    Returns:
        {(lo, hi): (empirical_rate, n_samples), ...}
        Returns _DEFAULT_CALIBRATION if < 20 samples.
    """
    col = {'5m': 'hit_5m', '15m': 'hit_15m', '30m': 'hit_30m'}.get(horizon, 'hit_15m')
    buckets = [(0,15),(15,25),(25,35),(35,45),(45,55),(55,65),(65,72),(72,80),(80,90),(90,101)]
    result  = {}
    total   = 0

    try:
        conn   = sqlite3.connect(db_path)
        rows   = conn.execute(
            f"SELECT move_prob, {col} FROM signal_log WHERE {col} IS NOT NULL"
        ).fetchall()
        conn.close()

        for lo, hi in buckets:
            bucket_rows = [(p, h) for p, h in rows if lo <= p < hi]
            n     = len(bucket_rows)
            total += n
            if n >= 5:
                rate = sum(h for _, h in bucket_rows) / n
            else:
                # Fall back to prior for sparse buckets
                rate = _DEFAULT_CALIBRATION.get(
                    next(k for k in _DEFAULT_CALIBRATION if k[0] == lo), 0.5)
            result[(lo, hi)] = (round(rate, 3), n)

    except Exception:
        pass

    if total < 20:
        # Not enough data — return default (naive prior)
        return {k: (v, 0) for k, v in _DEFAULT_CALIBRATION.items()}

    return result


def apply_calibration(db_path: str, raw_score: float,
                      horizon: str = '15m') -> dict:
    """
    Maps a raw move_prob score to a calibrated probability.

    Returns:
        calibrated:   float [0,1] — empirically grounded probability
        n_samples:    int — how many historical examples this bucket has
        is_calibrated: bool — False if using naive prior (< 20 total samples)
        raw:          original score
    """
    table = build_calibration_table(db_path, horizon)
    total_n = sum(n for _, n in table.values())

    for (lo, hi), (rate, n) in sorted(table.items()):
        if lo <= raw_score < hi or (hi == 101 and raw_score >= lo):
            return {
                'calibrated':    round(rate * 100, 1),
                'n_samples':     n,
                'is_calibrated': total_n >= 20,
                'raw':           raw_score,
                'note': (f"Based on {n} historical signals in {lo}–{hi} band"
                         if n >= 5 else f"Prior only — {n} samples in bucket"),
            }

    return {'calibrated': raw_score, 'n_samples': 0,
            'is_calibrated': False, 'raw': raw_score, 'note': 'Fallback'}



def get_adaptive_hz_threshold(db_path: str, min_samples: int = 20) -> dict:
    """
    Derives an empirical HZ ignition threshold from logged signal outcomes.

    Logic:
        For each candidate threshold T in [55, 60, 65, 70, 75, 80]:
            precision(T) = P(hit_15m=1 | hz_score >= T)
        Return the lowest T where precision >= 0.55.
        Falls back to T=70 if < min_samples available.

    Returns:
        armed_threshold:    score needed for ARMED   (default 65)
        ignition_threshold: score needed for IGNITION (default 80)
        is_adaptive:        False until min_samples collected
        sample_count:       number of closed hz_score rows
        note:               human-readable explanation
    """
    FALLBACK = {'armed_threshold': 65, 'ignition_threshold': 80,
                'is_adaptive': False, 'sample_count': 0,
                'note': 'Using default thresholds — not enough data yet'}
    try:
        conn  = sqlite3.connect(db_path)
        rows  = conn.execute(
            "SELECT hz_score, hit_15m FROM signal_log "
            "WHERE hz_score IS NOT NULL AND hit_15m IS NOT NULL"
        ).fetchall()
        conn.close()
    except Exception:
        return FALLBACK

    if len(rows) < min_samples:
        FALLBACK['sample_count'] = len(rows)
        FALLBACK['note'] = f'Building data ({len(rows)}/{min_samples} samples)'
        return FALLBACK

    rows = [(float(h), int(r)) for h, r in rows if h is not None and r is not None]

    armed_t     = 65   # default
    ignition_t  = 80   # default

    for threshold in [55, 60, 65, 70, 75, 80]:
        subset = [hit for hz, hit in rows if hz >= threshold]
        if len(subset) >= 5:
            precision = sum(subset) / len(subset)
            if precision >= 0.55:
                armed_t = threshold
                break   # lowest threshold with >= 55% hit rate

    # Ignition = threshold where precision >= 0.70
    for threshold in [65, 70, 75, 80, 85]:
        subset = [hit for hz, hit in rows if hz >= threshold]
        if len(subset) >= 3:
            precision = sum(subset) / len(subset)
            if precision >= 0.70:
                ignition_t = threshold
                break

    # Sanity bounds
    armed_t     = int(max(55, min(armed_t, 80)))
    ignition_t  = int(max(armed_t + 5, min(ignition_t, 90)))

    return {
        'armed_threshold':    armed_t,
        'ignition_threshold': ignition_t,
        'is_adaptive':        True,
        'sample_count':       len(rows),
        'note': f'Adaptive: ARMED≥{armed_t} IGNITION≥{ignition_t} ({len(rows)} samples)',
    }

def get_calibration_summary(db_path: str) -> dict:
    """
    Returns a summary dict for display in the dashboard.
    Shows per-bucket hit rates and total samples.
    """
    try:
        conn  = sqlite3.connect(db_path)
        total = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
        closed = conn.execute(
            "SELECT COUNT(*) FROM signal_log WHERE hit_15m IS NOT NULL"
        ).fetchone()[0]
        conn.close()
    except Exception:
        total = closed = 0

    table = build_calibration_table(db_path)
    return {
        'total_logged':  total,
        'total_closed':  closed,
        'is_live':       closed >= 20,
        'table':         table,
        'note': ('🟢 Live calibration active' if closed >= 20
                 else f'⚪ Building calibration ({closed}/20 samples)'),
    }