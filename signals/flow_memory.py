"""
SIGNAL LAYER — flow_memory.py
Market State Memory — Flow Persistence Engine  (Final Structural Upgrade)

The gap this fills:
    The dashboard has always evaluated markets as independent snapshots.
    Every refresh: context → signals → probability → display.
    No memory of what happened 3 minutes ago.

    Real markets behave like pressure vessels.
    Large moves rarely happen because of one snapshot condition.
    They happen because pressure ACCUMULATES over time.

    Example (real market sequence before a hero-zero move):
        11:30 → dealers slightly short gamma           ← weak
        11:40 → delta flow turning positive            ← weak
        11:50 → small cascade starting                 ← weak
        12:00 → liquidity vacuum forming               ← weak
        12:10 → gamma wall approaching                 ← weak
        12:15 → BOOM                                   ← explosive

    Your system saw each frame as INDEPENDENT.
    A professional system sees:
        "Short gamma has been building for 45 minutes.
         Delta flow has been positive for 12 consecutive frames.
         Compression has lasted 18 minutes.
         Vacuum has persisted for 6 minutes.
         This is pre-explosion configuration."

Architecture:
    FlowMemory class stores bounded ring buffers for each tracked signal.
    Called once per refresh cycle, AFTER market_context, BEFORE signal_engine.

    Computes:
        1. Per-signal duration/streak metrics
        2. pressure_score:  0–100 (stored energy)
        3. persistence_multiplier: amplifier for hero_zero and move_probability
        4. buildup_phase:  'LOADING' | 'PRIMED' | 'EXPLOSIVE' | 'DORMANT'

    The persistence_multiplier is what feeds into hero_zero_score:
        compression_duration > 10 min → ×1.30
        compression_duration > 20 min → ×1.60
        compression_duration > 30 min → ×2.00

    This converts snapshot intelligence → state-aware intelligence.

Tracked signals:
    delta_flow_norm:    directional options demand (positive = call buying)
    gamma_regime:       net_gex < 0 = short gamma = amplifying
    vacuum:             liquidity gap active
    compression:        market compressed (ATR falling, OI building)
    cascade:            cascade FSM state (BUILDING/CONFIRMED/FIRING)
    dealer_pressure:    dealer delta magnitude

Design notes:
    - All buffers are STRICTLY BOUNDED (max 120 frames = ~4 minutes at 2s refresh)
    - Frame count stored as raw int, time in seconds derived from refresh_secs
    - No datetime calls inside — caller passes current_time for testability
    - Returns a single dict — no Streamlit dependency, fully testable
"""
import time
from collections import deque
from typing import Optional


# ── Buffer configuration ───────────────────────────────────────────────────────
MAX_FRAMES       = 1800   # hard cap on ring buffer size (~60 min at 2s refresh)
                          # Institutional flows build over 30-60 min — 120 was too short
REFRESH_SECS     = 2.0    # default refresh; caller can override


# ── Persistence thresholds ─────────────────────────────────────────────────────
# Delta flow: must exceed this norm value to count as "active"
DELTA_FLOW_THRESH   = 0.20   # 0.20 = meaningful institutional directional demand

# Gamma: net_gex < threshold = short gamma = amplifying regime
GAMMA_SHORT_THRESH  = -0.5   # GEX below -0.5M = in short gamma

# Compression: defined by caller (is_compressed bool)

# Cascade: any active FSM state above IDLE
CASCADE_ACTIVE_STATES = {'WARMING', 'BUILDING', 'CONFIRMED', 'FIRING'}

# Vacuum: in_vacuum bool from liquidity detector

# Dealer: |dealer_delta_M| > threshold = significant pressure
DEALER_PRESSURE_THRESH = 0.5   # 0.5M = meaningful dealer positioning


class _RingBuffer:
    """
    Strictly bounded ring buffer for signal history.
    Stores the raw signal value each frame.
    Bounded at MAX_FRAMES regardless of session length.
    """
    __slots__ = ('_buf', '_max')

    def __init__(self, max_size: int = MAX_FRAMES):
        self._buf = deque(maxlen=max_size)   # deque with maxlen is O(1) append + auto-truncate
        self._max = max_size

    def push(self, value) -> None:
        self._buf.append(value)

    def last(self, n: int = 1):
        """Return last n values as list (oldest first)."""
        data = list(self._buf)
        return data[-n:] if n <= len(data) else data

    def __len__(self) -> int:
        return len(self._buf)

    def count(self) -> int:
        return len(self._buf)


class FlowMemory:
    """
    Market State Memory — tracks the duration and buildup of structural signals.

    Designed to live in st.session_state as a single object.
    Safe to recreate on session reset (no external state).

    Usage:
        # In state.py init:
        if 'flow_memory' not in st.session_state:
            st.session_state.flow_memory = FlowMemory()

        # In market_context.py (after computing raw signals):
        fm = st.session_state.flow_memory
        persistence_r = fm.update(
            delta_flow_norm  = delta_flow_r['norm_signal'],
            net_gex          = gex_r['net_gex'],
            in_vacuum        = liq_vac_r['in_vacuum'],
            vacuum_severity  = liq_vac_r['vacuum_severity'],
            is_compressed    = comp_r['is_compressed'],
            cascade_fsm      = cascade_fsm,
            dealer_delta_M   = dealer_r['dealer_delta_M'],
            refresh_secs     = 2.0,
        )
    """

    def __init__(self, refresh_secs: float = REFRESH_SECS):
        self._refresh       = refresh_secs
        self._buf_delta     = _RingBuffer()
        self._buf_gamma     = _RingBuffer()
        self._buf_vacuum    = _RingBuffer()
        self._buf_compress  = _RingBuffer()
        self._buf_cascade   = _RingBuffer()
        self._buf_dealer    = _RingBuffer()

        # Streak counters (consecutive frames above threshold)
        self._streak_delta      = 0   # positive direction streak
        self._streak_delta_neg  = 0   # negative direction streak
        self._streak_gamma      = 0   # short gamma streak
        self._streak_vacuum     = 0
        self._streak_compress   = 0
        self._streak_cascade    = 0

        # Peak tracking (for trend-within-trend)
        self._peak_delta_norm   = 0.0
        self._peak_vacuum_sev   = 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def update(
        self,
        delta_flow_norm: float,
        net_gex:         float,
        in_vacuum:       bool,
        vacuum_severity: float,
        is_compressed:   bool,
        cascade_fsm:     str,
        dealer_delta_M:  float,
        refresh_secs:    float = REFRESH_SECS,
    ) -> dict:
        """
        Called once per dashboard refresh cycle.
        Pushes new values into all buffers and computes persistence metrics.

        Returns:
            persistence_r dict — all metrics, ready to pass into signal_engine.
        """
        self._refresh = refresh_secs

        # ── 1. Push to buffers ────────────────────────────────────────────
        self._buf_delta.push(delta_flow_norm)
        self._buf_gamma.push(net_gex)
        self._buf_vacuum.push(1.0 if in_vacuum else vacuum_severity)
        self._buf_compress.push(1 if is_compressed else 0)
        self._buf_cascade.push(1 if cascade_fsm in CASCADE_ACTIVE_STATES else 0)
        self._buf_dealer.push(abs(dealer_delta_M))

        # ── 2. Update streaks ────────────────────────────────────────────
        # Delta flow directional streak
        if delta_flow_norm > DELTA_FLOW_THRESH:
            self._streak_delta     += 1
            self._streak_delta_neg  = 0
        elif delta_flow_norm < -DELTA_FLOW_THRESH:
            self._streak_delta_neg += 1
            self._streak_delta      = 0
        else:
            self._streak_delta      = max(self._streak_delta - 1, 0)
            self._streak_delta_neg  = max(self._streak_delta_neg - 1, 0)

        # Gamma regime streak
        if net_gex < GAMMA_SHORT_THRESH:
            self._streak_gamma += 1
        else:
            self._streak_gamma  = max(self._streak_gamma - 2, 0)   # faster decay

        # Vacuum streak
        if in_vacuum:
            self._streak_vacuum += 1
        else:
            self._streak_vacuum = max(self._streak_vacuum - 1, 0)

        # Compression streak
        if is_compressed:
            self._streak_compress += 1
        else:
            self._streak_compress = max(self._streak_compress - 1, 0)

        # Cascade streak
        if cascade_fsm in CASCADE_ACTIVE_STATES:
            self._streak_cascade += 1
        else:
            self._streak_cascade = max(self._streak_cascade - 3, 0)  # fast decay after cascade ends

        # Peak tracking
        self._peak_delta_norm = max(self._peak_delta_norm * 0.99, abs(delta_flow_norm))
        self._peak_vacuum_sev = max(self._peak_vacuum_sev * 0.99, vacuum_severity)

        # ── 3. Convert streaks → durations (seconds and minutes) ──────────
        def _dur(streak: int) -> float:
            """frames → seconds"""
            return streak * self._refresh

        delta_dur_s     = _dur(max(self._streak_delta, self._streak_delta_neg))
        gamma_dur_s     = _dur(self._streak_gamma)
        vacuum_dur_s    = _dur(self._streak_vacuum)
        compress_dur_s  = _dur(self._streak_compress)
        cascade_dur_s   = _dur(self._streak_cascade)

        # ── 4. Persistence scores (0–1 per signal, saturates at target secs) ──
        # Score = min(duration / target_duration, 1.0)
        # Target durations are calibrated to when a signal becomes institutional:
        #   delta flow:    8 min = institutional demand, not noise
        #   gamma regime:  15 min = regime, not tick
        #   vacuum:        5 min = structural gap, not momentary
        #   compression:   12 min = coiling, not random contraction
        #   cascade:       4 min = building pressure

        def _pers(dur_s: float, target_s: float) -> float:
            return float(min(dur_s / max(target_s, 1.0), 1.0))

        ps_delta    = _pers(delta_dur_s,    480)   # 8 min
        ps_gamma    = _pers(gamma_dur_s,    900)   # 15 min
        ps_vacuum   = _pers(vacuum_dur_s,   300)   # 5 min
        ps_compress = _pers(compress_dur_s, 720)   # 12 min
        ps_cascade  = _pers(cascade_dur_s,  240)   # 4 min

        # ── 5. Pressure score (0–100, weighted sum of persistence) ────────
        # Weights reflect critique document's formula:
        #   delta_flow 30%, gamma 30%, vacuum 20%, compression 20%
        # Cascade is a bonus (not in base formula — it's already in hero_zero)
        pressure_score = (
            0.30 * ps_delta    * 100 +
            0.30 * ps_gamma    * 100 +
            0.20 * ps_vacuum   * 100 +
            0.20 * ps_compress * 100
        )
        pressure_score = float(min(pressure_score, 100.0))

        # ── 6. Delta flow acceleration (is pressure INCREASING?) ──────────
        # Compare last 5 frames vs previous 5 frames
        recent = self._buf_delta.last(5)
        prev5  = self._buf_delta.last(10)[:5]  # frames 5–10 ago
        if len(recent) >= 3 and len(prev5) >= 3:
            recent_mean = sum(abs(v) for v in recent) / len(recent)
            prev_mean   = sum(abs(v) for v in prev5)  / len(prev5)
            delta_accel = float(recent_mean - prev_mean)  # positive = accelerating
        else:
            delta_accel = 0.0

        # ── 7. Persistence multiplier for hero_zero and move_probability ──
        # Critique document formula:
        #   compression_duration > 10 min → ×1.30
        #   compression_duration > 20 min → ×1.60
        #   compression_duration > 30 min → ×2.00
        # Also applies for combined signals

        persist_mult = 1.0

        # Compression duration bonus
        compress_min = compress_dur_s / 60.0
        if   compress_min > 30: persist_mult *= 2.00
        elif compress_min > 20: persist_mult *= 1.60
        elif compress_min > 10: persist_mult *= 1.30
        elif compress_min > 5:  persist_mult *= 1.15

        # Delta flow streak bonus (institutional demand confirmed)
        delta_min = delta_dur_s / 60.0
        if   delta_min > 15: persist_mult *= 1.40
        elif delta_min > 8:  persist_mult *= 1.20
        elif delta_min > 4:  persist_mult *= 1.10

        # Vacuum duration bonus
        vac_min = vacuum_dur_s / 60.0
        if   vac_min > 8:  persist_mult *= 1.30
        elif vac_min > 4:  persist_mult *= 1.15
        elif vac_min > 2:  persist_mult *= 1.08

        # Short gamma persistence bonus
        gamma_min = gamma_dur_s / 60.0
        if   gamma_min > 30: persist_mult *= 1.25
        elif gamma_min > 15: persist_mult *= 1.15
        elif gamma_min > 8:  persist_mult *= 1.08

        # Interaction bonus: compression + delta_flow both persistent
        if ps_compress > 0.4 and ps_delta > 0.4:
            persist_mult *= 1.20   # "coiling + institutional demand" = pre-explosion

        # Cascade active during persistent vacuum = very high conviction
        if self._streak_cascade >= 2 and ps_vacuum > 0.3:
            persist_mult *= 1.20

        # Hard cap — even in extreme conditions keep it sane
        persist_mult = float(min(persist_mult, 4.0))

        # ── 8. Buildup phase (qualitative state) ──────────────────────────
        if pressure_score >= 70:
            buildup_phase = 'EXPLOSIVE'
        elif pressure_score >= 45:
            buildup_phase = 'PRIMED'
        elif pressure_score >= 20:
            buildup_phase = 'LOADING'
        else:
            buildup_phase = 'DORMANT'

        # ── 9. Delta flow direction for narrative ─────────────────────────
        if self._streak_delta > self._streak_delta_neg:
            delta_direction = 'BULLISH'
        elif self._streak_delta_neg > self._streak_delta:
            delta_direction = 'BEARISH'
        else:
            delta_direction = 'NEUTRAL'

        # ── 10. Dominant signal for display ──────────────────────────────
        signal_scores = {
            'delta_flow': ps_delta,
            'gamma':      ps_gamma,
            'vacuum':     ps_vacuum,
            'compression':ps_compress,
            'cascade':    ps_cascade,
        }
        dominant = max(signal_scores, key=signal_scores.get)

        return {
            # Raw durations
            'delta_dur_s':      round(delta_dur_s, 1),
            'gamma_dur_s':      round(gamma_dur_s, 1),
            'vacuum_dur_s':     round(vacuum_dur_s, 1),
            'compress_dur_s':   round(compress_dur_s, 1),
            'cascade_dur_s':    round(cascade_dur_s, 1),

            # Duration in minutes (for display)
            'delta_dur_min':    round(delta_dur_s / 60, 1),
            'gamma_dur_min':    round(gamma_dur_s / 60, 1),
            'vacuum_dur_min':   round(vacuum_dur_s / 60, 1),
            'compress_dur_min': round(compress_dur_s / 60, 1),
            'cascade_dur_min':  round(cascade_dur_s / 60, 1),

            # Persistence scores per signal (0–1)
            'ps_delta':     round(ps_delta, 3),
            'ps_gamma':     round(ps_gamma, 3),
            'ps_vacuum':    round(ps_vacuum, 3),
            'ps_compress':  round(ps_compress, 3),
            'ps_cascade':   round(ps_cascade, 3),

            # Delta flow
            'delta_direction': delta_direction,
            'delta_streak':    self._streak_delta or self._streak_delta_neg,
            'delta_accel':     round(delta_accel, 3),

            # Composite scores
            'pressure_score':    round(pressure_score, 1),
            'persist_mult':      round(persist_mult, 3),
            'buildup_phase':     buildup_phase,
            'dominant_signal':   dominant,
            'signal_scores':     {k: round(v, 3) for k, v in signal_scores.items()},

            # Peak tracking
            'peak_delta_norm':   round(self._peak_delta_norm, 3),
            'peak_vacuum_sev':   round(self._peak_vacuum_sev, 3),

            # Buffer counts (for debug/audit)
            '_buf_len': len(self._buf_delta),
        }

    def reset(self) -> None:
        """Clean reset — call when lookback changes or session needs fresh start."""
        self.__init__(self._refresh)


# ── Module-level helper for session state management ─────────────────────────

def get_or_create_flow_memory(session_state, refresh_secs: float = REFRESH_SECS) -> 'FlowMemory':
    """
    Gets or creates FlowMemory from session state.
    Validates that existing object is correct type (guards against reload corruption).
    """
    existing = session_state.get('flow_memory')
    if not isinstance(existing, FlowMemory):
        session_state['flow_memory'] = FlowMemory(refresh_secs)
    return session_state['flow_memory']


def pressure_to_label(pressure_score: float) -> str:
    """Human-readable label for pressure_score."""
    if   pressure_score >= 75: return "🔴 EXTREME PRESSURE"
    elif pressure_score >= 50: return "🟠 HIGH PRESSURE"
    elif pressure_score >= 30: return "🟡 BUILDING"
    elif pressure_score >= 10: return "🔵 LOADING"
    else:                       return "⚫ DORMANT"


def persist_mult_to_label(mult: float) -> str:
    """Describes what the multiplier means for position sizing."""
    if   mult >= 3.0: return "×{:.2f} MAJOR — extreme persistence".format(mult)
    elif mult >= 2.0: return "×{:.2f} STRONG — multi-signal buildup".format(mult)
    elif mult >= 1.5: return "×{:.2f} MODERATE — sustained pressure".format(mult)
    elif mult >= 1.2: return "×{:.2f} MILD — short accumulation".format(mult)
    else:             return "×{:.2f} LOW — snapshot only".format(mult)
