"""
MARKET STATE LAYER — market_phase.py
Market Phase Engine  (Upgrade 1 — Critical)

The single most important architectural improvement in the critique.

The core problem it solves:
    Your signals run all the time, but they mean different things in
    different market phases.

    compression at 10:30 → often fake, pre-trend noise
    compression at 14:45 → pre-explosion, highest edge setup

    Without phase awareness, the system generates equal-confidence signals
    at 10:00 and 14:50. That is wrong.

Phase Definitions:
═══════════════════════════════════════════════════════════════════

OPEN_DISCOVERY   (9:15–9:45)
    Price discovery. High volatility. No signal reliability.
    Action: OBSERVE only. No entries.

BALANCED         (9:45–11:00, moderate velocity, no trend)
    Market is in equilibrium. Signals are mixed.
    Action: Wait for clear OI development.

TREND            (any time, high velocity, directional flow)
    Clear directional move underway.
    Action: Trend following valid. Momentum signals weighted higher.

COMPRESSION      (any time, low velocity, straddle falling, OI stable)
    Volatility compressing. Pre-move coil.
    Action: Watch for expansion trigger.

EXPIRY_PIN       (expiry day, spot near max pain, low velocity)
    Market makers pinning at max pain.
    Action: DO NOT fade pin. Sell volatility plays.

EXPIRY_BREAK     (expiry day, spot breaking away from max pain, velocity rising)
    The hero-zero window. Gamma feedback loop starting.
    Action: Highest edge window for buyers.

LIQUIDITY_VACUUM (any time, velocity high, thin OI ahead)
    Spot in a liquidity gap. Fast moves possible.
    Action: Momentum continuation expected.

MEAN_REVERSION   (any time, high acceptance_dist, velocity falling)
    Spot extended from equilibrium, momentum fading.
    Action: Counter-trend caution.

How phases change signal weights:
═══════════════════════════════════════════════════════════════════
Each phase returns a signal_weight_modifier dict.
Signal engine multiplies component weights by these modifiers before scoring.

Example:
    EXPIRY_BREAK → cascade_weight × 2.0, hero_zero_weight × 1.5
    OPEN_DISCOVERY → all weights × 0.3 (very low confidence)
    COMPRESSION → velocity_weight × 0.5, gamma_weight × 1.3
"""
import numpy as np
from datetime import datetime, time as dt_time
from typing import Optional


# ── Phase constants ───────────────────────────────────────────────────────────

PHASES = [
    'OPEN_DISCOVERY',
    'BALANCED',
    'TREND',
    'COMPRESSION',
    'EXPIRY_PIN',
    'EXPIRY_BREAK',
    'LIQUIDITY_VACUUM',
    'MEAN_REVERSION',
]

# Per-phase signal weight modifiers.
# Multiplied against base weights in signal_engine.
# Keys match move_probability driver names + hero_zero.
PHASE_MODIFIERS = {
    'OPEN_DISCOVERY': {
        'overall_confidence': 0.30,   # very low: discovery, not actionable
        'gamma_regime': 0.5,
        'cascade':      0.2,
        'delta_flow':   0.5,
        'vacuum':       0.8,
        'hero_zero':    0.2,
        'quality_cap':  45,           # max quality score during this phase
        'trade_allowed': False,
        'note': '🌅 Opening discovery — wait for market to establish',
    },
    'BALANCED': {
        'overall_confidence': 0.75,
        'gamma_regime': 1.0,
        'cascade':      0.8,
        'delta_flow':   1.0,
        'vacuum':       1.0,
        'hero_zero':    0.7,
        'quality_cap':  80,
        'trade_allowed': True,
        'note': '⚖️ Balanced — signals reliable, normal sizing',
    },
    'TREND': {
        'overall_confidence': 1.0,
        'gamma_regime': 0.8,
        'cascade':      1.2,
        'delta_flow':   1.3,          # trend = flow matters most
        'vacuum':       1.1,
        'hero_zero':    0.9,
        'quality_cap':  90,
        'trade_allowed': True,
        'note': '🚀 Trending — directional flow dominant',
    },
    'COMPRESSION': {
        'overall_confidence': 0.85,
        'gamma_regime': 1.3,          # gamma pinning is meaningful
        'cascade':      1.1,
        'delta_flow':   0.7,          # flow is quiet, reduce weight
        'vacuum':       1.2,
        'hero_zero':    1.2,          # compression before break = setup
        'quality_cap':  85,
        'trade_allowed': True,        # wait for break trigger
        'note': '🗜️ Compression — pre-move coil, watch for ignition',
    },
    'EXPIRY_PIN': {
        'overall_confidence': 0.6,
        'gamma_regime': 1.4,
        'cascade':      0.5,          # cascade unlikely while pinned
        'delta_flow':   0.8,
        'vacuum':       0.9,
        'hero_zero':    0.4,          # pin ≠ break
        'quality_cap':  60,
        'trade_allowed': False,       # unfavourable for buyers
        'note': '📌 Expiry pin — MM pinning, avoid buying volatility',
    },
    'EXPIRY_BREAK': {
        'overall_confidence': 1.0,
        'gamma_regime': 1.5,
        'cascade':      2.0,          # cascade is the mechanism of the break
        'delta_flow':   1.2,
        'vacuum':       1.4,
        'hero_zero':    1.5,          # maximum hero-zero weight
        'quality_cap':  100,
        'trade_allowed': True,
        'note': '💥 Expiry break — HIGHEST EDGE window for option buyers',
    },
    'LIQUIDITY_VACUUM': {
        'overall_confidence': 0.90,
        'gamma_regime': 1.1,
        'cascade':      1.3,
        'delta_flow':   1.0,
        'vacuum':       2.0,          # vacuum is the primary signal
        'hero_zero':    1.1,
        'quality_cap':  85,
        'trade_allowed': True,
        'note': '⚡ Liquidity vacuum — fast moves expected, momentum valid',
    },
    'MEAN_REVERSION': {
        'overall_confidence': 0.65,
        'gamma_regime': 1.0,
        'cascade':      0.6,
        'delta_flow':   0.7,
        'vacuum':       0.8,
        'hero_zero':    0.5,
        'quality_cap':  65,
        'trade_allowed': False,
        'note': '🔄 Mean reversion — spot extended, momentum fading',
    },
}


# ── Phase Classifier ──────────────────────────────────────────────────────────

def classify_market_phase(
    # Time
    is_expiry:       bool,
    # Spot mechanics
    velocity:        float,
    norm_slope:      float,
    acceptance_dist: float,
    dist_threshold:  float,
    # GEX / gamma
    net_gex:         float,
    gex_mode:        str,
    # OI
    net_w_oi:        float,
    STRONG_OI:       float,
    stagnation_counter: int,
    # Price structure
    max_pain:        int,
    spot:            float,
    # Straddle / IV
    straddle:        float,
    straddle_history: list,         # rolling list from session state
    # Compression flag
    is_compressed:   bool,
    # Liquidity
    in_vacuum:       bool,
    vacuum_severity: float,
    # Migration / OI flow
    migration_r:     dict,
    # Cascade
    cascade_fsm:     str,           # 'IDLE'|'WARMING'|'BUILDING'|'CONFIRMED'|'FIRING'
    # Override from previous phase (for hysteresis)
    prev_phase:      Optional[str] = None,
    # Velocity history for dynamic thresholds (file critique fix)
    velocity_history: Optional[list] = None,
) -> dict:
    """
    Classifies current market into one of 8 phases.

    File critique fix: velocity thresholds are now DYNAMIC percentiles.
    Uses velocity_history to compute regime-relative thresholds:
        LOW volatility day:  HIGH_VOL_THRESH = 4.0 (not 8.0)
        HIGH volatility day: HIGH_VOL_THRESH = 12.0
    This prevents the phase engine from mislabelling normal moves as
    TREND on high-vol days, or missing trends on low-vol days.
    """
    import numpy as _np2
    now = datetime.now().time()

    # ── Dynamic velocity thresholds (file critique fix) ───────────────────
    if velocity_history and len(velocity_history) >= 5:
        _vh = list(velocity_history)
        HIGH_VOL = max(float(_np2.percentile(_vh, 75)), 5.0)
        LOW_VOL  = max(float(_np2.percentile(_vh, 30)), 2.0)
    else:
        HIGH_VOL = 8.0
        LOW_VOL  = 4.0

    # ── 1. Opening discovery (hard time gate) ─────────────────────────────
    if now < dt_time(9, 45):
        return _make(
            'OPEN_DISCOVERY', 0.95,
            f"Pre-9:45 discovery — {now.strftime('%H:%M')}")

    # ── 2. Expiry-specific phases ─────────────────────────────────────────
    if is_expiry:
        pain_dist = abs(spot - max_pain) if max_pain > 0 else 999
        mins_to_close = _mins_to_close(now)

        # EXPIRY_BREAK: spot moving away from max pain, velocity rising, cascade building
        if (pain_dist > 75 and velocity > 6.0 and abs(norm_slope) > 0.4
                and cascade_fsm in ('BUILDING', 'CONFIRMED', 'FIRING')):
            conf = 0.85 + (0.10 if cascade_fsm == 'FIRING' else 0.0)
            return _make('EXPIRY_BREAK', min(conf, 1.0),
                         f"Breaking from pain {pain_dist:.0f}pt, cascade={cascade_fsm}, vel={velocity:.1f}")

        # EXPIRY_PIN: spot near max pain, velocity low
        if pain_dist <= 75 and velocity < 5.0 and mins_to_close > 45:
            conf = 0.80 + (0.15 if pain_dist < 30 else 0.0)
            return _make('EXPIRY_PIN', min(conf, 1.0),
                         f"Pinned {pain_dist:.0f}pt from pain {max_pain}, vel={velocity:.1f}")

        # EXPIRY_BREAK: late expiry + high velocity regardless of pain
        if mins_to_close < 90 and velocity > HIGH_VOL and abs(norm_slope) > 0.5:
            return _make('EXPIRY_BREAK', 0.80,
                         f"Late expiry ({mins_to_close}min) + surge vel={velocity:.1f}")

    # ── 3. Liquidity vacuum ───────────────────────────────────────────────
    if in_vacuum and velocity > LOW_VOL:
        conf = 0.75 + vacuum_severity * 0.20
        return _make('LIQUIDITY_VACUUM', min(conf, 1.0),
                     f"Vacuum active, severity={vacuum_severity:.2f}, vel={velocity:.1f}")

    # ── 4. Trend (dynamic HIGH_VOL threshold) ─────────────────────────────
    if (velocity > HIGH_VOL and abs(norm_slope) > 0.5
            and abs(net_w_oi) > STRONG_OI * 0.5):
        conf = min(0.65 + velocity / 40.0 + abs(norm_slope) * 0.2, 1.0)
        dir_ = 'UP' if norm_slope > 0 else 'DOWN'
        return _make('TREND', conf,
                     f"vel={velocity:.1f} slope={norm_slope:+.2f} dir={dir_} (thr={HIGH_VOL:.1f})")

    # ── 5. Compression ────────────────────────────────────────────────────
    if is_compressed and stagnation_counter >= 3:
        # Late-day compression is more meaningful
        late_boost = 0.10 if now >= dt_time(13, 30) else 0.0
        conf = 0.70 + late_boost
        return _make('COMPRESSION', conf,
                     f"Compressed + stagnation={stagnation_counter}, time={now.strftime('%H:%M')}")

    # ── 6. Mean reversion (dynamic LOW_VOL threshold) ─────────────────────
    if (acceptance_dist > dist_threshold * 1.5
            and velocity < LOW_VOL
            and abs(net_w_oi) < STRONG_OI * 0.4):
        return _make('MEAN_REVERSION', 0.70,
                     f"Extended {acceptance_dist:.0f}pt > thr {dist_threshold:.0f}pt, vel fading")

    # ── 7. Hysteresis: maintain previous phase if conditions still hold ────
    if prev_phase in ('TREND', 'COMPRESSION', 'EXPIRY_BREAK') and velocity > LOW_VOL:
        return _make(prev_phase, 0.60, f"Maintaining {prev_phase} (hysteresis)")

    # ── 8. Default: balanced ──────────────────────────────────────────────
    return _make('BALANCED', 0.65, "No dominant phase signal")


def _make(phase: str, confidence: float, reason: str) -> dict:
    mods = PHASE_MODIFIERS[phase]
    return {
        'phase':                 phase,
        'modifiers':             mods,
        'confidence':            round(float(confidence), 3),
        'reason':                reason,
        'trade_allowed_by_phase': mods['trade_allowed'],
        'quality_cap':           mods['quality_cap'],
        'note':                  mods['note'],
    }


def _mins_to_close(now: dt_time) -> int:
    now_m = now.hour * 60 + now.minute
    return max(15 * 60 + 30 - now_m, 0)


# ── Phase-aware quality cap ───────────────────────────────────────────────────

def apply_phase_quality_cap(raw_quality: int, phase_r: dict) -> int:
    """
    Caps the trade quality score at the phase maximum.
    Prevents high-quality scores during structurally unfavourable phases.
    """
    cap = phase_r.get('quality_cap', 100)
    return min(int(raw_quality), int(cap))


# ── Phase-aware weight modification ──────────────────────────────────────────

def get_phase_weight(phase_r: dict, driver: str, base_weight: float) -> float:
    """
    Returns phase-adjusted weight for a signal driver.
    Called by signal_engine when computing weighted scores.

    Example:
        base_weight = 0.20 (cascade in move_probability)
        phase = EXPIRY_BREAK → modifier = 2.0
        → effective weight = 0.40

    Weights are renormalized after application in move_probability.
    """
    mods = phase_r.get('modifiers', {})
    mult = mods.get(driver, 1.0)
    return base_weight * mult
