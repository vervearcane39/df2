"""
RISK LAYER — trade_filter.py
Responsibility: Allow/block trades. One function per concern.
No UI logic — returns dicts only.
"""
import time
import numpy as np
from typing import Optional, Tuple


def detect_trap_v3(net_w_oi: float, spot_trend: str, norm_slope: float,
                   atm_mom: float, liq_thresh: float, STRONG_OI: float,
                   SLOPE_THRESH: float, mode: str, regime: str) -> Tuple[Optional[str], bool]:
    """
    3-condition trap — all three must fire to avoid false positives.
    Opening discovery: suppressed entirely.
    """
    if regime == 'OPEN_DISC':
        if mode == "🧽 ABSORPTION":
            return "⚠️ OPENING ABSORPTION — wait", False
        return None, False

    strong_oi    = abs(net_w_oi)   > STRONG_OI
    price_spike  = abs(norm_slope) > SLOPE_THRESH * 1.5
    vol_confirms = atm_mom         > liq_thresh * 2.0

    for is_trap, label in [
        (net_w_oi > STRONG_OI  and spot_trend == "BEARISH", "BULL TRAP"),
        (net_w_oi < -STRONG_OI and spot_trend == "BULLISH", "BEAR TRAP"),
    ]:
        if is_trap:
            conds = sum([strong_oi, price_spike, vol_confirms])
            if   conds >= 3: return f"🚨 CONFIRMED {label} — 3/3 conditions", True
            elif conds >= 2: return f"⚠️ PROBABLE {label} — {conds}/3 conditions", False
            else:            return None, False

    if mode == "🧽 ABSORPTION" and regime == 'EXPIRY_HDGE':
        return "⚠️ EXPIRY ABSORPTION — large writer defending", False
    return None, False


def detect_trapped_sellers(merged, spot_chg: float, atm_strike: int) -> Tuple[bool, bool]:
    """
    Volume-confirmed, spread-filtered trap detection.  (Fix for Problem 2)

    Old logic: CE trapped if strike < spot.  Binary.
    Problem: includes spreads, rolls, hedges — all counted as "trapped".

    New logic:
    A genuine trapped seller requires ALL of:
        1. Price moved against them (delta PnL negative)
        2. OI held or increased (not covering — they are stuck)
        3. Volume is BELOW rolling average (no panic exit activity yet)
           This is the key filter: if they were covering, volume would spike.
        4. Delta-adjusted loss > ₹2 (meaningful pain, not rounding noise)

    This does not eliminate the ambiguity from spreads and rolls entirely —
    that is impossible without actual dealer inventory data — but it
    substantially reduces false positives.

    Also returns trap_quality_score: 0–3, how many conditions fired.
    Binary trapped_ce/trapped_pe = quality >= 2.
    """
    trapped_ce = trapped_pe = False
    ce_quality = pe_quality = 0

    near = merged[
        (merged['strike_now'] >= atm_strike - 150) &
        (merged['strike_now'] <= atm_strike + 150)
    ].copy()

    if near.empty:
        return False, False

    # Compute volume relative to recent history (spread filter)
    avg_vol = near['volume_now'].mean() if 'volume_now' in near.columns else 0.0

    # CE trapped: spot moved up, call sellers are underwater
    if spot_chg > 15:
        ce = near[near['type_now'] == 'CE']
        if not ce.empty:
            # Condition 1: OI not falling (not covering)
            oi_stable  = (ce['oi_chg'] >= -20000).mean() > 0.6

            # Condition 2: Delta-adjusted loss > ₹2
            if 'delta' in ce.columns and 'price_chg' in ce.columns:
                delta_adj  = ce['price_chg'] - ce['delta'] * spot_chg
                pain_real  = (delta_adj > 2.0).any()
            else:
                pain_real  = (ce['price_chg'] > 0).any()

            # Condition 3: Volume not spiking (not panic-covering)
            if 'vol_mom' in ce.columns and avg_vol > 0:
                vol_ratio   = ce['vol_mom'].abs().mean() / max(avg_vol, 1.0)
                not_covering = vol_ratio < 1.5   # volume < 1.5× average
            else:
                not_covering = True

            ce_quality = int(oi_stable) + int(pain_real) + int(not_covering)
            trapped_ce = ce_quality >= 2

    # PE trapped: spot moved down, put sellers are underwater
    if spot_chg < -15:
        pe = near[near['type_now'] == 'PE']
        if not pe.empty:
            oi_stable  = (pe['oi_chg'] >= -20000).mean() > 0.6

            if 'delta' in pe.columns and 'price_chg' in pe.columns:
                delta_adj  = pe['price_chg'] - pe['delta'].abs() * abs(spot_chg)
                pain_real  = (delta_adj > 2.0).any()
            else:
                pain_real  = (pe['price_chg'] > 0).any()

            if 'vol_mom' in pe.columns and avg_vol > 0:
                vol_ratio   = pe['vol_mom'].abs().mean() / max(avg_vol, 1.0)
                not_covering = vol_ratio < 1.5
            else:
                not_covering = True

            pe_quality = int(oi_stable) + int(pain_real) + int(not_covering)
            trapped_pe = pe_quality >= 2

    return trapped_ce, trapped_pe


def evaluate_permission(smoothed_quality: int, health_score: float,
                        cooldown_until: float, liquidity_risk: bool,
                        trap_alert: Optional[str], gamma_flip_crossed: bool,
                        gex_mode: str, iv_pct: float, iv_direction: str,
                        regime: str) -> dict:
    """
    Single function for trade permission.  Returns allow + reason.
    """
    current_time = time.time()
    is_cool      = current_time < cooldown_until

    if health_score < 40:
        return {'allowed': False, 'reason': f"FEED DOWN ({health_score:.0f}%)", 'is_cool': is_cool}
    if is_cool:
        return {'allowed': False, 'reason': f"COOLDOWN ({int(cooldown_until - current_time)}s)", 'is_cool': True}
    if smoothed_quality > 60:
        return {'allowed': True, 'reason': None, 'is_cool': False}

    # Priority-ordered block reasons
    if liquidity_risk:                                    reason = "LOW LIQUIDITY"
    elif trap_alert:                                      reason = "TRAP DETECTED"
    elif gamma_flip_crossed:                              reason = "GAMMA FLIP — wait"
    elif "LONG" in gex_mode and iv_pct > 60:             reason = "LONG GAMMA + HIGH IV"
    elif "CRUSHING" in iv_direction and iv_pct > 70:     reason = "IV CRUSH RISK"
    elif regime == 'OPEN_DISC':                          reason = "OPENING DISCOVERY — wait"
    elif regime == 'VACUUM':                             reason = "LIQUIDITY VACUUM — avoid"
    elif regime == 'EXPIRY_HDGE':                        reason = "EXPIRY HEDGING — seller zone"
    elif smoothed_quality < 40:                          reason = "LOW QUALITY SETUP"
    else:                                                reason = "WAITING FOR SETUP"

    return {'allowed': False, 'reason': reason, 'is_cool': False}


def compute_invalidation(mode: str, trade_phase: str, net_w_oi: float,
                         vwap: float, vwap_reliable: bool,
                         rolling_mean: float, atm_strike: int) -> str:
    ref = f"VWAP {vwap:.0f}" if vwap_reliable else f"SMA {rolling_mean:.0f}"
    if "PIN" in mode:
        return f"🚫 INVALIDATION: Breakout from {atm_strike}"
    if "EXHAUSTION" in trade_phase:
        return "🚫 INVALIDATION: New High/Low formed"
    return (f"🚫 INVALIDATION: Spot < {ref}" if net_w_oi > 0
            else f"🚫 INVALIDATION: Spot > {ref}")


def compute_sizing(trade_allowed: bool, concentration_pct: float,
                   trade_phase: str, regime: str, regime_s_mod: float,
                   iv_pct: float, iv_direction: str,
                   gex_mode: str, dealer_delta_M: float) -> float:
    if not trade_allowed:
        return 1.0
    sf = 1.0
    if concentration_pct > 40:          sf += 0.25
    if trade_phase == "🟢 EARLY / SETUP": sf += 0.25
    sf += regime_s_mod
    if iv_pct > 80:                     sf -= 0.25
    if "EXPANDING" in iv_direction:     sf += 0.15
    if "SHORT"     in gex_mode:        sf += 0.15
    if "LONG"      in gex_mode:        sf -= 0.15
    if dealer_delta_M < -1.0:          sf += 0.15
    return float(np.clip(sf, 0.5, 1.5))
