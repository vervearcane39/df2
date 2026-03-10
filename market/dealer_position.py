"""
MARKET STATE LAYER — dealer_position.py
Responsibility: Dealer delta exposure, max pain, strike magnetism.

Improvement 1 — Price-OI Classification (Doc 1.1):
    OI alone cannot tell you who opened what.
    The price-OI relationship is the correct classifier:

        Price ↑ + OI ↑  → Long build-up  (buyers opening)
        Price ↓ + OI ↑  → Short build-up (sellers opening)
        Price ↑ + OI ↓  → Short covering  (shorts exiting)
        Price ↓ + OI ↓  → Long unwinding  (longs exiting)

    This is classic open interest analysis and avoids the fundamental
    flaw of assuming "high CE OI = calls written."

    For dealer delta:
        Long build-up on CE  → public BUYING calls → dealers SHORT calls
                             → dealer delta negative (must buy to hedge)
        Short build-up on CE → public SELLING calls → dealers LONG calls
                             → dealer delta positive (must sell to hedge)
        Long build-up on PE  → public BUYING puts → dealers SHORT puts
                             → dealer delta positive (must sell to hedge)
        Short build-up on PE → public SELLING puts → dealers LONG puts
                             → dealer delta negative (must buy to hedge)

    Writer confidence is now derived from the price-OI signal,
    not just from raw OI levels.
"""
import numpy as np
import pandas as pd
from typing import Tuple

from features.greeks import NIFTY_LOT_SIZE


# ── Price-OI Classification ───────────────────────────────────────────────────

OI_FLOW_LABELS = {
    ('up', 'up'):   'LONG_BUILD',      # buyers opening  — bullish
    ('dn', 'up'):   'SHORT_BUILD',     # sellers opening — bearish
    ('up', 'dn'):   'SHORT_COVER',     # shorts closing  — mildly bullish
    ('dn', 'dn'):   'LONG_UNWIND',     # longs closing   — mildly bearish
    ('flat', 'up'): 'STALE_OI',
    ('up', 'flat'): 'PRICE_ONLY',
    ('dn', 'flat'): 'PRICE_ONLY',
    ('flat', 'dn'): 'STALE_OI',
    ('flat', 'flat'): 'NEUTRAL',
}


def classify_oi_flow(price_chg: float, oi_chg: float,
                     price_thresh: float = 0.5, oi_thresh: float = 500) -> str:
    """
    Returns one of: LONG_BUILD, SHORT_BUILD, SHORT_COVER, LONG_UNWIND,
                    STALE_OI, PRICE_ONLY, NEUTRAL
    """
    p = 'up' if price_chg > price_thresh else ('dn' if price_chg < -price_thresh else 'flat')
    o = 'up' if oi_chg > oi_thresh else ('dn' if oi_chg < -oi_thresh else 'flat')
    return OI_FLOW_LABELS.get((p, o), 'NEUTRAL')


def _dealer_sign_from_flow(flow: str, otype: str) -> float:
    """
    Returns sign ∈ {-1, 0, +1} of dealer's DELTA contribution.

    Logic:
    - LONG_BUILD:  public buying → dealer short → delta directional
    - SHORT_BUILD: public selling → dealer long → delta reverses
    - SHORT_COVER / LONG_UNWIND: closing trades → smaller signal (0.5×)
    - NEUTRAL / STALE: fall back to 0 (no directional information)

    CE dealer delta: negative means dealer must BUY futures to hedge.
    PE dealer delta: positive means dealer must SELL futures to hedge.
    """
    if flow == 'LONG_BUILD':
        return -1.0 if otype == 'CE' else +1.0   # dealer short option
    elif flow == 'SHORT_BUILD':
        return +1.0 if otype == 'CE' else -1.0   # dealer long option
    elif flow == 'SHORT_COVER':
        return -0.4 if otype == 'CE' else +0.4   # partial short cover
    elif flow == 'LONG_UNWIND':
        return +0.4 if otype == 'CE' else -0.4
    else:
        return 0.0   # no information


def compute_dealer_delta(merged: pd.DataFrame) -> dict:
    """
    Price-OI classification-based dealer delta.

    For each row, classify whether price moved with or against OI,
    then assign dealer sign accordingly.
    Net negative → dealers must BUY → bullish squeeze risk.
    Net positive → dealers must SELL → bearish pressure.
    """
    empty = {'dealer_delta_M': 0.0, 'pressure': '⚖️ Neutral',
             'buyer_contribution': 0, 'skew_note': 'NO DATA',
             'ce_oi_total': 0, 'pe_oi_total': 0,
             'flow_breakdown': {}}
    if merged.empty:
        return empty
    try:
        dd = 0.0
        flow_counts = {}
        total_ce = float(merged[merged['type_now'] == 'CE']['oi_now'].sum())
        total_pe = float(merged[merged['type_now'] == 'PE']['oi_now'].sum())

        for _, row in merged.iterrows():
            delta  = abs(float(row.get('delta', 0.5)))
            oi     = max(float(row.get('oi_now', 0)), 0.0)
            otype  = str(row.get('type_now', 'CE'))
            p_chg  = float(row.get('price_chg', 0.0))
            oi_chg = float(row.get('oi_chg', 0.0))

            flow   = classify_oi_flow(p_chg, oi_chg)
            sign   = _dealer_sign_from_flow(flow, otype)
            flow_counts[flow] = flow_counts.get(flow, 0) + 1

            # Greek to use: CE use delta, PE use (1-delta) for put exposure
            greek  = delta if otype == 'CE' else (1.0 - delta)
            dd    += sign * oi * greek

        dd *= NIFTY_LOT_SIZE

        if   dd < -1_000_000: pressure = "⬆️ DEALER BUYING  (bullish squeeze)"
        elif dd >  1_000_000: pressure = "⬇️ DEALER SELLING (bearish pressure)"
        else:                 pressure = "⚖️ Neutral Dealer Hedging"

        # Dominant flow type for display
        dominant = max(flow_counts, key=flow_counts.get) if flow_counts else 'NEUTRAL'
        skew_note = f"Dominant flow: {dominant} ({flow_counts.get(dominant, 0)} strikes)"

        return {
            'dealer_delta_M':     round(dd / 1e6, 3),
            'pressure':           pressure,
            'buyer_contribution': 1 if dd < 0 else 0,
            'skew_note':          skew_note,
            'ce_oi_total':        int(total_ce),
            'pe_oi_total':        int(total_pe),
            'flow_breakdown':     flow_counts,
            # P1 fix: Dealer uncertainty
            # Real dealer inventory includes OTC, block, cross-market hedges.
            # Our estimate uses only exchange OI × model delta.
            # Conservative uncertainty: ±25% (OTC hidden, roll contamination)
            'dealer_delta_low':   round(dd / 1e6 * 0.75, 3),
            'dealer_delta_high':  round(dd / 1e6 * 1.25, 3),
            'dealer_uncertainty_pct': 25,
            'dealer_note': (
                "±25% estimate. OTC options, block trades, and cross-market "
                "futures hedges are not visible in exchange OI data."
            ),
        }
    except Exception:
        return empty


def compute_max_pain_and_wall(now_df: pd.DataFrame, spot: float) -> Tuple[int, int]:
    """Max pain = strike minimising total option buyer payout at expiry."""
    default = int(round(spot / 50) * 50)
    if now_df.empty:
        return default, default
    try:
        ce_oi   = now_df[now_df['type'] == 'CE'].groupby('strike')['oi'].sum()
        pe_oi   = now_df[now_df['type'] == 'PE'].groupby('strike')['oi'].sum()
        strikes = sorted(now_df['strike'].unique())
        min_pay, max_pain = float('inf'), default
        for ep in strikes:
            pay  = sum(max(ep - k, 0) * oi for k, oi in ce_oi.items())
            pay += sum(max(k - ep, 0) * oi for k, oi in pe_oi.items())
            if pay < min_pay:
                min_pay  = pay
                max_pain = ep
        oi_total   = now_df.groupby('strike')['oi'].sum()
        gamma_wall = int(oi_total.idxmax()) if not oi_total.empty else default
        return int(max_pain), gamma_wall
    except Exception:
        return default, default


def compute_strike_magnetism(merged: pd.DataFrame, spot: float) -> dict:
    """Magnet strength = strike_total_OI / total_chain_OI. >15% = pin zone."""
    if merged.empty:
        return {'magnet_strike': None, 'pct_dict': {}, 'dist': 9999}
    try:
        by_strike = merged.groupby('strike_now')['oi_now'].sum()
        total_oi  = max(float(by_strike.sum()), 1.0)
        pct       = by_strike / total_oi * 100
        strong    = pct[pct > 15.0]
        m_strike  = int(strong.idxmax()) if not strong.empty else None
        dist      = abs(spot - m_strike) if m_strike else 9999
        return {'magnet_strike': m_strike, 'pct_dict': pct.to_dict(), 'dist': dist}
    except Exception:
        return {'magnet_strike': None, 'pct_dict': {}, 'dist': 9999}
