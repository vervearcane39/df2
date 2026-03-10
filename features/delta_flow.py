"""
FEATURE LAYER — delta_flow.py
Improvement 3 — Net Option Delta Flow

Options market moves when delta demand > available liquidity.
This module computes the aggregate directional delta being demanded
by option buyers and sellers each minute.

Why delta flow matters:
    Buying 1 lot of ATM call = demanding +0.50 delta from the dealer.
    The dealer must hedge by buying 0.50 lots of futures.
    Net delta demand = market-maker hedging PRESSURE = where spot is pushed.

Formula per row:
    flow_type = classify_oi_flow(price_chg, oi_chg)
    If LONG_BUILD:  buyers opening → positive delta demand for CE, negative for PE
    If SHORT_BUILD: sellers opening → negative delta demand for CE, positive for PE
    weight = oi_chg × delta × lot_size

Net delta flow:
    Positive → market absorbing bullish delta demand → bullish
    Negative → market absorbing bearish delta demand → bearish
    Near zero → no directional pressure

Normalised to [-1, +1] for use in signal scoring.
"""
import numpy as np
import pandas as pd
from typing import Tuple

from features.greeks import NIFTY_LOT_SIZE
from market.dealer_position import classify_oi_flow


def compute_delta_flow(merged: pd.DataFrame, spot: float,
                       radius: float = 500.0) -> dict:
    """
    Computes net option delta flow for the chain around spot.

    Returns:
        net_delta_flow:  raw value in delta-lots
        norm_signal:     [-1, +1] normalised
        state:           human-readable label
        bull_delta:      total bullish delta demand
        bear_delta:      total bearish delta demand
        imbalance_pct:   (bull - bear) / (bull + bear) × 100
    """
    empty = {'net_delta_flow': 0.0, 'norm_signal': 0.0,
             'state': '⚪ No Delta Flow', 'bull_delta': 0.0,
             'bear_delta': 0.0, 'imbalance_pct': 0.0}
    if merged.empty:
        return empty
    try:
        near = merged[abs(merged['strike_now'] - spot) <= radius].copy()
        if near.empty:
            near = merged.copy()

        bull = 0.0
        bear = 0.0

        for _, row in near.iterrows():
            delta  = abs(float(row.get('delta', 0.5)))
            oi_chg = float(row.get('oi_chg', 0.0))
            p_chg  = float(row.get('price_chg', 0.0))
            otype  = str(row.get('type_now', 'CE'))

            flow   = classify_oi_flow(p_chg, oi_chg)
            abs_oi = abs(oi_chg)
            if abs_oi < 500:
                continue   # noise threshold

            # Delta contribution per contract
            d_per_lot = delta if otype == 'CE' else -(1.0 - delta)  # CE positive, PE negative

            if flow == 'LONG_BUILD':
                # Public buying → demand is in direction of option delta
                contribution = abs_oi * abs(d_per_lot) * NIFTY_LOT_SIZE
                if d_per_lot > 0:
                    bull += contribution
                else:
                    bear += contribution

            elif flow == 'SHORT_BUILD':
                # Public selling → supply flowing against option direction
                contribution = abs_oi * abs(d_per_lot) * NIFTY_LOT_SIZE
                if d_per_lot > 0:
                    bear += contribution   # calls being sold → bearish delta supply
                else:
                    bull += contribution   # puts being sold → bullish delta supply

            elif flow in ('SHORT_COVER', 'LONG_UNWIND'):
                # Closing trades — half-weight, direction reverses
                contribution = abs_oi * abs(d_per_lot) * NIFTY_LOT_SIZE * 0.4
                if flow == 'SHORT_COVER':
                    bull += contribution
                else:
                    bear += contribution

        net   = bull - bear
        total = bull + bear + 1.0
        imbal = net / total * 100.0

        # Normalise [-1, +1] using total delta as scale
        scale = max(total * 0.1, 1.0)
        norm  = float(np.clip(net / scale, -1.0, 1.0))

        if   norm >  0.5: state = "🐂 STRONG BULLISH delta demand"
        elif norm >  0.2: state = "📈 MILD BULLISH delta demand"
        elif norm < -0.5: state = "🐻 STRONG BEARISH delta demand"
        elif norm < -0.2: state = "📉 MILD BEARISH delta demand"
        else:             state = "⚖️ Balanced delta flow"

        return {
            'net_delta_flow':  round(net / 1e6, 3),
            'norm_signal':     round(norm, 3),
            'state':           state,
            'bull_delta':      round(bull / 1e6, 3),
            'bear_delta':      round(bear / 1e6, 3),
            'imbalance_pct':   round(imbal, 1),
        }
    except Exception:
        return empty
