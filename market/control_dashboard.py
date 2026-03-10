"""
MARKET STATE LAYER — control_dashboard.py
Upgrade 3: Unified Market Control Dashboard

Combines dealer delta + gamma regime + futures flow + OI flow
into a single BUYERS vs SELLERS dominance score and visual state.

This makes signal interpretation fast:
    "Everything bullish: dealer buying, short gamma, aggressive call flow, futures surging"
    vs.
    "Mixed: dealer neutral, long gamma (pinning), passive flow, no futures"

Output:
    control_score:  0–10 composite score (5 = neutral)
    quadrant:       RISK-ON / RISK-OFF / PINNED / MIXED
    components:     named signals with direction and strength
    summary_line:   one human-readable sentence
"""
import numpy as np
from typing import Dict


def compute_market_control(
    # Dealer
    dealer_delta_M:  float,
    dealer_pressure: str,
    # Gamma
    net_gex:         float,
    gex_mode:        str,
    near_gex_wall:   bool,
    # Futures
    futures_signal:  float,
    futures_available: bool,
    # OI flow
    net_w_oi:        float,
    STRONG_OI:       float,
    flow_state:      str,
    trapped_ce:      bool,
    trapped_pe:      bool,
    # Structure
    vwap_above:      bool,
    spot_trend:      str,
    # Volatility
    iv_pct:          float,
    iv_direction:    str,
) -> dict:
    """
    Produces a 0–10 unified control score.
    5 = perfectly balanced.
    10 = complete buyer dominance.
    0  = complete seller dominance.
    """
    components = {}
    score = 5.0   # neutral baseline

    # ── Dealer (±1.5) ─────────────────────────────────────────────────────
    if   dealer_delta_M < -2.0:  d_pts, d_lbl =  1.5, "⬆️ Strong dealer buying"
    elif dealer_delta_M < -0.5:  d_pts, d_lbl =  0.8, "↗️ Mild dealer buying"
    elif dealer_delta_M >  2.0:  d_pts, d_lbl = -1.5, "⬇️ Strong dealer selling"
    elif dealer_delta_M >  0.5:  d_pts, d_lbl = -0.8, "↘️ Mild dealer selling"
    else:                         d_pts, d_lbl =  0.0, "⟷ Dealer neutral"
    score += d_pts
    components['dealer'] = {'score': d_pts, 'label': d_lbl}

    # ── Gamma (±1.5) ──────────────────────────────────────────────────────
    if   net_gex < -3.0 and not near_gex_wall: g_pts, g_lbl =  1.0, "💥 Short gamma (amplifying)"
    elif net_gex < -3.0 and near_gex_wall:     g_pts, g_lbl =  0.0, "⚠️ Short gamma near wall"
    elif net_gex >  3.0:                        g_pts, g_lbl = -1.0, "📌 Long gamma (pinning)"
    else:                                       g_pts, g_lbl =  0.0, "⚖️ Neutral gamma"
    score += g_pts
    components['gamma'] = {'score': g_pts, 'label': g_lbl}

    # ── Futures (±1.5, only if available) ─────────────────────────────────
    if futures_available:
        if   futures_signal >  0.6: f_pts, f_lbl =  1.5, "🚀 Aggressive futures buying"
        elif futures_signal >  0.3: f_pts, f_lbl =  0.8, "↗️ Moderate futures buying"
        elif futures_signal < -0.6: f_pts, f_lbl = -1.5, "🔻 Aggressive futures selling"
        elif futures_signal < -0.3: f_pts, f_lbl = -0.8, "↘️ Moderate futures selling"
        else:                       f_pts, f_lbl =  0.0, "⟷ Neutral futures"
        score += f_pts
        components['futures'] = {'score': f_pts, 'label': f_lbl}
    else:
        components['futures'] = {'score': 0.0, 'label': '⚪ No futures data'}

    # ── OI flow (±2.0) ────────────────────────────────────────────────────
    oi_ratio = abs(net_w_oi) / max(STRONG_OI, 1.0)
    if   trapped_ce:             oi_pts, oi_lbl =  2.0, "🪤 Call sellers trapped"
    elif trapped_pe:             oi_pts, oi_lbl = -2.0, "🪤 Put sellers trapped"
    elif net_w_oi < -STRONG_OI:  oi_pts, oi_lbl =  1.5, "📈 Heavy put writing"
    elif net_w_oi >  STRONG_OI:  oi_pts, oi_lbl = -1.5, "📉 Heavy call writing"
    elif "AGGRESSIVE" in flow_state: oi_pts, oi_lbl = 1.0, "🔥 Aggressive option buying"
    elif "Active"     in flow_state: oi_pts, oi_lbl = 0.5, "📈 Active flow"
    elif "Writing"    in flow_state: oi_pts, oi_lbl = -0.5, "✍️ Passive writing"
    else:                            oi_pts, oi_lbl = 0.0, "⟷ Mixed OI flow"
    score += oi_pts
    components['oi_flow'] = {'score': oi_pts, 'label': oi_lbl}

    # ── Structure (±0.5) ──────────────────────────────────────────────────
    if   vwap_above and spot_trend == "BULLISH": s_pts, s_lbl =  0.5, "✅ Above VWAP"
    elif not vwap_above and spot_trend == "BEARISH": s_pts, s_lbl = -0.5, "❌ Below VWAP"
    else:                                        s_pts, s_lbl =  0.0, "⟷ Mixed structure"
    score += s_pts
    components['structure'] = {'score': s_pts, 'label': s_lbl}

    # ── Volatility modifier (±0.5) ────────────────────────────────────────
    if   iv_pct < 20 and "EXPANDING" in iv_direction: v_pts, v_lbl =  0.5, "🟢 IV cheap + expanding"
    elif iv_pct > 80 and "CRUSHING"  in iv_direction: v_pts, v_lbl = -0.5, "🔴 IV rich + crushing"
    else:                                              v_pts, v_lbl =  0.0, "⟷ IV neutral"
    score += v_pts
    components['volatility'] = {'score': v_pts, 'label': v_lbl}

    # ── Final score: clip to [0, 10] ──────────────────────────────────────
    score = float(np.clip(score, 0.0, 10.0))

    # ── Quadrant classification ────────────────────────────────────────────
    if   score >= 7.5:             quadrant = "🚀 RISK-ON  — buyers dominant"
    elif score >= 6.0:             quadrant = "📈 BUYER LEAN"
    elif score >= 4.0 and score < 6.0:
        if near_gex_wall or abs(net_gex) < 1.0:
            quadrant = "📌 PINNED — gamma wall contains moves"
        else:
            quadrant = "⚖️ MIXED — no clear edge"
    elif score >= 2.5:             quadrant = "📉 SELLER LEAN"
    else:                          quadrant = "🔻 RISK-OFF — sellers dominant"

    # ── One-line summary ──────────────────────────────────────────────────
    top_components = sorted(components.items(),
                            key=lambda kv: abs(kv[1]['score']), reverse=True)[:3]
    summary_parts  = [c['label'] for _, c in top_components if c['score'] != 0.0]
    summary_line   = " + ".join(summary_parts) if summary_parts else "No dominant force"

    # ── Bar representation ────────────────────────────────────────────────
    filled = int(round(score))
    bar    = "█" * filled + "░" * (10 - filled)

    return {
        'score':       round(score, 2),
        'bar':         bar,
        'quadrant':    quadrant,
        'components':  components,
        'summary_line': summary_line,
    }
