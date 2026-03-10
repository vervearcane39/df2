"""
DASHBOARD LAYER — panels.py
Responsibility: Render HTML panels.  Zero business logic.
"""
import streamlit as st
import uuid
import pandas as pd



# ═══════════════════════════════════════════════════════════════════════════════
# FOCUS MODE PANEL
# One screen. Everything you need. Nothing you don't.
# ═══════════════════════════════════════════════════════════════════════════════

def focus_mode_panel(m: dict, table=None):
    """
    Focus Mode: trader-first view built around one question —
    "Should I trade right now, and if so, in which direction?"

    Layout:
        Row 1: PERMISSION (full width, giant)
        Row 2: 5-Condition counter | Spot dashboard | Direction
        Row 3: Levels bar
        Row 4: ATM ±4 strikes chain (stripped down)
    """
    import numpy as np

    spot        = m.get('spot', 0.0)
    atm         = m.get('atm_strike', int(round(spot / 50) * 50))
    vwap        = m.get('vwap', 0.0)
    vwap_above  = m.get('vwap_above', False)
    vwap_dist   = m.get('vwap_dist', 0.0)
    nwa         = m.get('next_wall_above')
    nwb         = m.get('next_wall_below')
    exp_move    = m.get('exp_move', 0.0)

    n_cond      = m.get('hz_conditions_met', 0)
    cond_lbl    = m.get('hz_cond_labels', {})
    core_cond   = m.get('hz_core_conditions', {})
    hz          = m.get('hz_score', 0.0)
    hz_conf     = m.get('hz_confidence', 'DORMANT')

    move_prob   = m.get('move_prob', 0.0)
    p_up        = m.get('p_up', 0.5)
    p_dn        = m.get('p_dn', 0.5)
    dirn        = m.get('dir_direction', 'NEUTRAL')
    dir_lbl     = m.get('dir_label', '')

    pressure    = float(m.get('pressure_score', 0.0))
    buildup     = m.get('buildup_phase', 'DORMANT')
    pm          = float(m.get('persist_mult', 1.0))

    net_gex     = m.get('net_gex', 0.0)
    gex_mode    = m.get('gex_mode', '—').split('(')[0].strip()
    pain        = m.get('pain', 'NEUTRAL')
    phase       = m.get('market_phase', 'BALANCED').replace('_', ' ')
    trap        = m.get('trap_alert')
    regime      = m.get('regime_label', 'MID')
    is_expiry   = m.get('is_expiry', False)

    # ── ROW 1: PERMISSION ────────────────────────────────────────────────
    if m.get('is_cool'):
        perm_bg, perm_border, perm_text, perm_sub = (
            'rgba(0,200,255,.10)', '#00C8FF', '❄️ COOLDOWN', 'Resetting mental state...')
    elif m.get('trade_allowed'):
        size_fac = m.get('size_fac', 1.0)
        size_tag = 'AGGRESSIVE' if size_fac > 1.0 else 'DEFENSIVE' if size_fac < 1.0 else 'NORMAL'
        perm_bg, perm_border, perm_text = (
            'rgba(0,200,80,.12)', '#00C850',
            f'🟢 EXECUTE &nbsp; {size_fac}×')
        perm_sub = f'{size_tag} &nbsp;|&nbsp; {phase} &nbsp;|&nbsp; {regime}'
    else:
        perm_bg, perm_border = 'rgba(255,50,50,.10)', '#FF3232'
        perm_text = f'🔴 SIT OUT'
        perm_sub  = m.get('reason', 'WAITING FOR SETUP')

    expiry_badge = (
        "<span style='background:#FF3232;color:#fff;font-size:11px;"
        "padding:2px 8px;border-radius:4px;letter-spacing:1px;'>EXPIRY</span>"
        if is_expiry else '')

    trap_html = ''
    if trap:
        trap_html = (
            f"<div style='margin-top:8px;padding:6px 12px;background:rgba(255,75,75,.15);"
            f"border:1px solid #FF4B4B;border-radius:4px;color:#FF4B4B;"
            f"font-size:13px;font-weight:bold;'>{trap}</div>")

    st.markdown(
        f"<div style='background:{perm_bg};border:3px solid {perm_border};"
        f"border-radius:12px;padding:22px 28px;margin-bottom:14px;"
        f"font-family:monospace;'>"
        f"<div style='display:flex;align-items:center;justify-content:space-between;'>"
        f"<div style='font-size:36px;font-weight:bold;color:{perm_border};'>{perm_text}</div>"
        f"{expiry_badge}</div>"
        f"<div style='font-size:14px;color:#AAA;margin-top:4px;'>{perm_sub}</div>"
        f"{trap_html}"
        f"</div>",
        unsafe_allow_html=True)

    # ── ROW 2: 3 columns ─────────────────────────────────────────────────
    col_cond, col_spot, col_dir = st.columns([1, 1, 1])

    # -- LEFT: 5-Condition counter ----------------------------------------
    with col_cond:
        cond_colour = {0:'#444',1:'#555',2:'#777',3:'#FFA500',4:'#FF6400',5:'#FF0000'}
        cond_bg     = {0:'rgba(60,60,60,.05)', 1:'rgba(60,60,60,.07)',
                       2:'rgba(80,80,80,.10)', 3:'rgba(255,165,0,.10)',
                       4:'rgba(255,100,0,.15)', 5:'rgba(255,0,0,.20)'}
        c = cond_colour.get(n_cond, '#444')
        bg = cond_bg.get(n_cond, 'rgba(60,60,60,.05)')

        cond_order = ['short_gamma','hedge_pressure','vacuum','directional','compression']
        cond_rows = ''.join(
            f"<div style='padding:4px 8px;margin:2px 0;border-radius:3px;"
            f"background:{'rgba(0,180,80,.12)' if core_cond.get(k) else 'rgba(50,50,50,.3)'};"
            f"border-left:3px solid {'#00B450' if core_cond.get(k) else '#2a2a2a'};"
            f"font-size:11px;color:{'#00C864' if core_cond.get(k) else '#555'};'>"
            f"{cond_lbl.get(k, k)}</div>"
            for k in cond_order)

        st.markdown(
            f"<div style='background:{bg};border:2px solid {c};border-radius:10px;"
            f"padding:14px;font-family:monospace;height:100%;'>"
            f"<div style='font-size:10px;color:#555;letter-spacing:2px;margin-bottom:6px;'>CONDITIONS</div>"
            f"<div style='font-size:44px;font-weight:bold;color:{c};line-height:1;'>{n_cond}<span style='font-size:20px;color:#444;'>/5</span></div>"
            f"<div style='margin:8px 0;'>{cond_rows}</div>"
            f"</div>",
            unsafe_allow_html=True)

    # -- CENTRE: Spot dashboard -------------------------------------------
    with col_spot:
        vwap_col   = '#00C864' if vwap_above else '#FF4B4B'
        vwap_arr   = '▲' if vwap_above else '▼'
        gex_col    = '#FF4B4B' if net_gex < 0 else '#00C864'
        pres_col   = '#FFA500' if pressure > 50 else ('#FF4B4B' if pressure > 70 else '#555')

        wall_above_str = f"▲ {int(nwa)}" if nwa else "▲ —"
        wall_below_str = f"▼ {int(nwb)}" if nwb else "▼ —"
        exp_str = f"±{exp_move:.0f}pts" if exp_move else "—"

        st.markdown(
            f"<div style='background:#0d1117;border:1px solid #222;border-radius:10px;"
            f"padding:14px;font-family:monospace;height:100%;'>"
            f"<div style='font-size:10px;color:#555;letter-spacing:2px;margin-bottom:6px;'>SPOT</div>"
            f"<div style='font-size:44px;font-weight:bold;color:#EEE;line-height:1;'>{spot:.0f}</div>"
            f"<div style='margin:10px 0 4px 0;display:flex;gap:10px;flex-wrap:wrap;'>"
            f"<span style='color:{vwap_col};font-size:13px;'>{vwap_arr} VWAP {vwap:.0f} ({vwap_dist:+.0f})</span>"
            f"<span style='color:#555;font-size:13px;'>|</span>"
            f"<span style='color:{gex_col};font-size:13px;'>GEX {net_gex:+.1f}M {gex_mode}</span>"
            f"</div>"
            f"<div style='display:flex;gap:12px;margin-top:8px;'>"
            f"<div style='background:#111;border:1px solid #1e2a1e;border-radius:5px;padding:5px 10px;flex:1;text-align:center;'>"
            f"<div style='font-size:10px;color:#555;'>WALL ABOVE</div>"
            f"<div style='color:#00C864;font-size:15px;font-weight:bold;'>{wall_above_str}</div></div>"
            f"<div style='background:#111;border:1px solid #2a1e1e;border-radius:5px;padding:5px 10px;flex:1;text-align:center;'>"
            f"<div style='font-size:10px;color:#555;'>WALL BELOW</div>"
            f"<div style='color:#FF4B4B;font-size:15px;font-weight:bold;'>{wall_below_str}</div></div>"
            f"<div style='background:#111;border:1px solid #222;border-radius:5px;padding:5px 10px;flex:1;text-align:center;'>"
            f"<div style='font-size:10px;color:#555;'>EXP MOVE</div>"
            f"<div style='color:#888;font-size:14px;font-weight:bold;'>{exp_str}</div></div>"
            f"</div>"
            f"<div style='margin-top:10px;padding-top:8px;border-top:1px solid #1a1a1a;"
            f"font-size:11px;color:#444;'>"
            f"<span style='color:{pres_col};'>● {buildup} ×{pm:.2f}</span>"
            f"&nbsp;|&nbsp;{pain}"
            f"</div>"
            f"</div>",
            unsafe_allow_html=True)

    # -- RIGHT: Direction -------------------------------------------------
    with col_dir:
        up_w   = int(p_up * 100)
        dn_w   = int(p_dn * 100)
        dir_col = '#00C864' if dirn == 'UP' else ('#FF4B4B' if dirn == 'DOWN' else '#888')
        dir_icon = '🔼' if dirn == 'UP' else ('🔽' if dirn == 'DOWN' else '↔️')

        hz_col  = '#FF0000' if hz >= 70 else ('#FFA500' if hz >= 50 else ('#00C8FF' if hz >= 30 else '#444'))

        move_col = '#FF4B4B' if move_prob >= 70 else ('#FFA500' if move_prob >= 50 else '#555')

        st.markdown(
            f"<div style='background:#0d1117;border:1px solid #222;border-radius:10px;"
            f"padding:14px;font-family:monospace;height:100%;'>"
            f"<div style='font-size:10px;color:#555;letter-spacing:2px;margin-bottom:6px;'>DIRECTION</div>"
            f"<div style='font-size:40px;font-weight:bold;color:{dir_col};line-height:1;'>"
            f"{dir_icon} {dirn}</div>"
            f"<div style='display:flex;gap:6px;margin:10px 0;'>"
            f"<div style='flex:{up_w};background:#00C86433;border-radius:3px 0 0 3px;height:8px;'></div>"
            f"<div style='flex:{dn_w};background:#FF4B4B33;border-radius:0 3px 3px 0;height:8px;'></div>"
            f"</div>"
            f"<div style='font-size:12px;color:#888;margin-bottom:10px;'>"
            f"<span style='color:#00C864;'>P↑ {p_up*100:.0f}%</span> &nbsp;"
            f"<span style='color:#FF4B4B;'>P↓ {p_dn*100:.0f}%</span></div>"
            f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:6px;'>"
            f"<div style='background:#111;border:1px solid #1a1a1a;border-radius:5px;padding:6px 10px;'>"
            f"<div style='font-size:10px;color:#555;'>MOVE PROB</div>"
            f"<div style='color:{move_col};font-size:20px;font-weight:bold;'>{move_prob:.0f}%</div></div>"
            f"<div style='background:#111;border:1px solid #1a1a1a;border-radius:5px;padding:6px 10px;'>"
            f"<div style='font-size:10px;color:#555;'>HERO-ZERO</div>"
            f"<div style='color:{hz_col};font-size:20px;font-weight:bold;'>{hz:.0f}</div>"
            f"<div style='font-size:9px;color:#444;'>{hz_conf}</div></div>"
            f"</div>"
            f"</div>",
            unsafe_allow_html=True)

    # ── ROW 3: ATM chain — stripped to essentials ────────────────────────
    st.markdown("<div style='margin-top:16px;font-size:10px;color:#444;"
                "letter-spacing:2px;margin-bottom:6px;'>ATM CHAIN  ±200pts</div>",
                unsafe_allow_html=True)

    if table is not None and not table.empty:
        view_range = slice(atm - 200, atm + 200)
        view = table.loc[view_range].copy()

        keep = [c for c in
                ['zone', 'oi_chg_ce', 'close_now_ce', 'close_now_pe', 'oi_chg_pe', 'intent_ce', 'intent_pe']
                if c in view.columns]
        if keep:
            view = view[keep]

            def _hi_atm(row):
                if row.name == atm:
                    return ['background-color:#1e2430;border-top:2px solid #00C8FF;'
                            'border-bottom:2px solid #00C8FF;'] * len(row)
                return [''] * len(row)

            safe_ce = max(float(view['oi_chg_ce'].fillna(0).max()), 1) if 'oi_chg_ce' in view.columns else 1
            safe_pe = max(float(view['oi_chg_pe'].fillna(0).max()), 1) if 'oi_chg_pe' in view.columns else 1

            styled = view.style.apply(_hi_atm, axis=1)
            if 'oi_chg_ce' in view.columns:
                styled = styled.background_gradient(subset=['oi_chg_ce'], cmap='Reds', vmin=0, vmax=safe_ce)
            if 'oi_chg_pe' in view.columns:
                styled = styled.background_gradient(subset=['oi_chg_pe'], cmap='Greens', vmin=0, vmax=safe_pe)
            if 'oi_chg_ce' in view.columns:
                styled = styled.format("{:,.0f}", subset=['oi_chg_ce', 'oi_chg_pe'])

            st.dataframe(styled, height=420, width='stretch')


def permission_panel(m: dict):
    if m['is_cool']:
        st.markdown("<div class='permission-box-blue'>❄️ COOLDOWN<br>"
                    "<span style='font-size:16px'>Resetting…</span></div>",
                    unsafe_allow_html=True)
    elif m['trade_allowed']:
        tag = "AGGRESSIVE" if m['size_fac'] > 1.0 else "DEFENSIVE" if m['size_fac'] < 1.0 else "NORMAL"
        st.markdown(
            f"<div class='permission-box-green'>🟢 EXECUTE ({m['size_fac']}x)<br>"
            f"<span style='font-size:16px'>{tag} | {m['phase']} | {m['regime_label']}</span></div>",
            unsafe_allow_html=True)
    else:
        st.markdown(
            f"<div class='permission-box-red'>🔴 SIT OUT<br>"
            f"<span style='font-size:16px'>{m['reason']}</span></div>",
            unsafe_allow_html=True)


def direction_panel(m: dict):
    bias = m['directional_bias']
    bc   = "#00FF00" if "CALL" in bias else "#FF4B4B" if "PUT" in bias else "#AAAAAA"
    bg   = ("rgba(0,255,0,.07)"   if "CALL" in bias else
            "rgba(255,75,75,.07)" if "PUT"  in bias else "rgba(128,128,128,.07)")
    st.markdown(
        f"<div class='direction-box' style='border:2px solid {bc};background:{bg};color:{bc};'>"
        f"{bias} &nbsp;|&nbsp; {m['ctrl_label']} &nbsp;|&nbsp; "
        f"Conf {m['confidence_pct']:.0f}%</div>",
        unsafe_allow_html=True)


def weighted_score_panel(m: dict):
    score = m['weighted_score']
    bar   = "█" * int(abs(score) * 20)
    direction = "▲ BULL" if score > 0 else "▼ BEAR"
    colour    = "#00FF00" if score > 0.25 else "#FF4B4B" if score < -0.25 else "#AAAAAA"
    c = m['score_components']
    st.markdown(
        f"<div class='layer-box'>"
        f"<b>Weighted Score:</b> {score:+.3f} {direction} [{bar}]&nbsp; "
        f"| D:{c['dealer']:+.2f} G:{c['gamma']:+.2f} F:{c['flow']:+.2f} "
        f"V:{c['vol']:+.2f} S:{c['struct']:+.2f}"
        f"</div>", unsafe_allow_html=True)


def layered_quality_panel(m: dict):
    def _bar(s):
        return "█" * int(s / 10) + "░" * (10 - int(s / 10))
    sq = m['scores']
    st.markdown(
        f"<div class='layer-box'>"
        f"<b>STRUCTURE</b> {_bar(sq['s'])} {sq['s']} &nbsp;&nbsp;"
        f"<b>FLOW</b> {_bar(sq['f'])} {sq['f']} &nbsp;&nbsp;"
        f"<b>VOLATILITY</b> {_bar(sq['v'])} {sq['v']} &nbsp;&nbsp;"
        f"<b>POSITIONING</b> {_bar(sq['p'])} {sq['p']} &nbsp;&nbsp;"
        f"→ Q:{m['quality']}"
        f"</div>", unsafe_allow_html=True)


def iv_panel(m: dict):
    skew = m['skew_25d']
    skew_txt = (f"25Δ PUT SKEW +{skew:.1%}" if skew > 0.01 else
                f"25Δ CALL SKEW {skew:.1%}"  if skew < -0.01 else "25Δ FLAT")
    st.markdown(
        f"<div class='iv-box'>"
        f"<b>IV:</b> {m['atm_iv']:.1%} &nbsp;|&nbsp; "
        f"<b>%tile:</b> {m['iv_pct']:.0f}th &nbsp;|&nbsp; "
        f"<b>{m['iv_label']}</b> &nbsp;|&nbsp; "
        f"<b>Trend:</b> {m['iv_direction']} (Δ{m['iv_change_rate']:+.3f}%/min) &nbsp;|&nbsp; "
        f"<b>RV:</b> {m['rv']:.1%} &nbsp;|&nbsp; "
        f"<b>{m['iv_rv_label']}</b> &nbsp;|&nbsp; "
        f"<b>ExpMove:</b> ±{m['exp_move']:.0f} &nbsp;|&nbsp; "
        f"<b>{skew_txt}</b> &nbsp;|&nbsp; "
        f"<b>DTE:</b> {m['dte_days']}d"
        f"</div>", unsafe_allow_html=True)


def vwap_panel(m: dict):
    pos   = "🟢 ABOVE" if m['vwap_above'] else "🔴 BELOW"
    slope = m['vwap_slope']
    stxt  = (f"↑ {slope:+.1f}" if slope > 0.5 else
             f"↓ {slope:+.1f}" if slope < -0.5 else "→ FLAT")
    warn  = "⚠️" if not m['vwap_reliable'] else ""
    st.markdown(
        f"<div class='vwap-box'>"
        f"<b>VWAP (equal-wt){warn}:</b> {m['vwap']:.0f} &nbsp;|&nbsp; "
        f"<b>{pos}</b> ({m['vwap_dist']:.0f} pts) &nbsp;|&nbsp; "
        f"<b>1SD:</b> {m['vwap_lower']:.0f}–{m['vwap_upper']:.0f} &nbsp;|&nbsp; "
        f"<b>Slope:</b> {stxt}"
        f"</div>", unsafe_allow_html=True)


def gex_panel(m: dict):
    flip = f"Flip@{m['gex_flip']}" if m['gex_flip'] else "No flip"
    st.markdown(
        f"<div class='gex-box'>"
        f"<b>GEX (total OI):</b> {m['gex_mode']} &nbsp;|&nbsp; "
        f"<b>Net:</b> {m['net_gex']:+.1f}M &nbsp;|&nbsp; "
        f"<b>Wall:</b> {m['gex_wall']} &nbsp;|&nbsp; "
        f"<b>{flip}</b> &nbsp;|&nbsp; "
        f"<b>OI Wall:</b> {m['gamma_wall_oi']} &nbsp;|&nbsp; "
        f"<b>DynOI:</b> {m['strong_oi']/1000:.0f}k"
        f"</div>", unsafe_allow_html=True)


def dealer_flow_panel(m: dict):
    """Improvement 1: shows price-OI flow breakdown, not just raw OI level."""
    prox    = "(⚠️ NEAR)" if m['magnet_dist'] < 100 else f"({m['magnet_dist']:.0f}pts)"
    fb      = m.get('dealer_flow_breakdown', {})
    # Dominant flow type from price-OI classification
    dom_flow = max(fb, key=fb.get) if fb else '—'
    flow_colors = {
        'LONG_BUILD':   '#00FF00',  # bullish
        'SHORT_BUILD':  '#FF4B4B',  # bearish
        'SHORT_COVER':  '#7FFF00',  # mildly bullish
        'LONG_UNWIND':  '#FFA500',  # mildly bearish
        'STALE_OI':     '#888888',
        'NEUTRAL':      '#888888',
    }
    dom_col = flow_colors.get(dom_flow, '#CCCCCC')
    fb_str  = ' '.join(f"{k[:2]}:{v}" for k, v in sorted(fb.items(), key=lambda x: -x[1])[:4]) if fb else '—'
    # Delta flow (Improvement 3)
    df_norm  = m.get('delta_flow_norm', 0.0)
    df_state = m.get('delta_flow_state', '—')
    df_col   = '#00FF00' if df_norm > 0.2 else ('#FF4B4B' if df_norm < -0.2 else '#888888')
    st.markdown(
        f"<div class='dealer-box'>"
        f"<b>Dealer δ:</b> {m['dealer_delta_M']:+.2f}M → {m['dealer_pressure'][:30]} &nbsp;|&nbsp; "
        f"<b>OI Flow (price×OI):</b> <span style='color:{dom_col};font-weight:bold;'>{dom_flow}</span> "
        f"<span style='font-size:12px;color:#888;'>[{fb_str}]</span> &nbsp;|&nbsp; "
        f"<b>δ Flow:</b> <span style='color:{df_col};'>{df_norm:+.2f} — {df_state[:30]}</span> &nbsp;|&nbsp; "
        f"<b>Magnet:</b> {m['magnet_strike'] or '—'} {prox}"
        f"</div>", unsafe_allow_html=True)


def struct_panel(m: dict):
    st_ = m['struct']
    pdh = f"PDH:{st_['pdh']:.0f}{'🚀' if st_['above_pdh'] else ''}" if st_['has_pdh'] else "PDH:—"
    pdl = f"PDL:{st_['pdl']:.0f}{'💀' if st_['below_pdl'] else ''}" if st_['has_pdh'] else "PDL:—"
    orh = f"ORH:{st_['orh']:.0f}{'🔝' if st_['above_orh'] else ''}" if st_['has_orh'] else "ORH:—"
    orl = f"ORL:{st_['orl']:.0f}{'🔻' if st_['below_orl'] else ''}" if st_['has_orh'] else "ORL:—"
    mp_dist = int(m['spot'] - m['max_pain'])
    mp_dir  = "↑" if mp_dist > 0 else "↓"
    mig     = m['migration_direction'].split('(')[0].strip() if m.get('migration_direction', 'NONE') != 'NONE' else "—"
    st.markdown(
        f"<div class='struct-box'>"
        f"<b>{pdh}</b> | <b>{pdl}</b> | <b>{orh}</b> | <b>{orl}</b> &nbsp;|&nbsp; "
        f"<b>MaxPain:</b>{m['max_pain']} ({abs(mp_dist):.0f}pts{mp_dir}) &nbsp;|&nbsp; "
        f"<b>Pin:</b>{m['conc_pct']:.0f}%@{m['dominant_strike']} &nbsp;|&nbsp; "
        f"<b>Migration:</b>{mig}"
        f"</div>", unsafe_allow_html=True)


def narrative_panel(m: dict):
    st.markdown(
        f"<div class='narrative-box'>"
        f"<b>CONTROL:</b> {m['dominant_actor']} &nbsp;|&nbsp; "
        f"<b>PHASE:</b> {m['phase']} &nbsp;|&nbsp; "
        f"<b>PAIN:</b> {m['pain']} &nbsp;|&nbsp; "
        f"<b>REGIME:</b> {m['regime_label']}"
        f"</div>", unsafe_allow_html=True)


def alerts_section(m: dict):
    # ── Hero-Zero Ignition: absolute highest priority ─────────────────────
    if m.get('hz_ignition'):
        st.markdown(
            f"<div style='background:rgba(255,0,0,.3);border:3px solid #FF0000;"
            f"border-radius:8px;padding:12px;margin-bottom:10px;font-weight:bold;"
            f"font-size:18px;color:#FF0000;animation:blink 0.8s linear infinite;"
            f"text-align:center;'>"
            f"💥 HERO-ZERO IGNITION — {m.get('hz_direction','?')} "
            f"| Score: {m.get('hz_score',0):.0f} "
            f"| {m.get('hz_theta_label','')}"
            f"</div>", unsafe_allow_html=True)
    # ── Cascade ───────────────────────────────────────────────────────────
    if m.get('is_cascade'):
        st.markdown(
            f"<div style='background:rgba(255,0,0,.15);border:2px solid #FF0000;"
            f"border-radius:6px;padding:10px;margin-bottom:8px;font-weight:bold;"
            f"font-size:16px;color:#FF0000;animation:blink 1s linear infinite;'>"
            f"{m['cascade_alert']}</div>", unsafe_allow_html=True)
    elif m.get('cascade_alert'):
        st.markdown(f"<div class='exit-alert'>{m['cascade_alert']}</div>", unsafe_allow_html=True)
    # ── Skew dynamics alert ───────────────────────────────────────────────
    if m.get('skew_alert'):
        st.markdown(
            f"<div style='background:rgba(255,215,0,.1);border:1px solid #FFD700;"
            f"border-radius:5px;padding:8px;margin-bottom:6px;color:#FFD700;"
            f"font-weight:bold;font-family:monospace;'>{m['skew_alert']}</div>",
            unsafe_allow_html=True)
    # ── Vacuum ────────────────────────────────────────────────────────────
    if m.get('vacuum_alert'):
        st.markdown(
            f"<div style='background:rgba(255,69,0,.1);border:1px solid #FF4500;"
            f"border-radius:5px;padding:8px;margin-bottom:6px;color:#FF4500;"
            f"font-weight:bold;font-family:monospace;'>{m['vacuum_alert']}</div>",
            unsafe_allow_html=True)
    if m.get('exit_warn'):
        st.markdown(f"<div class='exit-alert'>{m['exit_warn']}</div>", unsafe_allow_html=True)
    if m.get('gamma_flip_msg'):
        st.markdown(f"<div class='gamma-flip'>{m['gamma_flip_msg']}</div>", unsafe_allow_html=True)
    if m.get('trap_alert'):
        css = 'trap-confirmed' if m.get('trap_confirmed') else 'trap-alert'
        st.markdown(f"<div class='{css}'>{m['trap_alert']}</div>", unsafe_allow_html=True)
    if m.get('vol_shock'):
        st.markdown(f"<div class='vol-shock'>{m['vol_shock']}</div>", unsafe_allow_html=True)
    if m.get('compression_alert'):
        st.markdown(f"<div class='compression-alert'>{m['compression_alert']}</div>", unsafe_allow_html=True)
    if m.get('trapped_ce'):
        st.markdown("<div class='trapped-sellers'>🪤 CALL SELLERS TRAPPED — coiled for upside</div>",
                    unsafe_allow_html=True)
    if m.get('trapped_pe'):
        st.markdown("<div class='trapped-sellers'>🪤 PUT SELLERS TRAPPED — coiled for downside</div>",
                    unsafe_allow_html=True)
    if m.get('migration_direction', 'NONE') not in ('NONE', None) and m.get('migration_strike'):
        st.markdown(
            f"<div class='migration-alert'>🚚 OI MIGRATION → {m['migration_strike']} "
            f"({m['migration_direction'].split('(')[0].strip()})</div>",
            unsafe_allow_html=True)
    if m.get('liq_risk'):
        st.caption(f"⚠️ Low Liquidity (tick {m.get('tick_rate', 0):.1f}/min)")
    st.markdown(f"<div class='invalidation-box'><b>{m['invalidation']}</b></div>",
                unsafe_allow_html=True)
    if m.get('lookback_warning'):
        st.markdown(f"<div class='lookback-warn'>{m['lookback_warning']}</div>",
                    unsafe_allow_html=True)


def hud_metrics(m: dict):
    # ── Row 1: Price context (4 cols) ────────────────────────────────────
    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    with r1c1:
        st.metric("Spot", f"{m['spot']:.0f}" + (" 🔴EXP" if m['is_expiry'] else ""))
    with r1c2:
        arr = "↑" if m['vwap_above'] else "↓"
        st.metric("VWAP", f"{m['vwap']:.0f}",
                  f"{arr} {m['vwap_dist']:.0f}pts",
                  delta_color="normal" if m['vwap_above'] else "inverse")
    with r1c3:
        hc = "normal" if m['health_score'] >= 80 else "inverse"
        st.metric("FEED", f"{m['health_score']:.0f}%",
                  m['regime_label'][:14], delta_color=hc)
    with r1c4:
        st.metric("GEX", f"{m['net_gex']:+.2f}M",
                  m['gex_mode'].split('(')[0].strip()[:14], delta_color="off")

    # ── Row 2: Market character (4 cols) ──────────────────────────────────
    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    with r2c1:
        phase = m.get('market_phase', 'BALANCED')
        phase_conf = m.get('market_phase_conf', 0.0)
        ptrade = m.get('phase_trade_ok', True)
        pc = "normal" if ptrade else "inverse"
        st.metric("PHASE", phase.replace('_', ' '), f"{phase_conf*100:.0f}% conf", delta_color=pc)
    with r2c2:
        p_up = m.get('p_up', 0.5)
        p_dn = m.get('p_dn', 0.5)
        edge = m.get('dir_edge', 0.0)
        dirn = m.get('dir_direction', 'NEUTRAL')
        dir_arrow = '↑' if dirn == 'UP' else ('↓' if dirn == 'DOWN' else '—')
        dc = "normal" if dirn == 'UP' else ("inverse" if dirn == 'DOWN' else "off")
        st.metric("DIR PROB", f"P↑{p_up*100:.0f}%  P↓{p_dn*100:.0f}%",
                  f"{dir_arrow} edge {edge*100:.0f}%", delta_color=dc)
    with r2c3:
        st.metric("SIGNAL", f"{m['signal_strength']}/100",
                  f"Net OI: {m['net_w']/1000:,.0f}k")
    with r2c4:
        qc = "normal" if m['quality'] > 60 else "inverse"
        st.metric("QUALITY", f"{m['quality']}/100",
                  f"S{m['scores']['s']} F{m['scores']['f']} V{m['scores']['v']}",
                  delta_color=qc)

    # ── Row 3: Flow & scoring (3 cols) ────────────────────────────────────
    r3c1, r3c2, r3c3 = st.columns(3)
    with r3c1:
        mp = m.get('move_prob', 0.0)
        mp_conf = m.get('move_confidence', '—')
        mc = "normal" if mp >= 60 else ("inverse" if mp < 35 else "off")
        st.metric("MOVE PROB", f"{mp:.0f}%", mp_conf[:18], delta_color=mc)
    with r3c2:
        pressure = float(m.get('pressure_score', 0.0))
        phase_p  = m.get('buildup_phase', 'DORMANT')
        pm       = float(m.get('persist_mult', 1.0))
        prc      = "normal" if pressure > 50 else "off"
        st.metric("PRESSURE", f"{pressure:.0f}/100", f"×{pm:.2f}  {phase_p}", delta_color=prc)
    with r3c3:
        hz = m.get('hz_score', 0.0)
        hz_conf = m.get('hz_confidence', '—')
        hzc = "normal" if hz >= 70 else ("inverse" if hz < 30 else "off")
        st.metric("HERO-ZERO", f"{hz:.0f}/100", hz_conf[:18], delta_color=hzc)


def futures_panel(m: dict):
    if not m.get('futures_available'):
        st.caption("⚪ Futures data not available — add futures_candles table to engine")
        return
    surge = " ⚡SURGE" if m.get('futures_surge') else ""
    st.markdown(
        f"<div class='futures-box'>"
        f"<b>FUTURES FLOW:</b> {m['futures_state']}{surge} &nbsp;|&nbsp; "
        f"<b>Δ flow:</b> {m['futures_delta_M']:+.3f}M &nbsp;|&nbsp; "
        f"<b>Signal:</b> {m['futures_signal']:+.3f}"
        f"</div>", unsafe_allow_html=True)


def probability_panel(m: dict):
    win_col = "#00FF00" if m['prob_win_pct'] > 52 else ("#FF4B4B" if m['prob_win_pct'] < 45 else "#FFA500")
    overp   = " ⚠️ OVERPRICED" if m.get('straddle_overpriced') else " ✅ FAIR"
    edge    = m.get('iv_edge', {})
    edge_lbl = edge.get('edge', 'N/A')
    ratio   = edge.get('ratio', 0)
    st.markdown(
        f"<div class='prob-box'>"
        f"<b>P(win):</b> <span style='color:{win_col};font-size:18px;font-weight:bold'>"
        f"{m['prob_win_pct']}%</span> &nbsp;|&nbsp; "
        f"<b>ExpMove:</b> ±{m['exp_move']:.0f} &nbsp;|&nbsp; "
        f"<b>Straddle:</b> {m['straddle']:.0f}{overp} &nbsp;|&nbsp; "
        f"<b>IV/RV:</b> {ratio:.2f} ({edge_lbl})"
        f"</div>", unsafe_allow_html=True)


def dealer_flow_panel(m: dict):
    """Updated with OI skew note."""
    prox = "(⚠️ NEAR)" if m['magnet_dist'] < 100 else f"({m['magnet_dist']:.0f}pts)"
    ce_k  = m['dealer_ce_oi'] / 1000
    pe_k  = m['dealer_pe_oi'] / 1000
    st.markdown(
        f"<div class='dealer-box'>"
        f"<b>Dealer δ (skew-wt):</b> {m['dealer_delta_M']:+.2f}M → {m['dealer_pressure']} &nbsp;|&nbsp; "
        f"<b>OI Skew:</b> {m['dealer_skew_note']} "
        f"(CE:{ce_k:.0f}k / PE:{pe_k:.0f}k) &nbsp;|&nbsp; "
        f"<b>Flow:</b> {m['flow_state']} ({m['avg_flow_ratio']:.1f}× top-10%>{m['dyn_flow_threshold']:.1f}) &nbsp;|&nbsp; "
        f"<b>Magnet:</b> {m['magnet_strike'] or '—'} {prox}"
        f"</div>", unsafe_allow_html=True)


def gex_heatmap(m: dict):
    """Upgrade 3 — Gamma exposure bar chart by strike."""
    gbs = m.get('gex_by_strike', {})
    spot = m['spot']
    if not gbs:
        st.caption("No GEX data for heatmap")
        return
    rows = []
    for strike, d in sorted(gbs.items()):
        net = d.get('CE', 0.0) - d.get('PE', 0.0)
        rows.append({'strike': strike, 'GEX_CE': d.get('CE', 0.0),
                     'GEX_PE': -d.get('PE', 0.0), 'Net_GEX': net})
    df = pd.DataFrame(rows)
    # Filter to ±600 around spot
    df = df[abs(df['strike'] - spot) <= 600].copy()
    if df.empty:
        st.caption("No GEX in ±600 range")
        return
    try:
        import plotly.graph_objects as go
        colors = ['#00CC44' if v >= 0 else '#FF3333' for v in df['Net_GEX']]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df['strike'], y=df['Net_GEX'],
            marker_color=colors,
            name='Net GEX',
            hovertemplate='Strike: %{x}<br>GEX: %{y:.2f}M<extra></extra>'
        ))
        # Annotate key levels
        for label, val, col in [
            ('ATM', m['atm_strike'], '#FFFFFF'),
            ('GEX Wall', m['gex_wall'], '#FFD700'),
            ('Flip', m.get('gex_flip', None), '#00FFFF'),
            ('MaxPain', m['max_pain'], '#FF8C00'),
        ]:
            if val and abs(val - spot) <= 600:
                fig.add_vline(x=val, line_dash='dash', line_color=col,
                              annotation_text=label, annotation_position='top')
        fig.update_layout(
            title='Gamma Exposure by Strike (±600)',
            paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
            font=dict(color='#CCCCCC', size=11),
            height=320, margin=dict(l=40, r=20, t=40, b=30),
            xaxis=dict(title='Strike', gridcolor='#333'),
            yaxis=dict(title='Net GEX (M)', gridcolor='#333'),
            showlegend=False,
        )
        st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
    except ImportError:
        # Fallback: simple dataframe
        st.dataframe(df.set_index('strike')[['GEX_CE', 'GEX_PE', 'Net_GEX']]
                     .style.background_gradient(subset=['Net_GEX'], cmap='RdYlGn',
                                                vmin=-df['Net_GEX'].abs().max(),
                                                vmax=df['Net_GEX'].abs().max()),
                     height=300)


# ── Upgrade 3: Market Control Dashboard ──────────────────────────────────────

def market_control_panel(m: dict):
    """
    Unified BUYERS vs SELLERS dominance panel.
    Combines dealer delta + gamma + futures + OI flow into one view.
    """
    score = m.get('mctrl_score', 5.0)
    bar   = m.get('mctrl_bar', '░' * 10)
    quad  = m.get('mctrl_quadrant', '⚖️ MIXED')
    summ  = m.get('mctrl_summary', '—')
    comps = m.get('mctrl_components', {})

    # Colour based on score
    if   score >= 7.0: border_col, score_col = '#00FF00', '#00FF00'
    elif score >= 6.0: border_col, score_col = '#7FFF00', '#7FFF00'
    elif score >= 4.0: border_col, score_col = '#FFA500', '#FFA500'
    elif score >= 3.0: border_col, score_col = '#FF6347', '#FF6347'
    else:              border_col, score_col = '#FF0000', '#FF0000'

    comp_lines = ''.join(
        f"<span style='color:#888;font-size:12px;'>{k.upper():10s}</span> "
        f"<span style='color:{'#7FFF00' if v['score']>0 else ('#FF6347' if v['score']<0 else '#888')}'>"
        f"{v['label']}</span><br>"
        for k, v in comps.items()
    )

    st.markdown(f"""
    <div style='background:#1a1a1a;border:2px solid {border_col};border-radius:8px;
                padding:14px;margin-bottom:10px;font-family:monospace;'>
      <div style='display:flex;align-items:center;gap:16px;margin-bottom:10px;'>
        <span style='font-size:22px;font-weight:bold;color:{score_col};'>{score:.1f}/10</span>
        <span style='font-size:15px;color:#CCC;letter-spacing:2px;'>[{bar}]</span>
        <span style='font-size:16px;font-weight:bold;color:{score_col};'>{quad}</span>
      </div>
      <div style='font-size:13px;color:#DDD;margin-bottom:8px;'>
        <b>SUMMARY:</b> {summ}
      </div>
      <div style='line-height:1.7;'>{comp_lines}</div>
    </div>
    """, unsafe_allow_html=True)


# ── Upgrade 1: Spot Sensitivity Map ──────────────────────────────────────────

def sensitivity_panel(m: dict):
    """
    dGEX/dSpot chart — shows where gamma explodes if spot moves.
    Explosion zones = strikes professionals watch for acceleration/reversal.
    """
    import pandas as pd
    sens_df  = m.get('sens_df', pd.DataFrame())
    expl     = m.get('sens_explosion_zones', [])
    max_s    = m.get('sens_max_strike', None)
    chain_t  = m.get('sens_chain_total', 0.0)
    spot     = m.get('spot', 0)

    if sens_df is None or (hasattr(sens_df, 'empty') and sens_df.empty):
        st.caption("Sensitivity data not available")
        return

    st.markdown(
        f"<div style='background:#1a1e2a;border-left:5px solid #9B59B6;padding:10px;"
        f"border-radius:5px;font-family:monospace;font-size:13px;color:#DDD;margin-bottom:8px;'>"
        f"<b>dGEX/dSpot (Gamma Sensitivity)</b> &nbsp;|&nbsp; "
        f"Chain total: <b>{chain_t:+.3f}M/pt</b> &nbsp;|&nbsp; "
        f"Max sensitivity: <b>{max_s}</b> &nbsp;|&nbsp; "
        f"Explosion zones: <b>{', '.join(str(z) for z in expl[:6]) or '—'}</b>"
        f"</div>", unsafe_allow_html=True)

    try:
        import plotly.graph_objects as go
        df = sens_df[abs(sens_df['strike'] - spot) <= 500].copy() if not sens_df.empty else sens_df
        colors = ['#00CC44' if v >= 0 else '#FF3333' for v in df['dGEX_dS']]
        fig = go.Figure(go.Bar(
            x=df['strike'], y=df['dGEX_dS'],
            marker_color=colors,
            hovertemplate='Strike: %{x}<br>dGEX/dSpot: %{y:.4f}M/pt<extra></extra>'
        ))
        for ez in expl:
            if abs(ez - spot) <= 500:
                fig.add_vline(x=ez, line_dash='dot', line_color='#FFD700',
                              line_width=1, opacity=0.7)
        if max_s and abs(max_s - spot) <= 500:
            fig.add_vline(x=max_s, line_dash='dash', line_color='#FF00FF',
                          annotation_text='MAX SENS', annotation_position='top')
        fig.update_layout(
            title='dGEX/dSpot — Gamma Explosion Map',
            paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
            font=dict(color='#CCC', size=11),
            height=280, margin=dict(l=40, r=20, t=40, b=30),
            xaxis=dict(title='Strike', gridcolor='#333'),
            yaxis=dict(title='dGEX/dSpot (M/pt)', gridcolor='#333'),
            showlegend=False,
        )
        st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
    except ImportError:
        st.dataframe(sens_df.set_index('strike'), height=250)


# ── Upgrade 2: GEX Heatmap History ───────────────────────────────────────────

def gex_history_panel():
    """
    Shows GEX by strike over the last 30 minutes.
    Reveals gamma wall migration intraday.
    Reads directly from st.session_state.gex_history.
    """
    import streamlit as st
    import pandas as pd
    hist = st.session_state.get('gex_history', [])
    if len(hist) < 3:
        st.caption("Collecting GEX history… (need ≥3 snapshots, ~90s)")
        return
    try:
        import plotly.graph_objects as go
        from datetime import datetime

        # Build matrix: rows = snapshots (time), cols = strikes
        rows = []
        for snap in hist:
            gbs = snap.get('gex_by_strike', {})
            for strike, d in gbs.items():
                net = d.get('CE', 0.0) - d.get('PE', 0.0)
                rows.append({'ts': snap['ts'], 'strike': int(strike), 'net_gex': net,
                             'spot': snap['spot'], 'wall': snap['gex_wall']})
        if not rows:
            st.caption("No GEX history data yet")
            return

        df = pd.DataFrame(rows)
        pivot = df.pivot_table(index='ts', columns='strike', values='net_gex', fill_value=0)
        spot_series = df.groupby('ts')['spot'].first()
        wall_series = df.groupby('ts')['wall'].first()

        time_labels = [datetime.fromtimestamp(t).strftime('%H:%M') for t in pivot.index]
        strikes     = [str(s) for s in pivot.columns]

        # Clip extremes so one outlier doesn't crush the colour scale
        z = pivot.values.clip(-5, 5)

        fig = go.Figure(go.Heatmap(
            z=z.T,
            x=time_labels,
            y=strikes,
            colorscale='RdYlGn',
            zmid=0,
            colorbar=dict(title='Net GEX', tickfont=dict(color='#CCC')),
            hovertemplate='Time: %{x}<br>Strike: %{y}<br>GEX: %{z:.2f}M<extra></extra>'
        ))

        # Overlay spot price path
        fig.add_trace(go.Scatter(
            x=time_labels,
            y=[str(int(round(s / 50) * 50)) for s in spot_series.values],
            mode='lines',
            line=dict(color='white', width=2, dash='dot'),
            name='Spot',
        ))
        # Overlay gamma wall path
        fig.add_trace(go.Scatter(
            x=time_labels,
            y=[str(int(w)) for w in wall_series.values],
            mode='lines',
            line=dict(color='#FFD700', width=1, dash='dash'),
            name='GEX Wall',
        ))

        fig.update_layout(
            title=f'GEX Heatmap History ({len(pivot)}×30s snapshots = last ~{len(pivot)//2} min)',
            paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
            font=dict(color='#CCC', size=11),
            height=420, margin=dict(l=60, r=20, t=50, b=40),
            xaxis=dict(title='Time', gridcolor='#333'),
            yaxis=dict(title='Strike', gridcolor='#333'),
            legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color='#CCC')),
        )
        st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
        st.caption(
            "Green = net long gamma (pinning force). Red = net short gamma (amplifying). "
            "White dotted = spot path. Gold dashed = GEX wall migration.")
    except ImportError:
        st.caption("Install plotly for GEX history heatmap: pip install plotly")


# ── Improvement 5: Cascade + Hedge Flow Panel ─────────────────────────────────

def cascade_panel(m: dict):
    """
    Shows dealer gamma hedging cascade risk.
    Red pulsing border when cascade is confirmed.
    Strength bar: 1=setup forming, 2=conditions met, 3=full cascade.
    """
    is_casc  = m.get('is_cascade', False)
    ctype    = m.get('cascade_type', None)
    strength = m.get('cascade_strength', 0)
    alert    = m.get('cascade_alert', None)
    hf_norm  = m.get('hedge_flow_norm', 0.0)
    hf_state = m.get('hedge_flow_state', '—')
    imb_r    = m.get('imbalance_ratio', 0.0)
    imb_s    = m.get('imbalance_state', '—')

    # Cascade strength bar
    bar_filled = '█' * strength + '░' * (3 - strength)
    if is_casc:
        border = '#FF0000' if ctype == 'BEAR' else '#FF6600'
        label  = f"🚨 {'BULL' if ctype == 'BULL' else 'BEAR'} CASCADE"
        css    = "cascade-confirmed"
    elif strength >= 2:
        border, label, css = '#FFA500', '⚠️ CASCADE SETUP', "cascade-setup"
    else:
        border, label, css = '#333333', '⚖️ No Cascade', "cascade-none"

    hf_col = '#00FF00' if hf_norm > 0.5 else ('#FF4B4B' if hf_norm < -0.5 else '#888')
    imb_col = '#FF4B4B' if imb_r > 1.5 else ('#FFA500' if imb_r > 1.0 else '#00FF00')

    st.markdown(f"""
    <div style='background:#1a0a0a;border:2px solid {border};border-radius:8px;
                padding:12px;margin-bottom:8px;font-family:monospace;font-size:14px;'>
      <div style='display:flex;justify-content:space-between;align-items:center;'>
        <span style='color:{border};font-weight:bold;font-size:16px;'>{label}</span>
        <span style='color:#CCC;letter-spacing:2px;'>[{bar_filled}] {strength}/3</span>
      </div>
      {'<div style="color:#FF4B4B;font-size:13px;margin-top:6px;">'+alert+'</div>' if alert else ''}
      <div style='margin-top:8px;color:#BBB;'>
        <b>Hedge Flow:</b> <span style='color:{hf_col};'>{hf_norm:+.2f} — {hf_state}</span>
        &nbsp;|&nbsp;
        <b>δ Imbalance:</b> <span style='color:{imb_col};'>{imb_r:.2f}× — {imb_s[:30]}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ── Improvement 4: Liquidity Vacuum Panel ────────────────────────────────────

def vacuum_panel(m: dict):
    """
    Shows OI gap / liquidity vacuum status.
    Key insight: fast moves happen in zones with no hedgers.
    """
    in_vac   = m.get('in_vacuum', False)
    severity = m.get('vacuum_severity', 0.0)
    zones    = m.get('vacuum_zones', [])
    above    = m.get('next_wall_above', None)
    below    = m.get('next_wall_below', None)
    alert    = m.get('vacuum_alert', None)

    if in_vac:
        bg, border, icon = 'rgba(255,69,0,.15)', '#FF4500', '⚡'
        label = 'IN VACUUM — fast move risk'
    elif severity > 0.4:
        bg, border, icon = 'rgba(255,165,0,.1)', '#FFA500', '⚠️'
        label = f'SPARSE OI ({severity*100:.0f}% gaps)'
    else:
        bg, border, icon = 'rgba(0,255,0,.05)', '#2a5c2a', '✅'
        label = f'OI NORMAL ({severity*100:.0f}% gaps)'

    zones_str = ', '.join(f"{lo}–{hi}" for lo, hi in zones[:3]) if zones else '—'
    st.markdown(f"""
    <div style='background:{bg};border:2px solid {border};border-radius:6px;
                padding:10px;margin-bottom:8px;font-family:monospace;font-size:13px;'>
      <b>{icon} LIQUIDITY:</b> <span style='color:{border};font-weight:bold;'>{label}</span>
      &nbsp;|&nbsp; <b>Zones:</b> {zones_str}
      &nbsp;|&nbsp; <b>Wall ↑:</b> {above or '—'}
      &nbsp;|&nbsp; <b>Wall ↓:</b> {below or '—'}
      {'<br><span style="color:#FF4500;">'+alert+'</span>' if alert else ''}
    </div>
    """, unsafe_allow_html=True)


# ── Institutional Intelligence Panel (combined view) ─────────────────────────

def institutional_panel(m: dict):
    """
    Compact single-row panel showing all 5 institutional signals.
    Designed for the live tab header — one glance gives full picture.
    """
    # 1. Price-OI flow
    fb      = m.get('dealer_flow_breakdown', {})
    dom_oi  = max(fb, key=fb.get) if fb else '—'
    oi_col  = {'LONG_BUILD': '#00FF00', 'SHORT_BUILD': '#FF4B4B',
               'SHORT_COVER': '#7FFF00', 'LONG_UNWIND': '#FFA500'}.get(dom_oi, '#888')
    # 2. Gamma flip
    flip    = m.get('gex_flip_exact', None)
    flip_str = f"{flip}" if flip else '—'
    flip_dist = abs(m['spot'] - flip) if flip else 999
    flip_col = '#FF0000' if flip_dist < 50 else ('#FFA500' if flip_dist < 100 else '#888')
    # 3. Delta flow
    df      = m.get('delta_flow_norm', 0.0)
    df_col  = '#00FF00' if df > 0.2 else ('#FF4B4B' if df < -0.2 else '#888')
    # 4. Vacuum
    vac     = m.get('in_vacuum', False)
    sev     = m.get('vacuum_severity', 0.0)
    vac_col = '#FF4500' if vac else ('#FFA500' if sev > 0.4 else '#00FF00')
    vac_txt = '⚡VAC' if vac else (f'⚠️{sev*100:.0f}%' if sev > 0.4 else '✅OK')
    # 5. Cascade
    casc    = m.get('is_cascade', False)
    cstr    = m.get('cascade_strength', 0)
    casc_col = '#FF0000' if casc else ('#FFA500' if cstr >= 2 else '#888')
    casc_txt = f"🚨CASCADE" if casc else (f"⚠️{cstr}/3" if cstr >= 1 else '—')

    st.markdown(f"""
    <div style='background:#111;border:1px solid #444;border-radius:6px;padding:10px 14px;
                margin-bottom:10px;font-family:monospace;font-size:13px;
                display:flex;gap:20px;flex-wrap:wrap;'>
      <span><b style='color:#888;'>OI-FLOW:</b>
        <b style='color:{oi_col};'>{dom_oi.replace("_"," ")}</b></span>
      <span><b style='color:#888;'>FLIP:</b>
        <span style='color:{flip_col};'>{flip_str}
        {'⚠️' if flip_dist < 75 else ''}</span></span>
      <span><b style='color:#888;'>δ-FLOW:</b>
        <span style='color:{df_col};'>{df:+.2f}</span></span>
      <span><b style='color:#888;'>VACUUM:</b>
        <span style='color:{vac_col};'>{vac_txt}</span></span>
      <span><b style='color:#888;'>CASCADE:</b>
        <span style='color:{casc_col};'>{casc_txt}</span></span>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MOVE PROBABILITY ENGINE — primary panel
# ═══════════════════════════════════════════════════════════════════════════════

def move_probability_panel(m: dict):
    """
    The primary decision panel. Single glance → actionable output.
    Replaces the fragmented score display for the top of screen.
    """
    prob   = m.get('move_prob', 0.0)
    dirn   = m.get('move_direction', 'NEUTRAL')
    target = m.get('move_target', None)
    conf   = m.get('move_confidence', 'DORMANT')
    interp = m.get('move_interp', '—')
    drvs   = m.get('move_drivers', [])
    rmult  = m.get('move_regime_mult', 1.0)
    cnum   = m.get('move_confirm_n', 0)

    # Colour scale
    if   prob >= 85: bg, border, prob_col = 'rgba(255,0,0,.18)',    '#FF0000', '#FF0000'
    elif prob >= 70: bg, border, prob_col = 'rgba(255,100,0,.15)',  '#FF6400', '#FF6400'
    elif prob >= 50: bg, border, prob_col = 'rgba(255,200,0,.12)',  '#FFC800', '#FFC800'
    elif prob >= 30: bg, border, prob_col = 'rgba(0,180,255,.10)',  '#00B4FF', '#00B4FF'
    else:            bg, border, prob_col = 'rgba(100,100,100,.08)','#666666', '#888888'

    dir_icon = '🔼' if dirn == 'UP' else ('🔽' if dirn == 'DOWN' else '↔️')
    target_str = f"→ {target}" if target else ''
    drv_html = ''.join(f"<div style='color:#BBB;font-size:12px;'>{d}</div>" for d in drvs[:5])
    mult_str = f"×{rmult:.1f} ({['SUPP','NEUT','AMP','PANIC'][min(int(rmult), 3)][:4]})" if rmult != 1.0 else ''

    st.markdown(f"""
    <div style='background:{bg};border:2px solid {border};border-radius:10px;
                padding:16px 20px;margin-bottom:12px;font-family:monospace;'>
      <div style='display:flex;align-items:baseline;gap:24px;flex-wrap:wrap;margin-bottom:10px;'>
        <span style='font-size:36px;font-weight:bold;color:{prob_col};'>{prob:.0f}%</span>
        <span style='font-size:18px;color:#EEE;'>{dir_icon} {dirn} {target_str}</span>
        <span style='font-size:13px;color:{border};font-weight:bold;'>{conf}</span>
        <span style='font-size:12px;color:#666;'>{mult_str}</span>
      </div>
      <div style='font-size:14px;color:#DDD;margin-bottom:8px;'>{interp}</div>
      <div style='border-top:1px solid {border}30;padding-top:8px;'>
        {''.join(drv_html) if drvs else '<span style="color:#666;font-size:12px;">No dominant drivers</span>'}
      </div>
      {'<div style="font-size:11px;color:#555;margin-top:6px;">'+str(cnum)+' confirming signals</div>' if cnum > 0 else ''}
    </div>
    """, unsafe_allow_html=True)


def dealer_regime_panel(m: dict):
    """Nonlinear dealer regime — replaces linear +1/-1 control score."""
    regime  = m.get('dealer_regime', 'NEUTRAL')
    label   = m.get('dealer_regime_label', '—')
    mult    = m.get('dealer_regime_mult', 1.0)
    panic   = m.get('dealer_panic_score', 0)
    conds   = m.get('dealer_conditions', [])

    colour_map = {
        'PANIC_HEDGING':  '#FF0000',
        'AMPLIFICATION':  '#FF8C00',
        'NEUTRAL':        '#888888',
        'SUPPRESSION':    '#00BFFF',
    }
    col = colour_map.get(regime, '#888888')
    bar = '█' * panic + '░' * (4 - panic)

    # Hedge simulator
    up50 = m.get('hedge_up50', {})
    dn50 = m.get('hedge_dn50', {})
    dom  = m.get('hedge_dom_dir', '—')
    asym = m.get('hedge_asymmetry', 0.0)

    up_dir = '⬆️ BUY' if up50.get('net_direction') == 'BUY' else '⬇️ SELL'
    dn_dir = '⬆️ BUY' if dn50.get('net_direction') == 'BUY' else '⬇️ SELL'
    up_amp = '🔥 AMP' if up50.get('amplifying') else '🧊 DAM'
    dn_amp = '🔥 AMP' if dn50.get('amplifying') else '🧊 DAM'

    cond_str = ' · '.join(c.replace('_', ' ') for c in conds) or '—'
    st.markdown(f"""
    <div style='background:#1a1a1a;border:2px solid {col};border-radius:8px;
                padding:12px;margin-bottom:8px;font-family:monospace;font-size:13px;'>
      <div style='display:flex;justify-content:space-between;margin-bottom:6px;'>
        <span style='color:{col};font-weight:bold;font-size:15px;'>{label}</span>
        <span style='color:#CCC;letter-spacing:2px;'>[{bar}] panic:{panic}/4 · mult:{mult}×</span>
      </div>
      <div style='color:#888;font-size:12px;margin-bottom:8px;'>Active: {cond_str}</div>
      <div style='border-top:1px solid #333;padding-top:8px;font-size:12px;color:#CCC;'>
        <b>Hedge Sim:</b> &nbsp;
        spot+50 → dealers {up_dir} {up50.get('hedge_futures_M',0):+.1f}M {up_amp} &nbsp;|&nbsp;
        spot-50 → dealers {dn_dir} {dn50.get('hedge_futures_M',0):+.1f}M {dn_amp} &nbsp;|&nbsp;
        Dom cascade: <b>{dom}</b> (asym {asym:+.1f}M)
      </div>
    </div>
    """, unsafe_allow_html=True)


def liquidity_map_panel(m: dict):
    """Full OI density map — acceleration zones and walls."""
    import pandas as pd
    liq_df  = m.get('liq_map_df', pd.DataFrame())
    profile = m.get('liq_profile', '—')
    acc_z   = m.get('liq_acc_zones', [])
    dec_z   = m.get('liq_dec_zones', [])
    acc_a   = m.get('liq_acc_above', None)
    acc_b   = m.get('liq_acc_below', None)
    spot    = m.get('spot', 0)

    # Profile badge
    prof_col = {'GAPPED': '#FF6347', 'CLUSTERED': '#FFA500', 'UNIFORM': '#00C8FF'}.get(profile, '#888')
    acc_str  = f"ACC↑:{acc_a or '—'}  ACC↓:{acc_b or '—'}"

    st.markdown(
        f"<div style='background:#0f1a1a;border-left:4px solid {prof_col};padding:8px 12px;"
        f"border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:13px;color:#DDD;'>"
        f"<b>OI DENSITY:</b> <span style='color:{prof_col};font-weight:bold;'>{profile}</span>"
        f" &nbsp;|&nbsp; Acc zones: {len(acc_z)}  Wall zones: {len(dec_z)}"
        f" &nbsp;|&nbsp; {acc_str}</div>", unsafe_allow_html=True)

    if liq_df is None or (hasattr(liq_df, 'empty') and liq_df.empty):
        st.caption("No liquidity map data")
        return

    try:
        import plotly.graph_objects as go
        near = liq_df[abs(liq_df['strike'] - spot) <= 500] if not liq_df.empty else liq_df

        colors = []
        for _, row in near.iterrows():
            z = row.get('zone', 'NORMAL')
            if   z == 'ACC':    colors.append('#FF6347')   # red = fast-move zone
            elif z == 'WALL':   colors.append('#00BFFF')   # blue = wall
            else:                colors.append('#555555')   # grey = normal

        fig = go.Figure(go.Bar(
            x=near['strike'], y=near['density'],
            marker_color=colors,
            hovertemplate='Strike: %{x}<br>Density: %{y:.3f}<br><extra></extra>'
        ))
        fig.add_vline(x=spot, line_dash='dash', line_color='white',
                      annotation_text='SPOT', annotation_position='top')
        if acc_a:
            fig.add_vline(x=acc_a, line_dash='dot', line_color='#FF6347',
                          annotation_text='ACC↑', annotation_position='top right')
        if acc_b:
            fig.add_vline(x=acc_b, line_dash='dot', line_color='#FF6347',
                          annotation_text='ACC↓', annotation_position='top left')
        fig.update_layout(
            title='Liquidity Density Map  (Red=Acc zone  Blue=Wall)',
            paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
            font=dict(color='#CCC', size=11), height=260,
            margin=dict(l=40, r=20, t=40, b=30),
            xaxis=dict(gridcolor='#222'), yaxis=dict(title='OI Density', gridcolor='#222'),
            showlegend=False,
        )
        st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
        st.caption("Red bars = acceleration zones (low OI — fast moves). "
                   "Blue bars = walls (high OI — deceleration). "
                   "Price accelerates through red into blue.")
    except ImportError:
        st.dataframe(near[['strike', 'zone', 'density', 'oi']].set_index('strike'),
                     width='stretch')


def skew_dynamics_panel(m: dict):
    """Skew velocity tracker — predicts moves before price."""
    s_vel   = m.get('skew_velocity', 0.0)
    s_z     = m.get('skew_z', 0.0)
    s_state = m.get('skew_state', '—')
    s_alert = m.get('skew_alert', None)
    s_sig   = m.get('skew_signal', 0.0)
    s_25d   = m.get('skew_25d', 0.0)
    iv_25c  = m.get('iv_25c', 0.0)
    iv_25p  = m.get('iv_25p', 0.0)

    if s_alert:
        st.markdown(
            f"<div style='background:rgba(255,215,0,.12);border:1px solid #FFD700;"
            f"border-radius:5px;padding:8px;margin-bottom:6px;color:#FFD700;"
            f"font-weight:bold;font-family:monospace;'>{s_alert}</div>",
            unsafe_allow_html=True)

    vel_col = '#FF6347' if s_vel > 0.001 else ('#7FFF00' if s_vel < -0.001 else '#888')
    z_col   = '#FF4B4B' if abs(s_z) > 2 else ('#FFA500' if abs(s_z) > 1 else '#888')
    st.markdown(
        f"<div style='background:#1a1e10;border-left:4px solid #FFD700;padding:8px 12px;"
        f"border-radius:5px;margin-bottom:6px;font-family:monospace;font-size:13px;color:#DDD;'>"
        f"<b>SKEW:</b> {s_25d:.3f} &nbsp;|&nbsp; "
        f"25ΔC:{iv_25c:.1%}  25ΔP:{iv_25p:.1%} &nbsp;|&nbsp; "
        f"Vel: <span style='color:{vel_col};'>{s_vel*1000:+.2f}‰/min</span> &nbsp;|&nbsp; "
        f"Z: <span style='color:{z_col};'>{s_z:+.1f}σ</span> &nbsp;|&nbsp; "
        f"{s_state}</div>",
        unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# HERO-ZERO ENGINE PANELS
# ═══════════════════════════════════════════════════════════════════════════════

def hero_zero_panel(m: dict):
    """
    Primary hero-zero panel. Large, unambiguous, single-glance output.
    Designed to be the ONLY thing you look at on expiry day final 90 min.
    """
    hz   = m.get('hz_score', 0.0)
    conf = m.get('hz_confidence', 'DORMANT')
    ign  = m.get('hz_ignition', False)
    dirn = m.get('hz_direction', 'NEUTRAL')
    tgt  = m.get('hz_target', None)
    interp  = m.get('hz_interpretation', '—')
    conds   = m.get('hz_conditions', [])
    theta_l = m.get('hz_theta_label', '—')
    theta_m = m.get('hz_theta_mult', 1.0)
    comps   = m.get('hz_components', {})

    # Colour + animation
    if   conf == 'IGNITION':  bg, border, pulse = 'rgba(255,0,0,.25)',   '#FF0000', True
    elif conf == 'ARMED':     bg, border, pulse = 'rgba(255,100,0,.2)',  '#FF6400', True
    elif conf == 'SETUP':     bg, border, pulse = 'rgba(255,200,0,.12)', '#FFC800', False
    elif conf == 'WATCH':     bg, border, pulse = 'rgba(0,180,255,.1)',  '#00B4FF', False
    else:                     bg, border, pulse = 'rgba(60,60,60,.1)',   '#444444', False

    dir_icon = {'UP': '🔼', 'DOWN': '🔽', 'NEUTRAL': '↔️'}.get(dirn, '↔️')
    anim     = 'animation:blink 1s linear infinite;' if pulse else ''
    tgt_str  = f"→ {tgt}" if tgt else ''

    cond_html = ''.join(
        f"<div style='color:#CCC;font-size:12px;padding:1px 0;'>{c}</div>"
        for c in conds[:7])

    # Component bar (5 factors)
    comp_names  = ['wall_break', 'trap_density', 'hedge_pressure', 'acceleration', 'vacuum']
    comp_labels = ['WALL', 'TRAP', 'PRES', 'ACCEL', 'VAC']
    bars = ''
    for n, l in zip(comp_names, comp_labels):
        v   = comps.get(n, 0.0)
        pct = int(v * 100)
        c   = '#FF4B4B' if v > 0.7 else ('#FFA500' if v > 0.4 else '#444')
        bars += (f"<div style='display:inline-block;margin-right:8px;text-align:center;'>"
                 f"<div style='font-size:10px;color:#888;'>{l}</div>"
                 f"<div style='background:{c};width:28px;height:{max(pct//3,2)}px;margin:auto;'></div>"
                 f"<div style='font-size:10px;color:{c};'>{pct}%</div></div>")

    st.markdown(f"""
    <div style='background:{bg};border:3px solid {border};border-radius:12px;
                padding:18px 22px;margin-bottom:14px;font-family:monospace;{anim}'>
      <div style='display:flex;align-items:center;gap:20px;margin-bottom:12px;flex-wrap:wrap;'>
        <div>
          <div style='font-size:11px;color:#888;letter-spacing:2px;'>HERO-ZERO</div>
          <div style='font-size:48px;font-weight:bold;color:{border};line-height:1;'>{hz:.0f}</div>
          <div style='font-size:12px;color:#888;'>/ 100</div>
        </div>
        <div>
          <div style='font-size:20px;font-weight:bold;color:{border};'>{conf}</div>
          <div style='font-size:16px;color:#EEE;'>{dir_icon} {dirn} {tgt_str}</div>
          <div style='font-size:12px;color:#AAA;margin-top:4px;'>{theta_l}</div>
        </div>
      </div>
      <div style='font-size:14px;color:#DDD;margin-bottom:10px;'>{interp}</div>
      <div style='margin-bottom:10px;'>{bars}</div>
      <div style='border-top:1px solid {border}40;padding-top:8px;'>
        {''.join(cond_html) if conds else
         '<span style="color:#555;font-size:12px;">No active conditions</span>'}
      </div>
    </div>
    """, unsafe_allow_html=True)



def hero_zero_conditions_panel(m: dict):
    """
    The 5-condition counter. Only signal that matters for option buyers.
    0/5 = sit out. 4-5/5 = size up.

    The 5 conditions:
        1. Short Gamma    — dealers forced to amplify moves
        2. Hedge Pressure — dealer forced-hedging rate is high
        3. Liquidity Vacuum — no OI friction ahead
        4. Directional Flow — real buying/selling pressure
        5. Compression Break — volatility coil releasing energy
    """
    n    = m.get('hz_conditions_met', 0)
    lbls = m.get('hz_cond_labels', {})

    colour_map = {0: '#444', 1: '#555', 2: '#666',
                  3: '#FFA500', 4: '#FF6400', 5: '#FF0000'}
    bg_map     = {0: 'rgba(60,60,60,.05)', 1: 'rgba(60,60,60,.07)',
                  2: 'rgba(80,80,80,.10)', 3: 'rgba(255,165,0,.12)',
                  4: 'rgba(255,100,0,.18)', 5: 'rgba(255,0,0,.25)'}
    verdict_map = {
        0: "💤 DORMANT — no conditions active",
        1: "👁️ WATCH — 1 condition",
        2: "👁️ WATCH — 2 conditions",
        3: "⚡ SETUP — 3 conditions active",
        4: "💣 ARMED — 4 conditions. Size up.",
        5: "🔥 IGNITION — all 5 conditions. FULL SIZE.",
    }
    c   = colour_map.get(n, '#444')
    bg  = bg_map.get(n, 'rgba(60,60,60,.05)')
    vrd = verdict_map.get(n, "")

    # Build 5 condition pills
    order = ['short_gamma', 'hedge_pressure', 'vacuum', 'directional', 'compression']
    pills = ''.join(
        f"<div style='margin:3px 0;padding:5px 10px;border-radius:4px;"
        f"background:{'rgba(0,200,100,.15)' if m.get('hz_core_conditions',{}).get(k) else 'rgba(60,60,60,.2)'};"
        f"border-left:3px solid {'#00C864' if m.get('hz_core_conditions',{}).get(k) else '#333'};"
        f"font-size:12px;color:{'#00C864' if m.get('hz_core_conditions',{}).get(k) else '#666'};'>"
        f"{lbls.get(k, k)}</div>"
        for k in order
    )

    st.markdown(
        f"<div style='background:{bg};border:2px solid {c};border-radius:10px;"
        f"padding:14px 18px;margin-bottom:12px;font-family:monospace;'>"
        f"<div style='display:flex;align-items:center;gap:16px;margin-bottom:12px;'>"
        f"<div style='font-size:52px;font-weight:bold;color:{c};line-height:1;'>{n}</div>"
        f"<div><div style='font-size:11px;color:#666;letter-spacing:2px;'>CONDITIONS MET</div>"
        f"<div style='font-size:20px;color:{c};font-weight:bold;'>/ 5</div>"
        f"<div style='font-size:11px;color:#888;margin-top:2px;'>{vrd}</div></div>"
        f"</div>{pills}</div>",
        unsafe_allow_html=True,
    )


def hero_zero_detail_panel(m: dict):
    """Expanded view of each sub-detector for analysis."""
    wall_r  = m.get('hz_wall_r', {})
    trap_r  = m.get('hz_trap_r', {})
    accel_r = m.get('hz_accel_r', {})
    theta_r = m.get('hz_theta_r', {})

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown("**⚡ Gamma Wall**")
        wt = wall_r.get('break_type') or '—'
        wc = '#FF4B4B' if wall_r.get('wall_break') else ('#FFA500' if 'BREAK' in str(wt) else '#888')
        st.markdown(f"<span style='color:{wc};font-size:13px;font-weight:bold;'>{wt}</span>",
                    unsafe_allow_html=True)
        st.caption(f"Dist: {wall_r.get('wall_distance', 0):.0f}pt  "
                   f"Short-γ: {'✓' if wall_r.get('short_gamma') else '✗'}")

    with c2:
        st.markdown("**🪤 OI Trap**")
        td = trap_r.get('trap_density_pct', 0.0)
        tc = '#FF4B4B' if td >= 25 else ('#FFA500' if td >= 15 else '#888')
        st.markdown(f"<span style='color:{tc};font-size:20px;font-weight:bold;'>{td:.0f}%</span>",
                    unsafe_allow_html=True)
        st.caption(f"Level: {trap_r.get('trap_level','—')}  "
                   f"Side: {trap_r.get('dominant_side','—')}")

    with c3:
        st.markdown("**📈 Acceleration**")
        al = accel_r.get('accel_level', 'FLAT')
        ac = '#FF4B4B' if al == 'SURGING' else ('#FFA500' if al == 'BUILDING' else '#888')
        st.markdown(f"<span style='color:{ac};font-size:13px;font-weight:bold;'>{al}</span>",
                    unsafe_allow_html=True)
        st.caption(f"Now:{accel_r.get('velocity_now',0):.1f}  "
                   f"Old:{accel_r.get('velocity_old',0):.1f}  "
                   f"Score:{accel_r.get('accel_score',0):+.2f}")

    with c4:
        st.markdown("**⏰ Theta Clock**")
        bkt = theta_r.get('time_bucket', '—')
        mult = theta_r.get('multiplier', 1.0)
        tc2 = '#FF4B4B' if mult >= 2.2 else ('#FFA500' if mult >= 1.5 else '#888')
        st.markdown(f"<span style='color:{tc2};font-size:20px;font-weight:bold;'>×{mult:.1f}</span>",
                    unsafe_allow_html=True)
        st.caption(f"{bkt}  {theta_r.get('mins_to_close', 0)}min to close")

    # Hedge pressure summary
    hp_level = m.get('hp_level', 'LOW')
    hp_pt    = m.get('hp_pressure_pt', 0.0)
    hp_vp    = m.get('hp_vel_pressure', 0.0)
    hp_col   = {'EXTREME': '#FF0000', 'HIGH': '#FF6400', 'MEDIUM': '#FFC800', 'LOW': '#888'}.get(hp_level, '#888')
    st.markdown(
        f"<div style='background:#111;border-left:4px solid {hp_col};padding:8px 12px;"
        f"margin-top:8px;border-radius:4px;font-family:monospace;font-size:13px;'>"
        f"<b>HEDGE PRESSURE:</b> <span style='color:{hp_col};font-weight:bold;'>{hp_level}</span>"
        f" — {hp_pt:.2f}M/pt &nbsp;|&nbsp; Velocity pressure: {hp_vp:.2f}M/min"
        f"</div>", unsafe_allow_html=True)


def cascade_state_panel(m: dict):
    """Shows cascade FSM state — more informative than just is_cascade bool."""
    fsm    = m.get('cascade_fsm_state', 'IDLE')
    frames = m.get('cascade_frames_in', 0)
    n_cond = m.get('cascade_n_conditions', 0)
    ctype  = m.get('cascade_type', None)
    alert  = m.get('cascade_alert', None)

    state_col = {
        'IDLE':      '#444444',
        'WARMING':   '#00BFFF',
        'BUILDING':  '#FFA500',
        'CONFIRMED': '#FF6400',
        'FIRING':    '#FF0000',
    }.get(fsm, '#666')

    prog = {'IDLE': 0, 'WARMING': 1, 'BUILDING': 2, 'CONFIRMED': 3, 'FIRING': 4}
    filled = '█' * prog.get(fsm, 0) + '░' * (4 - prog.get(fsm, 0))

    ctype_html = (" &nbsp;|&nbsp; <b>" + ctype + "</b>") if ctype else ""
    alert_html = ("<br><span style='color:" + state_col + ";font-size:12px;'>" + alert + "</span>") if alert else ""
    st.markdown(
        f"<div style='background:#1a1a1a;border:2px solid {state_col};border-radius:6px;"
        f"padding:8px 12px;margin-bottom:8px;font-family:monospace;font-size:13px;>"
        f"<b>CASCADE FSM:</b> <span style='color:{state_col};font-weight:bold;'>{fsm}</span>"
        f" <span style='color:#CCC;letter-spacing:2px;'>[{filled}]</span>"
        f" {frames} frames &nbsp;|&nbsp; {n_cond} conditions"
        f"{ctype_html}{alert_html}"
        f"</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# dGamma/dSpot — GAMMA CONVEXITY PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def gamma_convexity_panel(m: dict):
    """
    Displays the dGamma/dSpot (Speed) regime.

    The convexity regime answers the question that hedge_pressure alone cannot:
    is the hedging pressure LINEAR (constant per point) or EXPLOSIVE (growing
    faster than linearly with each point of movement)?

    EXPLOSIVE: Each 1pt move triggers MORE hedging than the previous 1pt.
               This is the feedback loop that creates vertical candles.
    LINEAR:    Hedging demand is proportional to the move. Directional but controlled.
    DAMPENING: Gamma shrinking as price moves. Moves are self-limiting.
    """
    regime   = m.get('gconv_regime', 'DAMPENING')
    speed    = m.get('gconv_speed', 0.0)
    score    = m.get('gconv_score', 0.0)
    dom_dir  = m.get('gconv_dom_dir', 'SYMMETRIC')
    up_c     = m.get('gconv_up', 0.0)
    dn_c     = m.get('gconv_dn', 0.0)
    note     = m.get('gconv_note', '—')
    exp_s    = m.get('gconv_exp_strikes', [])

    colour = {'EXPLOSIVE': '#FF0000', 'LINEAR': '#FFA500', 'DAMPENING': '#00BFFF'}.get(regime, '#888')
    icon   = {'EXPLOSIVE': '🔥', 'LINEAR': '📈', 'DAMPENING': '🧊'}.get(regime, '⚪')
    bar    = '█' * int(score * 10) + '░' * (10 - int(score * 10))

    # Directional split
    dir_col = {'UP': '#00FF00', 'DOWN': '#FF4B4B', 'SYMMETRIC': '#888'}.get(dom_dir, '#888')

    exp_str = ', '.join(str(s) for s in exp_s[:5]) if exp_s else '—'

    st.markdown(f"""
    <div style='background:#0d1117;border:2px solid {colour};border-radius:8px;
                padding:14px 18px;margin-bottom:10px;font-family:monospace;'>
      <div style='display:flex;align-items:baseline;gap:20px;margin-bottom:8px;flex-wrap:wrap;'>
        <span style='font-size:28px;font-weight:bold;color:{colour};'>{icon} {regime}</span>
        <span style='font-size:14px;color:#AAA;'>dγ/dS = {speed*1000:.3f}k/pt²</span>
        <span style='color:{dir_col};font-weight:bold;font-size:14px;'>Dominant: {dom_dir}</span>
      </div>
      <div style='color:#888;font-size:12px;letter-spacing:1px;margin-bottom:8px;'>
        [{bar}] {int(score*100)}%  &nbsp;|&nbsp;
        ↑ {up_c*1000:.3f}k  ↓ {dn_c*1000:.3f}k/pt²
      </div>
      <div style='font-size:13px;color:#DDD;margin-bottom:6px;'>{note}</div>
      {'<div style="font-size:11px;color:#666;">Explosive strikes: ' + exp_str + '</div>' if exp_s else ''}
    </div>
    """, unsafe_allow_html=True)

    try:
        gconv_df_data = m.get('gconv_by_strike', {})
        if gconv_df_data:
            import pandas as _pd
            import plotly.graph_objects as go
            spot = m.get('spot', 0)
            rows = [{'strike': int(s), 'speed_M': float(v)}
                    for s, v in gconv_df_data.items()
                    if abs(int(s) - spot) <= 400]
            if rows:
                df = _pd.DataFrame(rows).sort_values('strike')
                colors = ['#FF4B4B' if v > 0 else '#00BFFF' for v in df['speed_M']]
                fig = go.Figure(go.Bar(
                    x=df['strike'], y=df['speed_M'],
                    marker_color=colors,
                    hovertemplate='Strike:%{x}<br>dγ/dS:%{y:.5f}M/pt²<extra></extra>'
                ))
                fig.add_vline(x=spot, line_dash='dash', line_color='white',
                              annotation_text='SPOT')
                fig.update_layout(
                    title='dGamma/dSpot per Strike  (Red = hedging accelerates UP  Blue = accelerates DOWN)',
                    paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
                    font=dict(color='#CCC', size=11), height=240,
                    margin=dict(l=40, r=20, t=40, b=30),
                    xaxis=dict(gridcolor='#222'),
                    yaxis=dict(title='M/pt²', gridcolor='#222'),
                )
                st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
                st.caption(
                    "Red bars: if price rises through this strike, dealers must buy MORE futures with each tick. "
                    "Blue bars: self-limiting (gamma shrinking). "
                    "Tall red bars near spot = explosion zone.")
    except ImportError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET PHASE ENGINE PANEL  (Upgrade 1)
# ═══════════════════════════════════════════════════════════════════════════════

_PHASE_COLOURS = {
    'OPEN_DISCOVERY':   '#888888',
    'BALANCED':         '#00C8FF',
    'TREND':            '#00FF88',
    'COMPRESSION':      '#FFA500',
    'EXPIRY_PIN':       '#FF6464',
    'EXPIRY_BREAK':     '#FF0000',
    'LIQUIDITY_VACUUM': '#FFFF00',
    'MEAN_REVERSION':   '#AA88FF',
}
_PHASE_ICONS = {
    'OPEN_DISCOVERY':   '🌅',
    'BALANCED':         '⚖️',
    'TREND':            '🚀',
    'COMPRESSION':      '🗜️',
    'EXPIRY_PIN':       '📌',
    'EXPIRY_BREAK':     '💥',
    'LIQUIDITY_VACUUM': '⚡',
    'MEAN_REVERSION':   '🔄',
}


def market_phase_panel(m: dict):
    """
    Primary market phase display. Anchors all other panels.
    Phase is the single most important context signal.
    All other signals should be read through this lens.
    """
    phase   = m.get('market_phase', 'BALANCED')
    note    = m.get('market_phase_note', '—')
    conf    = m.get('market_phase_conf', 0.0)
    reason  = m.get('market_phase_reason', '—')
    trade   = m.get('phase_trade_ok', True)
    qcap    = m.get('phase_quality_cap', 100)

    col    = _PHASE_COLOURS.get(phase, '#888')
    icon   = _PHASE_ICONS.get(phase, '⚪')
    pulse  = phase in ('EXPIRY_BREAK', 'TREND', 'LIQUIDITY_VACUUM')
    anim   = 'animation:blink 1.2s linear infinite;' if pulse else ''
    bar    = '█' * int(conf * 10) + '░' * (10 - int(conf * 10))
    tblock = (f"<span style='background:rgba(255,0,0,.15);border:1px solid #FF4B4B;"
              f"border-radius:4px;padding:2px 8px;font-size:12px;color:#FF4B4B;'>"
              f"TRADE BLOCKED</span>") if not trade else (
             f"<span style='background:rgba(0,255,0,.1);border:1px solid #00FF00;"
             f"border-radius:4px;padding:2px 8px;font-size:12px;color:#00FF00;'>"
             f"TRADE OK</span>")

    st.markdown(f"""
    <div style='background:#0d1117;border:2px solid {col};border-radius:8px;
                padding:12px 18px;margin-bottom:10px;font-family:monospace;{anim}'>
      <div style='display:flex;align-items:center;gap:16px;flex-wrap:wrap;'>
        <span style='font-size:32px;font-weight:bold;color:{col};'>{icon} {phase}</span>
        {tblock}
        <span style='font-size:11px;color:#888;'>Q-cap: {qcap}</span>
      </div>
      <div style='color:#888;font-size:12px;margin:6px 0;'>
        [{bar}] {conf*100:.0f}% confidence
      </div>
      <div style='color:#DDD;font-size:13px;margin-bottom:4px;'>{note.split('—',1)[-1].strip() if '—' in note else note}</div>
      <div style='color:#666;font-size:11px;'>{reason}</div>
    </div>
    """, unsafe_allow_html=True)


def phase_signal_modifier_panel(m: dict):
    """Shows how the current phase is modifying each signal weight."""
    phase_r = m.get('phase_r', {})
    mods    = phase_r.get('modifiers', {})
    if not mods:
        st.caption("No phase modifiers active")
        return

    phase = m.get('market_phase', 'BALANCED')
    col   = _PHASE_COLOURS.get(phase, '#888')

    driver_labels = {
        'gamma_regime': 'γ Regime', 'cascade': 'Cascade',
        'delta_flow': 'δ Flow', 'vacuum': 'Vacuum',
        'hero_zero': 'Hero-Zero', 'overall_confidence': 'Overall',
    }

    bars = ''
    for k, label in driver_labels.items():
        v   = mods.get(k, 1.0)
        oc  = mods.get('overall_confidence', 1.0)
        if k == 'overall_confidence':
            pct = int(v * 100)
            c   = '#00FF88' if v >= 1.0 else ('#FFA500' if v >= 0.7 else '#FF4B4B')
        else:
            pct = int(v * 50)  # 1.0 = 50%, 2.0 = 100%
            c   = '#FF4B4B' if v >= 1.5 else ('#00FF88' if v >= 1.0 else '#888')
        bars += (f"<div style='display:inline-block;margin-right:10px;text-align:center;min-width:55px;'>"
                 f"<div style='font-size:10px;color:#888;'>{label}</div>"
                 f"<div style='background:{c};width:100%;height:{max(pct//2,2)}px;'></div>"
                 f"<div style='font-size:11px;color:{c};font-weight:bold;'>×{v:.1f}</div></div>")

    st.markdown(
        f"<div style='background:#111;border:1px solid {col}40;border-radius:6px;"
        f"padding:10px;margin-bottom:8px;'>"
        f"<div style='font-size:11px;color:#666;margin-bottom:6px;letter-spacing:1px;'>PHASE WEIGHT MODIFIERS</div>"
        f"{bars}</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# DIRECTIONAL PROBABILITY PANEL  (Upgrade 3)
# ═══════════════════════════════════════════════════════════════════════════════

def directional_probability_panel(m: dict):
    """
    The single most actionable panel on the dashboard.

    Replaces the arbitrary CALL LEAN / PUT LEAN scoring with
    proper probability estimates.

    P(up move) = 62%  is far more useful than "CALL LEAN (edge 4)"
    """
    p_up   = m.get('p_up', 0.5)
    p_dn   = m.get('p_dn', 0.5)
    dirn   = m.get('dir_direction', 'NEUTRAL')
    edge   = m.get('dir_edge', 0.0)
    label  = m.get('dir_label', '⚪ —')
    mp     = m.get('move_prob', 0.0)
    mp_d   = m.get('move_direction', '—')

    # Colour by conviction
    if   edge >= 0.20:  col = '#FF0000' if dirn == 'DOWN' else '#00FF00'
    elif edge >= 0.10:  col = '#FFA500'
    else:               col = '#888888'

    # Bar chart
    up_w = int(p_up * 200)
    dn_w = int(p_dn * 200)

    st.markdown(f"""
    <div style='background:#0d1117;border:2px solid {col};border-radius:8px;
                padding:14px 18px;margin-bottom:10px;font-family:monospace;'>
      <div style='font-size:11px;color:#666;letter-spacing:2px;margin-bottom:8px;'>DIRECTIONAL PROBABILITY</div>
      <div style='font-size:22px;font-weight:bold;color:{col};margin-bottom:10px;'>{label}</div>
      <div style='margin-bottom:10px;'>
        <div style='display:flex;align-items:center;gap:8px;margin-bottom:4px;'>
          <span style='color:#00FF88;font-size:12px;width:24px;'>↑</span>
          <div style='background:#00FF88;height:18px;width:{up_w}px;border-radius:2px;'></div>
          <span style='color:#00FF88;font-weight:bold;font-size:14px;'>{p_up*100:.0f}%</span>
        </div>
        <div style='display:flex;align-items:center;gap:8px;'>
          <span style='color:#FF4B4B;font-size:12px;width:24px;'>↓</span>
          <div style='background:#FF4B4B;height:18px;width:{dn_w}px;border-radius:2px;'></div>
          <span style='color:#FF4B4B;font-weight:bold;font-size:14px;'>{p_dn*100:.0f}%</span>
        </div>
      </div>
      <div style='display:flex;gap:20px;font-size:12px;color:#AAA;border-top:1px solid #222;padding-top:8px;'>
        <span>Edge: <b style='color:{col};'>{edge*100:.0f}%</b></span>
        <span>Move prob: <b style='color:#FFA500;'>{mp*100:.0f}%</b> {mp_d}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL STABILITY PANEL  (Upgrade 5)
# ═══════════════════════════════════════════════════════════════════════════════

def stability_status_bar(m: dict):
    """
    Compact status bar showing which signals are stability-confirmed
    vs pending/unstable. Gives the trader an instant read on signal quality.

    CONFIRMED = persisted for N consecutive frames = actionable
    PENDING   = appeared but not yet confirmed = observe only
    """
    stable_cascade  = m.get('stable_cascade',  False)
    stable_vacuum   = m.get('stable_vacuum',   False)
    stable_compress = m.get('stable_compress', False)

    # Also show raw signals for comparison
    raw_cascade  = m.get('is_cascade',   False)
    raw_vacuum   = m.get('in_vacuum',    False)
    raw_compress = m.get('is_compressed', False)

    def _dot(stable, raw, label):
        if stable:
            col, tag, txt = '#00FF00', 'CONFIRMED', label
        elif raw:
            col, tag, txt = '#FFA500', 'PENDING', label
        else:
            col, tag, txt = '#333333', 'INACTIVE', label
        return (f"<div style='display:inline-block;margin-right:12px;font-family:monospace;"
                f"font-size:12px;border:1px solid {col}40;border-radius:4px;"
                f"padding:3px 8px;background:{col}15;'>"
                f"<span style='color:{col};font-weight:bold;'>●</span> "
                f"<span style='color:#CCC;'>{txt}</span> "
                f"<span style='color:{col};font-size:10px;'>{tag}</span></div>")

    html = (
        "<div style='background:#0d1117;border:1px solid #222;border-radius:6px;"
        "padding:8px 12px;margin-bottom:8px;'>"
        "<span style='font-size:10px;color:#555;letter-spacing:1px;margin-right:12px;'>STABILITY FILTER</span>"
        + _dot(stable_cascade,  raw_cascade,  'CASCADE')
        + _dot(stable_vacuum,   raw_vacuum,   'VACUUM')
        + _dot(stable_compress, raw_compress, 'COMPRESSION')
        + "</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT AWARENESS PANEL  (Problem 7)
# ═══════════════════════════════════════════════════════════════════════════════

_EVENT_COLOURS = {
    'NONE':    '#333333',
    'LOW':     '#006633',
    'MEDIUM':  '#CC8800',
    'HIGH':    '#CC3300',
    'EXTREME': '#FF0000',
}
_GLOBAL_RISK_COLOURS = {
    'CALM':     '#00AA44',
    'ELEVATED': '#FFA500',
    'RISK_OFF': '#FF3333',
}


def event_awareness_panel(m: dict):
    """
    Event awareness + global market context.

    Honest about what it cannot do:
    - Cannot read live news feeds
    - Cannot detect intraday macro shocks
    - RBI dates require manual list maintenance

    What it does reliably:
    - NSE expiry calendar (always accurate, computed from weekday arithmetic)
    - Pre-populated RBI / US CPI dates (quarterly update needed)
    - India VIX regime if provided
    """
    level       = m.get('event_risk_level', 'NONE')
    events_t    = m.get('events_today', [])
    events_s    = m.get('events_soon',  [])
    note        = m.get('event_note', '✅ No major events')
    q_damp      = m.get('event_quality_damp', 0.0)
    expiry_type = m.get('expiry_type', 'DAILY')
    global_risk = m.get('global_risk', 'CALM')
    vix_regime  = m.get('vix_regime',  'NORMAL')
    gr_note     = m.get('global_risk_note', '✅ Global calm')

    ec  = _EVENT_COLOURS.get(level, '#333')
    grc = _GLOBAL_RISK_COLOURS.get(global_risk, '#888')

    events_html = ''.join(
        f"<div style='color:#DDD;font-size:12px;'>• {e}</div>" for e in events_t
    ) or "<div style='color:#555;font-size:12px;'>No events today</div>"

    soon_html = ''.join(
        f"<div style='color:#888;font-size:11px;'>⏰ {e}</div>" for e in events_s[:3]
    ) if events_s else ''

    damp_str = f" | Quality −{q_damp*100:.0f}%" if q_damp > 0 else ''

    st.markdown(f"""
    <div style='background:#0d1117;border:2px solid {ec};border-radius:8px;
                padding:12px 18px;margin-bottom:8px;font-family:monospace;'>
      <div style='display:flex;align-items:center;gap:16px;margin-bottom:8px;flex-wrap:wrap;'>
        <span style='font-size:18px;font-weight:bold;color:{ec};'>
          EVENT RISK: {level}{damp_str}
        </span>
        <span style='font-size:13px;color:{grc};border:1px solid {grc}40;
                     border-radius:4px;padding:2px 8px;'>
          GLOBAL: {global_risk}
        </span>
        <span style='font-size:12px;color:#888;'>{expiry_type.replace("_"," ")}</span>
      </div>
      <div style='margin-bottom:6px;'>{events_html}</div>
      {soon_html}
      <div style='font-size:11px;color:#555;margin-top:6px;border-top:1px solid #222;
                  padding-top:6px;'>{gr_note}</div>
      <div style='font-size:10px;color:#333;margin-top:4px;'>
        ⚠ RBI/macro dates require quarterly manual update. Cannot detect intraday news.
      </div>
    </div>
    """, unsafe_allow_html=True)


def event_sidebar_widget(m: dict):
    """Compact version for sidebar."""
    level = m.get('event_risk_level', 'NONE')
    note  = m.get('event_note', '—')
    col   = _EVENT_COLOURS.get(level, '#333')
    import streamlit as _st
    _st.sidebar.markdown(
        f"<div style='background:{col}20;border-left:3px solid {col};"
        f"padding:6px 10px;border-radius:4px;font-size:12px;font-family:monospace;'>"
        f"<b style='color:{col};'>EVENT: {level}</b><br>"
        f"<span style='color:#AAA;'>{note[:60]}</span></div>",
        unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# DEALER / GEX UNCERTAINTY PANEL  (Problem 1)
# ═══════════════════════════════════════════════════════════════════════════════

def dealer_uncertainty_panel(m: dict):
    """
    Visualises the uncertainty ranges on GEX and dealer delta estimates.

    The critique's core point:
        Real dealer inventory = exchange OI + OTC + block + cross-market.
        We can only see exchange OI.
        So our estimates could be 25–35% off.

    This panel makes the uncertainty VISIBLE rather than hiding it
    behind a single confident number, which was the critique's suggestion.
    """
    net_gex    = m.get('net_gex', 0.0)
    gex_lo     = m.get('gex_low', net_gex * 0.65)
    gex_hi     = m.get('gex_high', net_gex * 1.35)
    gex_note   = m.get('gex_uncertainty_note', '')

    dd         = m.get('dealer_delta_M', 0.0)
    dd_lo      = m.get('dealer_delta_low',  dd * 0.75)
    dd_hi      = m.get('dealer_delta_high', dd * 1.25)
    dd_note    = m.get('dealer_note', '')

    def _bar_html(lo, hi, point, label, unit='M'):
        total_range = abs(hi - lo)
        pct = 50 if total_range < 0.001 else int(max(0, min((point - lo) / total_range * 100, 100)))
        colour = '#FF4B4B' if point < 0 else '#00C8FF'
        band_left  = max(pct - 35, 0)
        band_width = min(pct + 35, 100) - band_left
        # No leading whitespace — avoids Streamlit markdown treating indented HTML as code block
        return (
            "<div style='margin-bottom:12px;'>"
            "<div style='display:flex;justify-content:space-between;"
            "font-size:11px;color:#666;margin-bottom:3px;'>"
            f"<span>{label}</span>"
            f"<span style='color:{colour};font-weight:bold;'>{point:+.2f}{unit}</span>"
            "</div>"
            "<div style='position:relative;background:#1a1a1a;height:12px;"
            "border-radius:6px;overflow:visible;'>"
            "<div style='position:absolute;left:0;right:0;top:2px;height:8px;"
            "background:#333;border-radius:4px;'></div>"
            f"<div style='position:absolute;left:{band_left}%;"
            f"width:{band_width}%;top:2px;height:8px;"
            f"background:{colour}33;border-radius:4px;'></div>"
            f"<div style='position:absolute;left:{pct}%;top:-2px;width:3px;height:16px;"
            f"background:{colour};border-radius:2px;transform:translateX(-50%);'></div>"
            "</div>"
            "<div style='display:flex;justify-content:space-between;"
            "font-size:10px;color:#444;margin-top:2px;'>"
            f"<span>Low: {lo:+.2f}{unit}</span>"
            f"<span>High: {hi:+.2f}{unit}</span>"
            "</div></div>"
        )

    gex_bar = _bar_html(gex_lo, gex_hi, net_gex, "Net GEX  (±35%)")
    dd_bar  = _bar_html(dd_lo, dd_hi, dd, "Dealer Δ (±25%)")

    html_out = (
        "<div style='background:#0d1117;border:1px solid #333;border-radius:8px;"
        "padding:12px 18px;margin-bottom:8px;font-family:monospace;'>"
        "<div style='font-size:11px;color:#555;letter-spacing:2px;margin-bottom:10px;'>"
        "MODEL UNCERTAINTY — exchange OI only, OTC/block positions invisible"
        "</div>"
        + gex_bar + dd_bar +
        f"<div style='font-size:10px;color:#333;margin-top:6px;line-height:1.4;'>"
        f"{gex_note[:120]}</div></div>"
    )
    st.markdown(html_out, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CALIBRATION STATUS PANEL  (Problem 6)
# ═══════════════════════════════════════════════════════════════════════════════

def calibration_panel(m: dict, db_path: str = 'market_data.db'):
    """
    Shows current calibration status and per-bucket hit rates.

    Before 20 samples: shows prior (uncalibrated).
    After 20 samples:  shows empirical hit rates from signal_log table.

    The honest message: "P=70% means different things before and after calibration."
    """
    from signals.calibration import get_calibration_summary, apply_calibration
    try:
        summary = get_calibration_summary(db_path)
    except Exception:
        st.caption("⚪ Calibration engine not yet initialised")
        return

    total   = summary['total_logged']
    closed  = summary['total_closed']
    is_live = summary['is_live']
    note    = summary['note']
    table   = summary['table']

    status_col = '#00FF88' if is_live else '#888'

    # Calibration table rows
    rows_html = ''
    for (lo, hi), (rate, n) in sorted(table.items()):
        bar_w  = int(rate * 80)
        r_col  = '#00FF88' if rate >= 0.55 else ('#FFA500' if rate >= 0.45 else '#FF4B4B')
        rows_html += (
            f"<div style='display:flex;align-items:center;gap:8px;margin:3px 0;"
            f"font-size:11px;'>"
            f"<span style='color:#666;width:55px;text-align:right;'>{lo}–{hi}</span>"
            f"<div style='background:{r_col};height:10px;width:{bar_w}px;"
            f"border-radius:3px;'></div>"
            f"<span style='color:{r_col};font-weight:bold;width:36px;'>{rate*100:.0f}%</span>"
            f"<span style='color:#333;'>n={n}</span>"
            f"</div>"
        )

    raw_q  = m.get('quality', 50)
    try:
        cal_r  = apply_calibration(db_path, float(raw_q))
        cal_str = f"Raw {raw_q} → Calibrated {cal_r['calibrated']:.0f}% ({cal_r['note'][:40]})"
    except Exception:
        cal_str = f"Raw quality: {raw_q}"

    st.markdown(f"""
    <div style='background:#0d1117;border:1px solid #222;border-radius:8px;
                padding:12px 18px;margin-bottom:8px;font-family:monospace;'>
      <div style='display:flex;align-items:center;gap:12px;margin-bottom:8px;'>
        <span style='color:{status_col};font-weight:bold;font-size:13px;'>{note}</span>
        <span style='color:#555;font-size:11px;'>{total} logged | {closed} closed</span>
      </div>
      <div style='font-size:10px;color:#555;margin-bottom:8px;letter-spacing:1px;'>
        15-MIN HIT RATES BY SCORE BAND
      </div>
      {rows_html}
      <div style='font-size:11px;color:#888;margin-top:8px;border-top:1px solid #222;
                  padding-top:6px;'>{cal_str}</div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MOVE PROBABILITY INTERACTION PANEL  (Problem 3)
# ═══════════════════════════════════════════════════════════════════════════════

def move_interaction_panel(m: dict):
    """
    Shows which signal interactions fired and their multiplicative amplification.

    Addresses the critique: "gamma + vacuum should MULTIPLY, not add."
    This panel makes the interaction multiplier visible so the trader
    understands why move_prob jumped from 45 to 72.
    """
    mult = m.get('interaction_mult', 1.0)
    mp   = m.get('move_prob', 0.0)
    n    = m.get('move_confirming_n', 0)

    # Colour by amplification level
    if   mult >= 1.8:  col = '#FF0000'
    elif mult >= 1.4:  col = '#FF6600'
    elif mult >= 1.1:  col = '#FFA500'
    else:              col = '#666666'

    bar_w = int(min((mult - 1.0) / 1.5 * 160, 160))   # 0px at 1.0×, 160px at 2.5×

    # Determine which interactions fired
    driver_scores = m.get('move_driver_scores', {})
    gs = driver_scores.get('gamma_regime', 0.0)
    vs = driver_scores.get('vacuum', 0.0)
    cs = driver_scores.get('cascade', 0.0)
    gc = driver_scores.get('gamma_convexity', 0.0)

    combos = []
    if gs > 0.5 and vs > 0.5:
        combos.append(f"γ×Vacuum ×{1.0+gs*vs*0.4:.2f}")
    if gs > 0.5 and gc > 0.5:
        combos.append(f"γ×Convexity ×{1.0+gs*gc*0.3:.2f}")
    if cs > 0.7 and vs > 0.5:
        combos.append(f"Cascade×Vacuum ×{1.0+cs*vs*0.35:.2f}")
    if gs > 0.6 and vs > 0.5 and cs > 0.6:
        combos.append("TRIPLE ×1.15")

    combos_html = ' &nbsp; '.join(
        f"<span style='background:{col}22;border:1px solid {col}44;"
        f"border-radius:3px;padding:2px 6px;font-size:11px;color:{col};'>{c}</span>"
        for c in combos
    ) if combos else "<span style='color:#444;font-size:11px;'>No active interactions</span>"

    st.markdown(f"""
    <div style='background:#0d1117;border:1px solid {col}40;border-radius:8px;
                padding:10px 16px;margin-bottom:8px;font-family:monospace;'>
      <div style='font-size:10px;color:#555;letter-spacing:2px;margin-bottom:6px;'>
        SIGNAL INTERACTIONS  (gamma×vacuum multiply, not add)
      </div>
      <div style='display:flex;align-items:center;gap:12px;margin-bottom:8px;'>
        <span style='color:{col};font-size:20px;font-weight:bold;'>×{mult:.2f}</span>
        <div style='background:{col};height:12px;width:{bar_w}px;border-radius:4px;'></div>
        <span style='color:#888;font-size:12px;'>
          Move prob: <b style='color:#FFA500;'>{mp:.0f}%</b> | {n} drivers confirming
        </span>
      </div>
      <div>{combos_html}</div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# FLOW PERSISTENCE PANEL  (Final Structural Upgrade)
# ═══════════════════════════════════════════════════════════════════════════════

from signals.flow_memory import pressure_to_label, persist_mult_to_label


def flow_persistence_panel(m: dict):
    """
    Renders the Market State Memory — Flow Persistence Engine.

    This panel answers the question the rest of the dashboard cannot:
        "Has this pressure been building, or is it a single snapshot?"

    Displays:
        - Pressure score (0–100): stored energy in the market
        - Buildup phase: DORMANT / LOADING / PRIMED / EXPLOSIVE
        - Per-signal duration bars: delta flow, gamma, vacuum, compression
        - Persist multiplier: how much this amplifies hero_zero
        - Delta flow acceleration: is demand increasing?

    The critique insight being implemented:
        "Compression lasting 22 minutes + gamma wall nearby + delta flow
        increasing = much higher probability than single-frame detection."
    """
    pressure    = float(m.get('pressure_score',   0.0))
    persist_m   = float(m.get('persist_mult',     1.0))
    phase       = m.get('buildup_phase',           'DORMANT')
    hz_pm       = float(m.get('hz_persist_mult',   1.0))
    mp_bonus    = float(m.get('move_pressure_bonus', 0.0))
    delta_accel = float(m.get('delta_accel_p',     0.0))
    delta_dir   = m.get('delta_direction_p',       'NEUTRAL')

    # Signal durations
    d_delta   = float(m.get('delta_dur_min',    0.0))
    d_gamma   = float(m.get('gamma_dur_min',    0.0))
    d_vac     = float(m.get('vacuum_dur_min',   0.0))
    d_comp    = float(m.get('compress_dur_min', 0.0))

    # Persistence scores (0–1)
    ps_d   = float(m.get('ps_delta',   0.0))
    ps_g   = float(m.get('ps_gamma',   0.0))
    ps_v   = float(m.get('ps_vacuum',  0.0))
    ps_c   = float(m.get('ps_compress',0.0))

    # Phase colour
    phase_col = {
        'EXPLOSIVE': '#FF0000',
        'PRIMED':    '#FF6600',
        'LOADING':   '#FFA500',
        'DORMANT':   '#444444',
    }.get(phase, '#444')

    # Main pressure bar
    bar_pct = int(min(pressure, 100))
    pbar_col = phase_col

    # Duration bars
    def _dur_bar(label, dur_min, ps, target_min, colour='#00C8FF'):
        """Render a single duration bar with fill based on ps (0–1)."""
        fill_w = int(ps * 160)
        dur_str = f"{dur_min:.1f}m"
        target_str = f"/{target_min}m"
        accel_dot = (
            f"<span style='color:#00FF88;font-size:10px;margin-left:4px;'>▲</span>"
            if label == 'δ FLOW' and delta_accel > 0.05 else
            f"<span style='color:#FF4B4B;font-size:10px;margin-left:4px;'>▼</span>"
            if label == 'δ FLOW' and delta_accel < -0.05 else ""
        )
        return (
            f"<div style='display:flex;align-items:center;gap:8px;margin:4px 0;"
            f"font-size:11px;font-family:monospace;'>"
            f"<span style='color:#999;width:80px;text-align:right;'>{label}</span>"
            f"<div style='background:#111;width:160px;height:10px;border-radius:5px;"
            f"overflow:hidden;position:relative;'>"
            f"<div style='background:{colour};width:{fill_w}px;height:100%;"
            f"border-radius:5px;'></div></div>"
            f"<span style='color:{colour};font-weight:bold;'>{dur_str}</span>"
            f"<span style='color:#333;'>{target_str}</span>"
            f"{accel_dot}</div>"
        )

    # Delta flow colour based on direction
    dfl_col = '#00FF88' if delta_dir == 'BULLISH' else ('#FF4B4B' if delta_dir == 'BEARISH' else '#888')

    bars_html = (
        _dur_bar('δ FLOW', d_delta, ps_d, 8,  dfl_col)  +
        _dur_bar('γ SHORT', d_gamma, ps_g, 15, '#FF9900') +
        _dur_bar('VACUUM',  d_vac,   ps_v, 5,  '#00C8FF') +
        _dur_bar('COMPRESS',d_comp,  ps_c, 12, '#CC44FF')
    )

    accel_str = (
        f"<span style='color:#00FF88;'>▲ accelerating</span>" if delta_accel > 0.05 else
        f"<span style='color:#FF4B4B;'>▼ fading</span>" if delta_accel < -0.05 else
        f"<span style='color:#555;'>→ steady</span>"
    )

    st.markdown(f"""
    <div style='background:#0d1117;border:2px solid {phase_col};border-radius:8px;
                padding:14px 18px;margin-bottom:8px;font-family:monospace;'>

      <div style='display:flex;align-items:center;justify-content:space-between;
                  margin-bottom:10px;flex-wrap:wrap;gap:10px;'>
        <div>
          <span style='font-size:11px;color:#555;letter-spacing:2px;'>
            MARKET PRESSURE MEMORY
          </span><br>
          <span style='font-size:22px;font-weight:bold;color:{phase_col};'>
            {phase}
          </span>
        </div>
        <div style='text-align:right;'>
          <span style='font-size:28px;font-weight:bold;color:{phase_col};'>
            {pressure:.0f}
          </span>
          <span style='font-size:13px;color:#666;'>/100</span>
          <br>
          <span style='font-size:11px;color:#555;'>stored energy</span>
        </div>
      </div>

      <div style='background:#111;height:12px;border-radius:6px;overflow:hidden;
                  margin-bottom:12px;'>
        <div style='background:linear-gradient(90deg,{phase_col}88,{phase_col});
                    width:{bar_pct}%;height:100%;border-radius:6px;
                    transition:width 0.5s ease;'></div>
      </div>

      {bars_html}

      <div style='display:flex;gap:20px;margin-top:10px;border-top:1px solid #222;
                  padding-top:8px;flex-wrap:wrap;'>
        <div style='font-size:12px;'>
          <span style='color:#555;'>δ FLOW: </span>
          <span style='color:{dfl_col};font-weight:bold;'>{delta_dir}</span>
          &nbsp;{accel_str}
        </div>
        <div style='font-size:12px;'>
          <span style='color:#555;'>HZ ×</span>
          <span style='color:#FFA500;font-weight:bold;'>{hz_pm:.2f}</span>
        </div>
        <div style='font-size:12px;'>
          <span style='color:#555;'>MP +</span>
          <span style='color:#FFA500;font-weight:bold;'>{mp_bonus:.1f}pts</span>
        </div>
        <div style='font-size:12px;'>
          <span style='color:#555;'>mult: </span>
          <span style='color:#DDD;'>{persist_mult_to_label(persist_m)}</span>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)


def persistence_hud_metric(m: dict):
    """Compact pressure score for HUD metrics row."""
    pressure = float(m.get('pressure_score', 0.0))
    phase    = m.get('buildup_phase', 'DORMANT')
    pm       = float(m.get('persist_mult', 1.0))
    col      = "normal" if pressure > 50 else "off"
    st.metric(
        "PRESSURE",
        f"{pressure:.0f}/100",
        f"×{pm:.2f} {phase[:8]}",
        delta_color=col,
    )