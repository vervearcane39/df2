"""
DRIFT HUNTER Phase-9.2  —  drift_dash.py
Entry point: display only. Zero business logic.

Tabs:
  1. 📡 Live          — real-time permission + flow dashboard
  2. 🎯 Market Control — unified buyers/sellers dominance
  3. 🌡️ GEX           — heatmap + sensitivity map + history
  4. 📊 Backtest       — real-replay with BS PnL model
  5. 🔬 Regime Stats   — rolling distribution diagnostics

Run:  streamlit run drift_dash.py
"""
import streamlit as st
import uuid
import pandas as pd
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="Drift Hunter Phase-9.2", layout="wide")
DB_PATH = "market_data.db"

from data.db_loader         import load_all
from dashboard.state        import init_state
from dashboard.orchestrator  import run
from dashboard.panels        import (
    focus_mode_panel,
    permission_panel, direction_panel, weighted_score_panel,
    layered_quality_panel, iv_panel, vwap_panel, gex_panel,
    dealer_flow_panel, struct_panel, narrative_panel,
    alerts_section, hud_metrics,
    futures_panel, probability_panel,
    market_control_panel, sensitivity_panel, gex_history_panel, gex_heatmap,
    cascade_panel, vacuum_panel, institutional_panel,
    move_probability_panel, dealer_regime_panel,
    liquidity_map_panel, skew_dynamics_panel,
    # Phase 9.5 — Hero-Zero Engine
    hero_zero_panel, hero_zero_detail_panel, hero_zero_conditions_panel, cascade_state_panel,
    # Final layer — dGamma/dSpot
    gamma_convexity_panel,
    # Upgrade 1: Market Phase Engine
    market_phase_panel, phase_signal_modifier_panel,
    # Upgrade 3: Directional Probability
    directional_probability_panel,
    # Upgrade 5: Signal Stability
    stability_status_bar,
    # Problem 1: Dealer/GEX uncertainty
    dealer_uncertainty_panel,
    # Problem 3: Move interaction multiplier
    move_interaction_panel,
    # Problem 6: Calibration status
    calibration_panel,
    # Problem 7: Event awareness
    event_awareness_panel, event_sidebar_widget,
    # Final Structural Upgrade: Flow Persistence
    flow_persistence_panel, persistence_hud_metric,
)

init_state()

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""<style>
.stMetric{background:#0E1117;border:1px solid #333;padding:10px;border-radius:5px}
.trap-alert{color:#FF4B4B;font-weight:bold;font-size:17px;padding:10px;
  border:1px solid #FF4B4B;border-radius:5px;background:rgba(255,75,75,.1);
  animation:blink 2s linear infinite}
.exit-alert{color:#FFA500;font-weight:bold;font-size:17px;padding:10px;
  border:1px solid #FFA500;border-radius:5px;background:rgba(255,165,0,.1);margin-bottom:8px}
.gamma-flip{color:#00FFFF;font-weight:bold;font-size:17px;padding:10px;
  border:2px solid #00FFFF;border-radius:5px;background:rgba(0,255,255,.08);
  animation:blink 1.5s linear infinite;margin-bottom:8px}
.vol-shock{color:#00FFFF;font-weight:bold;font-size:15px;padding:8px;
  border:1px solid #00FFFF;border-radius:5px;background:rgba(0,255,255,.07);margin-bottom:8px}
.trapped-sellers{color:#FF69B4;font-weight:bold;font-size:15px;padding:8px;
  border:1px solid #FF69B4;border-radius:5px;background:rgba(255,105,180,.07);margin-bottom:8px}
.compression-alert{color:#7FFF00;font-weight:bold;font-size:15px;padding:8px;
  border:1px solid #7FFF00;border-radius:5px;background:rgba(127,255,0,.07);margin-bottom:8px}
.migration-alert{color:#DA70D6;font-weight:bold;font-size:15px;padding:8px;
  border:1px solid #DA70D6;border-radius:5px;background:rgba(218,112,214,.07);margin-bottom:8px}
.narrative-box{background:#1E2329;border-left:5px solid #00C8FF;color:#EEE;padding:14px;
  border-radius:5px;margin-bottom:10px;font-family:monospace;font-size:15px}
.iv-box{background:#1a1a2e;border-left:5px solid #9B59B6;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:14px}
.vwap-box{background:#1a2a1e;border-left:5px solid #32CD32;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:14px}
.gex-box{background:#1a1e2a;border-left:5px solid #FFD700;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:14px}
.dealer-box{background:#2a1a1a;border-left:5px solid #FF6347;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:14px}
.futures-box{background:#1a2a2a;border-left:5px solid #20B2AA;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:14px}
.prob-box{background:#1a1a1a;border-left:5px solid #FFD700;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:15px}
.struct-box{background:#1e1a2a;border-left:5px solid #FF8C00;color:#DDD;padding:11px;
  border-radius:5px;margin-bottom:8px;font-family:monospace;font-size:14px}
.layer-box{background:#1a2030;border:1px solid #334;color:#CCC;padding:12px;
  border-radius:8px;margin-bottom:10px;font-family:monospace;font-size:13px}
.direction-box{padding:12px;border-radius:8px;text-align:center;font-size:20px;
  font-weight:bold;margin-bottom:12px}
.permission-box-green{background:rgba(0,255,0,.1);border:2px solid #00FF00;color:#00FF00;
  padding:15px;text-align:center;font-size:24px;font-weight:bold;border-radius:10px;margin-bottom:18px}
.permission-box-red{background:rgba(255,0,0,.1);border:2px solid #FF0000;color:#FF4B4B;
  padding:15px;text-align:center;font-size:24px;font-weight:bold;border-radius:10px;margin-bottom:18px}
.permission-box-blue{background:rgba(0,200,255,.1);border:2px solid #00C8FF;color:#00C8FF;
  padding:15px;text-align:center;font-size:24px;font-weight:bold;border-radius:10px;margin-bottom:18px}
.invalidation-box{color:#FFA500;font-size:14px;border-left:3px solid #FFA500;
  padding-left:10px;margin:8px 0}
.lookback-warn{color:#FFD700;font-size:12px;padding:4px 8px;background:rgba(255,215,0,.08);
  border-left:3px solid #FFD700;margin-bottom:6px}
.health-warn{font-weight:bold;padding:5px 10px;border-radius:4px;margin-bottom:8px;font-size:13px}
@keyframes blink{50%{opacity:.6}}
</style>""", unsafe_allow_html=True)


# ── Chain table styler ────────────────────────────────────────────────────────
def _style_chain(view: pd.DataFrame, m: dict, center: int):
    oi_ce = max(view['oi_chg_ce'].fillna(0).abs().max(), 1) if 'oi_chg_ce' in view.columns else 1
    oi_pe = max(view['oi_chg_pe'].fillna(0).abs().max(), 1) if 'oi_chg_pe' in view.columns else 1

    def _hi_atm(row):
        b = 'background-color:#262730;border-top:2px solid #777;border-bottom:2px solid #777;'
        return [b] * len(row) if row.name == center else [''] * len(row)

    def _hi_levels(row):
        if row.name == m.get('gex_wall'):
            return ['border-top:2px solid #FFD700;border-bottom:2px solid #FFD700;'] * len(row)
        if row.name == m.get('max_pain'):
            return ['border-top:2px dashed #FF8C00;border-bottom:2px dashed #FF8C00;'] * len(row)
        gf = m.get('gex_flip')
        if gf and row.name == gf:
            return ['border-top:2px dashed #00FFFF;border-bottom:2px dashed #00FFFF;'] * len(row)
        ms = m.get('magnet_strike')
        if ms and row.name == ms:
            return ['background-color:rgba(255,215,0,.05);'] * len(row)
        ez = m.get('sens_explosion_zones', [])
        if row.name in ez:
            return ['border-left:3px solid #9B59B6;'] * len(row)
        st_ = m.get('struct', {})
        a50 = lambda l: int(round(l / 50) * 50)
        if st_.get('has_pdh') and row.name == a50(st_['pdh']): return ['border-top:1px solid #FF6347;'] * len(row)
        if st_.get('has_pdl') and row.name == a50(st_.get('pdl', 0)): return ['border-top:1px solid #6495ED;'] * len(row)
        return [''] * len(row)

    cols_ce  = [c for c in ['oi_chg_ce'] if c in view.columns]
    cols_pe  = [c for c in ['oi_chg_pe'] if c in view.columns]
    int_cols = [c for c in ['intent_ce', 'intent_pe'] if c in view.columns]
    zn_cols  = [c for c in ['zone'] if c in view.columns]

    styled = view.style.apply(_hi_atm, axis=1).apply(_hi_levels, axis=1)
    if cols_ce: styled = styled.background_gradient(subset=cols_ce, cmap='RdYlGn_r', vmin=-oi_ce, vmax=oi_ce)
    if cols_pe: styled = styled.background_gradient(subset=cols_pe, cmap='RdYlGn', vmin=-oi_pe, vmax=oi_pe)
    if int_cols:
        styled = styled.map(
            lambda x: ("color:#FF4B4B;font-weight:900;" if "+" in str(x) and "SB" in str(x)
                       else ("color:#00FF00;font-weight:900;" if "+" in str(x) else "")),
            subset=int_cols)
    if zn_cols:
        styled = styled.map(
            lambda x: ("color:#FFA500;font-weight:bold;" if "⚔️" in str(x)
                       else ("color:#00C8FF;font-weight:bold;" if "🛡️" in str(x) else "")),
            subset=zn_cols)
    fmt = [c for c in ['oi_chg_ce', 'oi_chg_pe'] if c in view.columns]
    if fmt: styled = styled.format("{:,.0f}", subset=fmt)
    return styled


# ── Sidebar ───────────────────────────────────────────────────────────────────
lookback = st.sidebar.slider("Lookback (Mins)", 1, 360, 15)

st.sidebar.markdown("---")
st.sidebar.markdown("**🌍 Global Context  (P7)**")
# India VIX is now auto-fetched from the live engine feed (token 1340)
# and stored in system_health.india_vix — no manual input needed.
st.sidebar.caption("📡 India VIX: auto-fetched from feed")
_sgx_gap = st.sidebar.number_input("SGX Gap %  (pre-open only)", min_value=-5.0,
                                    max_value=5.0,
                                    value=float(st.session_state.get('sgx_gap_pct', 0.0)),
                                    step=0.1, format="%.1f",
                                    help="GIFT Nifty vs prev close %. Set to 0 once market opens.")
st.session_state['sgx_gap_pct'] = _sgx_gap

st.sidebar.markdown("---")
st.sidebar.caption("Phase 9.5 — 7 weaknesses fixed")
for _line in [
    "P1 ±35% GEX / ±25% dealer ranges",
    "P2 Volume-confirmed trap detection",
    "P3 Multiplicative signal interactions",
    "P4 Vacuum extends hero-zero targets",
    "P5 Bounded stability buffers",
    "P6 Signal calibration logger",
    "P7 Event awareness + VIX context",
    "File: Dynamic velocity thresholds",
    "File: Gamma convexity perf fix",
]:
    st.sidebar.markdown(f"✅ {_line}")

st.sidebar.markdown("---")
st.sidebar.caption("Drift Hunter © 2025")

# ── Shared data loader (runs once per refresh cycle) ─────────────────────────
@st.cache_data(ttl=2, show_spinner=False)
def _load(lb):
    return load_all(DB_PATH, lb)

# ── Tab layout ────────────────────────────────────────────────────────────────
tabs = st.tabs(["🎯 Focus", "📡 Live", "💥 Hero-Zero", "🎯 Impact Engine", "🌡️ GEX", "📊 Backtest", "🔬 Deep Analysis"])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 0 — Focus Mode
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=2)
def focus_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for Engine Data…")
        return
    table, m = run(raw, lookback, DB_PATH)
    if not m:
        st.info("No data yet.")
        return
    h = m['health_score']
    if h < 50:
        st.markdown(
            f"<div style='background:rgba(255,50,50,.1);border:1px solid #FF3232;"
            f"border-radius:6px;padding:8px 14px;color:#FF3232;font-size:13px;"
            f"font-family:monospace;margin-bottom:10px;'>⚠️ FEED {h:.0f}% — DATA DEGRADED</div>",
            unsafe_allow_html=True)
    focus_mode_panel(m, table)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — Live dashboard
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=5)
def live_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for Engine Data…")
        return
    table, m = run(raw, lookback, DB_PATH)
    if table.empty or not m:
        st.info("No options data available.")
        return

    # Health banner
    h = m['health_score']
    if h < 80:
        c = "#FF4B4B" if h < 50 else "#FFA500"
        st.markdown(
            f"<div class='health-warn' style='border-left:4px solid {c};color:{c};'>"
            f"⚠️ FEED {h:.0f}% — {'BLOCKED' if h < 40 else 'DEGRADED'}</div>",
            unsafe_allow_html=True)

    if m.get('lookback_warning'):
        st.markdown(f"<div class='lookback-warn'>{m['lookback_warning']}</div>",
                    unsafe_allow_html=True)

    # Overpriced straddle warning — top of page
    if m.get('straddle_warn'):
        st.markdown(f"<div class='exit-alert'>{m['straddle_warn']}</div>",
                    unsafe_allow_html=True)

    permission_panel(m)

    # ── P7: Event awareness — top of page, cannot miss it ────────────────
    if m.get('event_risk_level', 'NONE') != 'NONE' or m.get('global_risk') != 'CALM':
        event_awareness_panel(m)

    # ── Upgrade 1: Market Phase — the lens for all other signals ─────────
    market_phase_panel(m)

    # ── Upgrade 5: Signal stability — confirmed vs pending ────────────────
    stability_status_bar(m)

    # ── FINAL UPGRADE: Flow Persistence — the "pressure vessel" view ──────
    # This is the most important new panel. It shows accumulated market energy,
    # not just current snapshot. Placed ABOVE directional probability because
    # persistence context changes how you read every other signal.
    flow_persistence_panel(m)

    # ── Upgrade 3: Directional probability + move probability + interactions
    _dc1, _dc2, _dc3 = st.columns([1.2, 1.2, 1])
    with _dc1:
        directional_probability_panel(m)
    with _dc2:
        move_probability_panel(m)
    with _dc3:
        move_interaction_panel(m)   # P3: multiplicative interaction view

    institutional_panel(m)
    direction_panel(m)
    weighted_score_panel(m)
    layered_quality_panel(m)
    probability_panel(m)

    col1, col2 = st.columns([3, 2])
    with col1:
        iv_panel(m)
        vwap_panel(m)
        gex_panel(m)
        # P1: Uncertainty ranges immediately after GEX
        dealer_uncertainty_panel(m)
        dealer_flow_panel(m)
        futures_panel(m)
        struct_panel(m)
    with col2:
        phase_signal_modifier_panel(m)
        narrative_panel(m)
        alerts_section(m)

    hud_metrics(m)

    # Chain table
    center = m['atm_strike']
    view   = table.loc[center - 400: center + 400].copy()
    if view.empty:
        return
    target = ['zone', 'magnet', 'intent_ce', 'flow_type_ce', 'vol_surge_ce',
              'signal_ce', 'oi_chg_ce', 'close_now_ce',
              'close_now_pe', 'oi_chg_pe', 'signal_pe',
              'vol_surge_pe', 'flow_type_pe', 'intent_pe']
    view = view[[c for c in target if c in view.columns]]
    st.dataframe(_style_chain(view, m, center), height=820, width='stretch')


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — Impact Engine  (Move Probability + Dealer Regime + Liquidity Map)
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=5)
def impact_engine_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for data…")
        return
    _, m = run(raw, lookback, DB_PATH)
    if not m:
        return

    st.subheader("🎯 Impact Engine")
    st.caption(
        "Unified move probability from 6 structural drivers. "
        "Nonlinear regime multiplier amplifies signals under dealer panic conditions.")

    # ── Primary: Move probability ─────────────────────────────────────────
    move_probability_panel(m)

    st.markdown("---")

    # ── Two columns: dealer regime + driver breakdown ─────────────────────
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("#### Dealer Regime")
        dealer_regime_panel(m)
        st.markdown("#### Skew Dynamics")
        skew_dynamics_panel(m)

    with col2:
        st.markdown("#### Driver Breakdown")
        driver_scores = m.get('move_driver_scores', {})
        if driver_scores:
            rows = []
            weight_map = {
                'gamma_regime': 22, 'cascade': 20, 'vacuum': 18,
                'delta_flow': 15, 'dealer_pressure': 12,
                'futures_flow': 8, 'sensitivity': 5
            }
            for k, v in sorted(driver_scores.items(), key=lambda x: -x[1]):
                wt = weight_map.get(k, 0)
                rows.append({
                    'Driver':      k.replace('_', ' ').title(),
                    'Score':       round(v, 2),
                    'Weight':      f"{wt}%",
                    'Contribution': round(v * wt, 1),
                })
            import pandas as _pd
            ddf = _pd.DataFrame(rows)
            st.dataframe(ddf.style.map(
                lambda v: ('color:#00FF00' if isinstance(v, float) and v > 0.6
                           else ('color:#FFA500' if isinstance(v, float) and v > 0.3
                                 else 'color:#888')),
                subset=['Score']), hide_index=True, width='stretch')

        # Probability history
        st.markdown("**Confirming signals:** " + str(m.get('move_confirm_n', 0)))
        st.markdown("**Regime multiplier:** " + f"{m.get('move_regime_mult', 1.0):.1f}×")

    st.markdown("---")

    # ── Liquidity map ─────────────────────────────────────────────────────
    st.markdown("#### Liquidity Density Map")
    liquidity_map_panel(m)

    # ── Hedge simulator summary ───────────────────────────────────────────
    st.markdown("#### Dealer Hedge Scenarios")
    up50 = m.get('hedge_up50', {})
    dn50 = m.get('hedge_dn50', {})
    c1, c2, c3 = st.columns(3)
    c1.metric("Spot +50 → Dealers",
              f"{up50.get('net_direction','—')} {up50.get('hedge_futures_M',0):+.1f}M",
              "🔥 Amplifying" if up50.get('amplifying') else "🧊 Dampening")
    c2.metric("Spot -50 → Dealers",
              f"{dn50.get('net_direction','—')} {dn50.get('hedge_futures_M',0):+.1f}M",
              "🔥 Amplifying" if dn50.get('amplifying') else "🧊 Dampening")
    c3.metric("Dominant cascade dir", m.get('hedge_dom_dir', '—'),
              f"Asymmetry: {m.get('hedge_asymmetry',0):+.1f}M")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — Institutional Intelligence (5 Improvements)
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=5)
def institutional_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for data…")
        return
    _, m = run(raw, lookback, DB_PATH)
    if not m:
        return

    st.subheader("⚡ Institutional Intelligence")
    st.caption("5 structural improvements — each targets a conceptual weakness in the previous system.")
    # ── Row 1: Status metrics ────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    fb     = m.get('dealer_flow_breakdown', {})
    dom_oi = max(fb, key=fb.get) if fb else '—'
    c1.metric("1️⃣ OI Flow",         dom_oi.replace('_', ' '))
    c2.metric("2️⃣ Gamma Flip",      str(m.get('gex_flip_exact') or '—'),
              f"±{m.get('gex_dyn_radius', 400)}pt radius")
    c3.metric("3️⃣ Delta Flow",       f"{m.get('delta_flow_norm', 0.0):+.2f}",
              m.get('delta_flow_state', '—')[:20])
    c4.metric("4️⃣ Vacuum",
              "⚡ IN VAC" if m.get('in_vacuum') else f"{m.get('vacuum_severity', 0)*100:.0f}% gaps",
              f"Wall↑:{m.get('next_wall_above') or '—'} ↓:{m.get('next_wall_below') or '—'}")
    c5.metric("5️⃣ Cascade",
              f"{'🚨 ACTIVE' if m.get('is_cascade') else str(m.get('cascade_strength', 0)) + '/3'}",
              m.get('cascade_type') or '—')

    st.markdown("---")

    # ── Improvement 1: Price-OI Flow breakdown ───────────────────────────
    with st.expander("1️⃣ Price-OI Classification — Long/Short Build-up", expanded=True):
        st.caption(
            "Previous system assumed high CE OI = calls written. "
            "Fix: classify each strike by price direction × OI direction. "
            "LONG_BUILD = public buying calls. SHORT_BUILD = public selling calls.")
        if fb:
            col1, col2 = st.columns([2, 3])
            import pandas as _pd
            df_fb = _pd.DataFrame([{'Flow Type': k.replace('_', ' '), 'Count': v}
                                   for k, v in sorted(fb.items(), key=lambda x: -x[1])])
            flow_colors_map = {
                'LONG BUILD': '#00FF00', 'SHORT BUILD': '#FF4B4B',
                'SHORT COVER': '#7FFF00', 'LONG UNWIND': '#FFA500',
                'NEUTRAL': '#888888', 'STALE OI': '#555555',
            }
            col1.dataframe(df_fb.style.map(
                lambda v: f"color:{flow_colors_map.get(v, '#CCC')}" if isinstance(v, str) else '',
                subset=['Flow Type']), hide_index=True, width='stretch')
            col2.markdown(
                f"**Dealer δ (price-OI based):** {m['dealer_delta_M']:+.2f}M\n\n"
                f"**Pressure:** {m['dealer_pressure']}\n\n"
                f"**Skew note:** {m.get('dealer_skew_note', '—')}")
        else:
            st.info("No flow breakdown yet — needs ≥500 OI change per strike")

    # ── Improvement 2: Gamma flip ─────────────────────────────────────────
    with st.expander("2️⃣ True Gamma Flip — Root Solver", expanded=True):
        st.caption(
            "Previous: scanned for sign change between adjacent strikes. "
            "Fix: evaluates GEX(spot+Δ) across a grid, finds exact zero crossing via "
            "linear interpolation. Per-strike IV uses linear skew model.")
        flip    = m.get('gex_flip_exact')
        radius  = m.get('gex_dyn_radius', 400)
        dist    = abs(m['spot'] - flip) if flip else None
        flipcol = '#FF0000' if (dist and dist < 50) else ('#FFA500' if (dist and dist < 100) else '#00FF00')
        st.markdown(
            f"| Field | Value |\n|---|---|\n"
            f"| Exact flip level | **{flip or 'None found in ±' + str(radius) + 'pt range'}** |\n"
            f"| Distance from spot | **{dist:.0f}pts** |\n" if dist else
            f"| Exact flip level | **{flip or 'None'}** |\n"
            f"| Dynamic radius | **±{radius}pts** |\n"
            f"| Net GEX | **{m['net_gex']:+.1f}M** — {m['gex_mode']} |")
        if m.get('gamma_flip_msg'):
            st.warning(m['gamma_flip_msg'])

    # ── Improvement 3: Delta flow ─────────────────────────────────────────
    with st.expander("3️⃣ Net Option Delta Flow", expanded=False):
        st.caption(
            "New: computes aggregate directional delta demand from all option buyers/sellers. "
            "Positive = market absorbing bullish delta demand → dealers must buy futures.")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Norm Signal",  f"{m.get('delta_flow_norm', 0):+.2f}")
        c2.metric("Bull δ (M)",   f"{m.get('delta_bull', 0):+.3f}")
        c3.metric("Bear δ (M)",   f"{m.get('delta_bear', 0):+.3f}")
        c4.metric("Imbalance",    f"{m.get('delta_imbalance', 0):+.1f}%")
        st.info(m.get('delta_flow_state', '—'))

    # ── Improvement 4: Liquidity vacuum ───────────────────────────────────
    with st.expander("4️⃣ Liquidity Vacuum", expanded=False):
        st.caption(
            "Detects OI gaps — strikes with < 25% of median chain OI. "
            "In a vacuum: no hedgers to absorb directional flow → moves accelerate.")
        vacuum_panel(m)
        zones = m.get('vacuum_zones', [])
        if zones:
            import pandas as _pd2
            df_z = _pd2.DataFrame([{'Start': lo, 'End': hi, 'Width': hi - lo}
                                    for lo, hi in zones])
            st.dataframe(df_z, hide_index=True, width='stretch')

    # ── Improvement 5: Cascade ────────────────────────────────────────────
    with st.expander("5️⃣ Hedging Cascade Detector", expanded=True):
        st.caption(
            "Cascade = short gamma + directional flow + trapped sellers. "
            "HedgeFlow = net_gex × spot_velocity × dealer_position. "
            "When large: dealers forced to hedge in same direction as market = feedback loop.")
        cascade_panel(m)
        c1, c2, c3 = st.columns(3)
        c1.metric("Hedge Flow",      f"{m.get('hedge_flow_norm', 0):+.2f}", m.get('hedge_flow_state', '—')[:20])
        c2.metric("δ Imbalance",     f"{m.get('imbalance_ratio', 0):.2f}×", m.get('imbalance_state', '—')[:20])
        c3.metric("Cascade Strength", f"{m.get('cascade_strength', 0)}/3",
                  m.get('cascade_type') or 'none')


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — Market Control Dashboard
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=3)
def market_control_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for data…")
        return
    _, m = run(raw, lookback, DB_PATH)
    if not m:
        return

    st.subheader("🎯 Market Control Dashboard")
    st.caption(
        "Combines dealer delta + gamma regime + futures flow + OI flow "
        "into a single BUYERS vs SELLERS dominance reading.")

    # Top metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Control Score",  f"{m['mctrl_score']:.1f}/10")
    c2.metric("Dealer δ (M)",   f"{m['dealer_delta_M']:+.2f}", m['dealer_pressure'][:20])
    c3.metric("Net GEX (M)",    f"{m['net_gex']:+.1f}", m['gex_mode'].split('(')[0])
    c4.metric("Futures Signal", f"{m['futures_signal']:+.2f}",
              "✅ live" if m['futures_available'] else "⚪ no data")
    c5.metric("Flow State",     m['flow_state'][:20])

    st.markdown("---")
    market_control_panel(m)

    st.markdown("---")
    st.subheader("Component Breakdown")
    comps = m.get('mctrl_components', {})
    comp_rows = [{'Component': k.upper(), 'Score': round(v['score'], 2), 'Signal': v['label']}
                 for k, v in comps.items()]
    if comp_rows:
        cdf = pd.DataFrame(comp_rows)
        def _score_color(v):
            if v > 0:   return 'color: #7FFF00'
            if v < 0:   return 'color: #FF6347'
            return 'color: #888888'
        st.dataframe(cdf.style.map(_score_color, subset=['Score']),
                     width='stretch', hide_index=True)

    st.markdown("---")
    st.subheader("Directional Bias Detail")
    st.info(m.get('directional_bias', '—'))


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — Hero-Zero Engine  (dedicated expiry-day view)
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=5)
def hero_zero_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for data…")
        return
    _, m = run(raw, lookback, DB_PATH)
    if not m:
        return

    st.subheader("💥 Hero-Zero Engine")
    st.caption(
        "Dedicated expiry-day module. Detects the exact structural setup behind "
        "₹20 → ₹200 moves. All 5 components must align for ignition.")

    # ── Phase context — most important context for hero-zero ─────────────
    _hz_c1, _hz_c2 = st.columns([1.2, 1])
    with _hz_c1:
        market_phase_panel(m)
    with _hz_c2:
        stability_status_bar(m)
        directional_probability_panel(m)

    st.markdown("---")

    # ── FINAL UPGRADE: Flow Persistence — pressure accumulation view ──────
    # This is what makes hero-zero detection early rather than reactive.
    # If PRESSURE = PRIMED/EXPLOSIVE and compression_age > 15min:
    # the hero-zero setup was building long before the score hit ARMED.
    flow_persistence_panel(m)

    st.markdown("---")

    # Primary score
    hero_zero_conditions_panel(m)
    hero_zero_panel(m)

    # Ignition alert banner
    if m.get('hz_ignition'):
        st.error(
            f"🔥 IGNITION CONFIRMED — {m['hz_direction']} to {m['hz_target'] or '?'}  "
            f"| Score: {m['hz_score']:.0f} | Theta ×{m['hz_theta_mult']:.1f}")

    st.markdown("---")

    # Sub-detector breakdown
    hero_zero_detail_panel(m)

    st.markdown("---")

    # Cascade FSM state
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("**Cascade State Machine**")
        cascade_state_panel(m)
        st.caption("FSM prevents false triggers — requires conditions to persist across multiple frames.")

    with col2:
        st.markdown("**Move Probability**")
        mp = m.get('move_prob', 0)
        conf = m.get('move_confidence', '—')
        dir_ = m.get('move_direction', '—')
        st.metric("Move Probability", f"{mp:.0f}%", f"{conf} — {dir_}")
        for d in m.get('move_drivers', [])[:4]:
            st.caption(d)

    # Hedge pressure curve
    import pandas as _pd
    hp_df = m.get('hp_curve_df', _pd.DataFrame())
    if hp_df is not None and hasattr(hp_df, 'empty') and not hp_df.empty:
        try:
            import plotly.graph_objects as go
            spot = m['spot']
            near = hp_df[abs(hp_df['strike'] - spot) <= 400] if not hp_df.empty else hp_df
            colors = ['#00FF00' if d == 'BULL' else '#FF4B4B'
                      for d in near.get('direction', ['BULL'] * len(near))]
            fig = go.Figure(go.Bar(
                x=near['strike'], y=near['pressure'].abs(),
                marker_color=colors,
                hovertemplate='Strike:%{x}<br>Pressure:%{y:.3f}M/pt<extra></extra>'
            ))
            fig.add_vline(x=spot, line_dash='dash', line_color='white',
                          annotation_text='SPOT')
            fig.update_layout(
                title='Dealer Hedge Pressure (dHedge/dSpot per strike)',
                paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
                font=dict(color='#CCC', size=11), height=250,
                margin=dict(l=40, r=20, t=40, b=30),
                xaxis=dict(gridcolor='#222'), yaxis=dict(title='M/pt', gridcolor='#222'),
            )
            st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
            st.caption("Green = dealers must BUY futures on up-move (amplifying ↑). "
                       "Red = dealers must SELL futures on up-move. "
                       "Tall bars near spot = explosive feedback zone.")
        except ImportError:
            st.dataframe(hp_df.head(20), width='stretch')

    # Gamma convexity — final layer
    st.markdown("---")
    st.markdown("#### dGamma/dSpot — Dealer Gamma Convexity")
    gamma_convexity_panel(m)
    st.caption(
        "This is the acceleration layer. hedge_pressure tells you HOW MUCH dealers must hedge. "
        "dGamma/dSpot tells you whether that hedging requirement is GROWING with each tick — "
        "which is what separates a normal move from a vertical candle.")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 6 — Deep Analysis  (merged: Market Control + Institutional + Regime)
# Problem 5 fix: consolidate 3 rarely-used tabs into 1 deep-dive tab
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=5)
def deep_analysis_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for data…")
        return
    _, m = run(raw, lookback, DB_PATH)
    if not m:
        return

    st.subheader("🔬 Deep Analysis")

    sub = st.tabs(["🎮 Market Control", "⚡ Institutional", "📊 Regime Stats", "🎯 Calibration", "🌍 Events"])

    with sub[0]:
        st.markdown("#### Market Control Dashboard")
        market_control_panel(m)

    with sub[1]:
        st.markdown("#### Institutional Intelligence (5 Structural Upgrades)")
        st.caption("Price-OI classification, true gamma flip, delta flow, vacuum, cascade.")
        # 5-metric row
        c1, c2, c3, c4, c5 = st.columns(5)
        fb = m.get('dealer_flow_breakdown', {})
        dom_oi = max(fb, key=fb.get) if fb else '—'
        c1.metric("OI Flow",      dom_oi.replace('_', ' '))
        c2.metric("Gamma Flip",   str(m.get('gex_flip_exact') or '—'))
        c3.metric("Delta Flow",   f"{m.get('delta_flow_norm', 0):+.2f}", m.get('delta_flow_state', '—')[:15])
        c4.metric("Vacuum",
                  "⚡ IN VAC" if m.get('in_vacuum') else f"{m.get('vacuum_severity',0)*100:.0f}% gaps")
        c5.metric("Cascade",
                  f"{'🚨 ACTIVE' if m.get('is_cascade') else str(m.get('cascade_strength',0))+'/3'}",
                  m.get('cascade_fsm_state', '—'))
        st.markdown("---")
        st.markdown("#### Dealer Regime")
        dealer_regime_panel(m)
        st.markdown("#### Skew Dynamics")
        skew_dynamics_panel(m)
        st.markdown("#### dGamma/dSpot — Convexity Regime")
        gamma_convexity_panel(m)
        st.markdown("#### Liquidity Map")
        liquidity_map_panel(m)

    with sub[2]:
        st.markdown("#### Regime Learning & Stats")
        try:
            regime_stats_tab()
        except Exception:
            st.info("Regime stats available after session accumulates data.")

    with sub[3]:  # P6: Calibration
        st.markdown("#### Signal Probability Calibration")
        st.caption(
            "The calibration engine tracks every signal snapshot and compares it to "
            "what actually happened 15 minutes later. After 20 samples, the dashboard "
            "shows empirically-grounded hit rates rather than naive model probabilities. "
            "Before that: prior estimates only.")
        calibration_panel(m, db_path=DB_PATH)
        st.markdown("---")
        st.markdown("#### Model Uncertainty Ranges")
        st.caption(
            "GEX and dealer delta are estimates from exchange OI only. "
            "OTC options, block trades, and cross-market positions are invisible. "
            "These ranges show the conservative uncertainty band.")
        dealer_uncertainty_panel(m)
        st.markdown("---")
        move_interaction_panel(m)

    with sub[4]:  # P7: Events
        st.markdown("#### Event Awareness")
        st.caption(
            "NSE expiry calendar is computed automatically from weekday arithmetic "
            "— always accurate. RBI / US CPI dates come from a pre-populated list "
            "that requires quarterly manual update. Cannot detect intraday news.")
        event_awareness_panel(m)
        st.markdown("---")
        st.markdown("**Add custom events:**")
        _ev_input = st.text_input(
            "Event  (format: YYYY-MM-DD | Label)",
            placeholder="2025-06-06 | RBI MPC Meeting",
            key="custom_event_input")
        if _ev_input and '|' in _ev_input:
            _parts = [p.strip() for p in _ev_input.split('|')]
            if len(_parts) >= 2:
                _existing = st.session_state.get('custom_events', [])
                _new_ev = {'date': _parts[0], 'label': _parts[1]}
                if _new_ev not in _existing:
                    _existing.append(_new_ev)
                    st.session_state['custom_events'] = _existing
                    st.success(f"Added: {_new_ev['label']} on {_new_ev['date']}")
        _ce = st.session_state.get('custom_events', [])
        if _ce:
            st.markdown("**Custom events:**")
            for _i, _ev in enumerate(_ce):
                _c1, _c2 = st.columns([4, 1])
                _c1.markdown(f"📅 {_ev.get('date','?')} — {_ev.get('label','?')}")
                if _c2.button("Remove", key=f"rm_ev_{_i}"):
                    _ce.pop(_i)
                    st.session_state['custom_events'] = _ce
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — GEX Tab (heatmap + sensitivity + history)
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=5)
def gex_tab():
    raw = load_all(DB_PATH, lookback)
    if raw['options_df'].empty or raw['spot_df'].empty:
        st.warning("⏳ Waiting for data…")
        return
    _, m = run(raw, lookback, DB_PATH)
    if not m:
        return

    st.subheader("🌡️ Gamma Exposure Analysis")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Net GEX",    f"{m['net_gex']:+.1f}M",   m['gex_mode'].split('(')[0])
    c2.metric("GEX Wall",   str(m['gex_wall']),         "📌 PINNING" if m['gex_wall_pin'] else "")
    c3.metric("Gamma Flip", str(m.get('gex_flip') or '—'))
    c4.metric("Max Sensitivity", str(m.get('sens_max_strike') or '—'),
              f"Chain: {m.get('sens_chain_total', 0):+.3f}M/pt")

    expl = m.get('sens_explosion_zones', [])
    if expl:
        st.markdown(
            f"<div class='compression-alert'>⚡ EXPLOSION ZONES (top 20% sensitivity): "
            f"{', '.join(str(z) for z in expl[:8])}</div>",
            unsafe_allow_html=True)

    subtab1, subtab2, subtab3 = st.tabs(["Current GEX", "Sensitivity Map", "History (30 min)"])
    with subtab1:
        gex_heatmap(m)
    with subtab2:
        sensitivity_panel(m)
        st.caption(
            "Purple dotted lines = explosion zones (sensitivity > p80). "
            "Magenta dashed = max sensitivity strike. "
            "Green = gamma increases if spot moves up. Red = gamma decreases.")
    with subtab3:
        gex_history_panel()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — Backtest (real replay)
# ─────────────────────────────────────────────────────────────────────────────
def backtest_tab():
    st.header("📊 Real-Replay Backtest")
    st.caption(
        "Runs the **full orchestrator** on each historical minute. "
        "Uses real trade signals — no proxy. "
        "PnL: dOption ≈ δ·dSpot + ½γ·dSpot² + ν·dIV − θ·dt")

    st.info(
        "⚡ **Performance note:** Full orchestrator replay is CPU-intensive. "
        "For 1 day of data (~375 snapshots) expect 30–120 seconds. "
        "Use `max_snapshots` slider to test on a subset first.")

    with st.expander("⚙️ Parameters", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        lb      = c1.slider("Lookback (mins)", 5, 60, 15)
        qt      = c2.slider("Quality threshold", 40, 85, 60)
        hm      = c3.slider("Hold (mins)", 5, 60, 15)
        maxsnap = c4.slider("Max snapshots", 50, 2000, 200,
                             help="Limit for fast testing. Set to 2000 for full day.")

    if st.button("▶ Run Real-Replay Backtest", type="primary"):
        from backtest.backtester import run_backtest
        prog = st.progress(0, text="Initialising…")
        status = st.empty()
        with st.spinner(f"Replaying up to {maxsnap} snapshots…"):
            t0  = time.time()
            res = run_backtest(DB_PATH, lb, qt, hm, max_snapshots=maxsnap)
            elapsed = time.time() - t0
        prog.progress(100, text=f"Done in {elapsed:.1f}s")

        if 'error' in res:
            st.error(res['error'])
            return

        if res['n'] == 0:
            st.warning(res.get('note', 'No trades found.'))
            st.json({'snapshots': res.get('snapshots', 0),
                     'skipped':   res.get('skipped', 0),
                     'errors':    res.get('errors', 0)})
            return

        # Summary metrics
        st.markdown("### Results")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Trades",         res['n'])
        c2.metric("Win Rate",        f"{res['win_rate']}%",
                  "🟢" if res['win_rate'] > 55 else ("🔴" if res['win_rate'] < 45 else "🟡"))
        c3.metric("Avg Return",      f"{res['avg_return']}%")
        c4.metric("Max Drawdown",    f"{res['max_drawdown']}%")
        c5.metric("Total PnL",       f"{res['total_pnl']}%")
        st.caption(
            f"Avg hold: {res['avg_hold']} min  |  "
            f"Avg delta: {res['avg_delta']}  |  "
            f"Snapshots: {res['snapshots']} (skipped: {res['skipped']}, errors: {res['errors']})  |  "
            f"Elapsed: {elapsed:.1f}s")

        # Equity curve
        if res['n'] > 1:
            import plotly.graph_objects as go
            df_t = pd.DataFrame(res['trades'])
            fig = go.Figure()
            cumul = df_t['pnl_pct'].cumsum()
            colors = ['#00CC44' if v >= 0 else '#FF3333'
                      for v in df_t['pnl_pct']]
            fig.add_trace(go.Bar(x=list(range(len(df_t))), y=df_t['pnl_pct'],
                                 marker_color=colors, name='Trade PnL'))
            fig.add_trace(go.Scatter(x=list(range(len(df_t))), y=cumul,
                                     mode='lines', line=dict(color='white', width=2),
                                     name='Cumulative'))
            fig.update_layout(paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
                              font=dict(color='#CCC'), height=280,
                              margin=dict(l=40, r=20, t=20, b=30),
                              xaxis=dict(gridcolor='#333'),
                              yaxis=dict(title='PnL %', gridcolor='#333'))
            st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))

        # Breakdown tables
        bcol1, bcol2, bcol3 = st.columns(3)
        for col, label, data in [
            (bcol1, "By Exit Reason",  res.get('by_exit', {})),
            (bcol2, "By Direction",    res.get('by_dir', {})),
            (bcol3, "By Regime",       res.get('by_regime', {})),
        ]:
            col.markdown(f"**{label}**")
            if data:
                rows = [{'Key': k, 'Count': v.get('count', 0),
                         'Avg %': round(v.get('mean', 0), 1)}
                        for k, v in data.items()]
                bdf = pd.DataFrame(rows)
                col.dataframe(bdf.style.map(
                    lambda v: 'color:#7FFF00' if isinstance(v, (int, float)) and v > 0
                    else ('color:#FF6347' if isinstance(v, (int, float)) and v < 0 else ''),
                    subset=['Avg %']), hide_index=True)

        # Full trade log
        with st.expander("📋 Full trade log"):
            df_t2 = pd.DataFrame(res['trades'])
            if not df_t2.empty:
                st.dataframe(df_t2.style.map(
                    lambda v: 'color:#00FF00' if isinstance(v, (int, float)) and v > 0
                    else ('color:#FF4B4B' if isinstance(v, (int, float)) and v < 0 else ''),
                    subset=['pnl_pct', 'pnl_pts']),
                    width='stretch')


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — Regime Stats
# ─────────────────────────────────────────────────────────────────────────────
def regime_stats_tab():
    from signals.regime_learning import _dists
    st.header("🔬 Regime Learning Diagnostics")
    st.caption(
        "Rolling distributions auto-calibrate thresholds intra-session. "
        "p90 = top-10% threshold (used for 'aggressive flow'). "
        "p80 = strong OI threshold.")

    if not _dists:
        st.info("No distribution data yet. Run the live dashboard first (need ≥50 observations).")
        return

    for name, dist in _dists.items():
        buf = list(dist._buf)
        if len(buf) < 5:
            continue
        import numpy as np
        p50 = np.percentile(buf, 50)
        p80 = np.percentile(buf, 80)
        p90 = np.percentile(buf, 90)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric(name.upper(), f"n={len(buf)}")
        c2.metric("p50 (median)", f"{p50:.2f}")
        c3.metric("p80 (strong)", f"{p80:.2f}")
        c4.metric("p90 (aggressive)", f"{p90:.2f}")
        c5.metric("latest", f"{buf[-1]:.2f}")

        try:
            import plotly.express as px
            import pandas as _pd
            fig = px.histogram(
                _pd.DataFrame({'value': buf}), x='value', nbins=30,
                title=f"{name} distribution",
                color_discrete_sequence=['#00C8FF'])
            fig.add_vline(x=p80, line_dash='dash', line_color='#FFA500',
                          annotation_text='p80')
            fig.add_vline(x=p90, line_dash='dash', line_color='#FF4B4B',
                          annotation_text='p90')
            fig.update_layout(paper_bgcolor='#0E1117', plot_bgcolor='#0E1117',
                              font=dict(color='#CCC'), height=180,
                              margin=dict(l=20, r=10, t=30, b=20),
                              showlegend=False)
            st.plotly_chart(fig, width='stretch', key=str(uuid.uuid4()))
        except Exception:
            st.line_chart(pd.DataFrame({'value': buf}))

        st.markdown("---")


# ── Render tabs ───────────────────────────────────────────────────────────────
with tabs[0]: focus_tab()
with tabs[1]: live_tab()
with tabs[2]: hero_zero_tab()
with tabs[3]: impact_engine_tab()
with tabs[4]: gex_tab()
with tabs[5]: backtest_tab()
with tabs[6]: deep_analysis_tab()