"""
DASHBOARD LAYER — state.py
Manages Streamlit session state initialisation.
"""
import streamlit as st
from signals.flow_memory import FlowMemory

DEFAULTS = {
    'conf_history':          [],
    'iv_history':            [],
    'straddle_history':      [],
    'spot_range_history':    [],
    'oi_chg_history':        [],
    'velocity_history':      [],
    'slope_history':         [],
    'dealer_delta_history':  [],
    'gex_history':           [],   # list of {ts, gex_by_strike, spot} for heatmap history
    'stagnation_counter':    0,
    'compression_counter':   0,
    'last_net_oi':           0,
    'cooldown_until':        0,
    'prev_trade_state':      False,
    'prev_mode':             'Wait',
    'last_iv_write':         0,
    'logger_setup':          False,
    # Flow persistence engine  (Final Structural Upgrade)
    'flow_memory':           None,   # set to FlowMemory() on first access
    # Custom events (P7)
    'custom_events':         [],
    'india_vix':             0.0,
    'prev_india_vix':        0.0,
    'sgx_gap_pct':           0.0,
}

def init_state():
    for k, v in DEFAULTS.items():
        if k not in st.session_state:
            st.session_state[k] = v
    # FlowMemory needs a proper object, not None
    if not isinstance(st.session_state.get('flow_memory'), FlowMemory):
        st.session_state['flow_memory'] = FlowMemory()


def push_history(key: str, value, max_len: int = 400):
    st.session_state[key].append(value)
    if len(st.session_state[key]) > max_len:
        st.session_state[key].pop(0)
