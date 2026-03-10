"""
BACKTEST LAYER — session_mock.py
Responsibility: Provide a drop-in replacement for st.session_state
so the real orchestrator layers can run in a backtest loop
without any Streamlit dependency.

Design:
    BacktestSession is a plain dict-like object.
    We monkey-patch the imports in dashboard/* to use it
    instead of st.session_state + push_history.
    The patch is applied once per backtest run and reverted after.
"""
import sys
import types
from collections import deque
from typing import Any
from dashboard.state import DEFAULTS


class BacktestSession:
    """
    Mimics st.session_state attribute access for backtest replay.
    Holds a fresh copy of all DEFAULTS each time it is reset.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        for k, v in DEFAULTS.items():
            # Deep-copy mutable defaults
            object.__setattr__(self, k, list(v) if isinstance(v, list) else v)

    def __getattr__(self, key):
        # Return 0/[] for any undeclared key (safety valve)
        if key.startswith('_'):
            raise AttributeError(key)
        return DEFAULTS.get(key, 0)

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)


def make_push_history(session: BacktestSession):
    """
    Returns a push_history function that writes into
    the BacktestSession instead of st.session_state.
    """
    def push_history(key: str, value: Any, max_len: int = 400):
        lst = getattr(session, key, [])
        if not isinstance(lst, list):
            lst = []
        lst.append(value)
        if len(lst) > max_len:
            lst.pop(0)
        setattr(session, key, lst)
    return push_history


class StreamlitMock:
    """
    Minimal st mock: only session_state is needed by orchestrator layers.
    Other st.* calls (metrics, markdown etc.) are silently ignored.
    """
    def __init__(self, session: BacktestSession):
        self.session_state = session

    def __getattr__(self, name):
        # Any st.* call not explicitly defined → no-op callable
        return lambda *a, **kw: None


class OrchestratorPatch:
    """
    Context manager that patches Streamlit + push_history in all
    dashboard modules so they use BacktestSession.
    Reverts on exit so normal Streamlit operation is unaffected.
    """
    def __init__(self, session: BacktestSession):
        self._session   = session
        self._st_mock   = StreamlitMock(session)
        self._originals = {}

    def __enter__(self):
        ph = make_push_history(self._session)

        # Modules that import st and/or push_history
        targets = [
            'dashboard.market_context',
            'dashboard.signal_engine',
            'dashboard.risk_engine',
            'dashboard.state',
        ]
        for mod_name in targets:
            mod = sys.modules.get(mod_name)
            if mod is None:
                continue
            # Patch st
            if hasattr(mod, 'st'):
                self._originals[f'{mod_name}:st'] = mod.st
                mod.st = self._st_mock
            # Patch push_history
            if hasattr(mod, 'push_history'):
                self._originals[f'{mod_name}:push_history'] = mod.push_history
                mod.push_history = ph
        return self

    def __exit__(self, *args):
        for key, orig in self._originals.items():
            mod_name, attr = key.rsplit(':', 1)
            mod = sys.modules.get(mod_name)
            if mod:
                setattr(mod, attr, orig)
        self._originals.clear()
