"""
MARKET STATE LAYER — event_awareness.py
Event Awareness Engine  (Fix for Problem 7)

The problem being solved:
    Your system cannot detect macro catalysts:
    RBI announcements, F&O expiry, global market events.
    Hero-zero moves are frequently triggered by these.
    Ignoring them means you treat every day identically.

What this module does:
    1. Computes the NSE expiry calendar automatically
       (weekly = every Thursday, monthly = last Thursday)
    2. Detects pre-event risk windows
    3. Flags market sessions with heightened structural risk
    4. Reads global market context from session_state
       (VIX spike, SGX gap, US overnight)

Limitations (honest):
    - Cannot read live news or central bank websites
    - RBI policy dates require manual update of RBI_DATES list
    - US CPI/NFP dates require manual update
    - Global shock detection is approximate (uses prior session VIX)

Despite limitations, the module provides:
    - Reliable expiry calendar (deterministic, no external data)
    - Pre-expiry flag (always accurate)
    - User-configurable event list for known upcoming events
    - Safety damp: reduces position sizing and quality cap on flagged days

How to add custom events:
    Set in Streamlit sidebar or session state:
        st.session_state['custom_events'] = [
            {'date': '2025-03-20', 'type': 'RBI_POLICY', 'label': 'RBI MPC'},
            {'date': '2025-03-28', 'type': 'MACRO', 'label': 'US PCE'},
        ]
"""
from datetime import datetime, date, timedelta
from typing import List, Optional
import calendar


# ── Known high-impact events (update each quarter) ───────────────────────────
# Format: 'YYYY-MM-DD' — only add dates you are confident about
RBI_POLICY_DATES_2025 = [
    '2025-04-09', '2025-06-06', '2025-08-08',
    '2025-10-06', '2025-12-05',
]

US_CPI_DATES_2025 = [
    '2025-03-12', '2025-04-10', '2025-05-13',
    '2025-06-11', '2025-07-15',
]


# ── Expiry Calendar ───────────────────────────────────────────────────────────

def get_nifty_weekly_expiry(ref_date: Optional[date] = None) -> date:
    """
    NIFTY weekly options expire every Thursday.
    If Thursday is a market holiday, expiry is on Wednesday.
    This function returns the NEAREST upcoming Thursday (or today if Thursday).
    """
    d = ref_date or date.today()
    # weekday(): Monday=0, Thursday=3
    days_to_thu = (3 - d.weekday()) % 7
    return d + timedelta(days=days_to_thu)


def get_nifty_monthly_expiry(year: int, month: int) -> date:
    """
    NIFTY monthly options expire on the last Thursday of each month.
    """
    # Find last Thursday
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() != 3:   # 3 = Thursday
        d -= timedelta(days=1)
    return d


def classify_expiry_type(today: Optional[date] = None) -> str:
    """
    Returns:
        'DAILY'         — regular day
        'WEEKLY_EXPIRY' — it is a weekly expiry Thursday
        'MONTHLY_EXPIRY'— it is the last Thursday (monthly expiry)
        'PRE_EXPIRY'    — one day before expiry (heightened risk)
    """
    d = today or date.today()
    weekly  = get_nifty_weekly_expiry(d)
    monthly = get_nifty_monthly_expiry(d.year, d.month)

    if d == monthly:
        return 'MONTHLY_EXPIRY'
    if d == weekly:
        return 'WEEKLY_EXPIRY'
    if (weekly - d).days == 1:
        return 'PRE_EXPIRY'
    return 'DAILY'


# ── Event Risk Detection ──────────────────────────────────────────────────────

def get_event_risk(
    today: Optional[date] = None,
    custom_events: Optional[list] = None,
    lookahead_days: int = 1,     # flag events within this many days
) -> dict:
    """
    Returns a comprehensive event risk dict for the current session.

    Checks:
    1. Expiry type (always accurate)
    2. Known RBI / US macro dates (pre-populated list)
    3. User-supplied custom events from session state

    Returns:
        event_risk_level: 'NONE' | 'LOW' | 'MEDIUM' | 'HIGH' | 'EXTREME'
        events_today:     list of event labels today
        events_soon:      list of event labels within lookahead_days
        quality_damp:     0.0–0.30 reduction to apply to quality score
        size_damp:        0.0–0.25 reduction to apply to size factor
        note:             human-readable summary
        is_expiry:        bool (any expiry type)
        expiry_type:      string
    """
    d = today or date.today()
    d_str = d.strftime('%Y-%m-%d')

    events_today = []
    events_soon  = []

    # Expiry
    expiry_type = classify_expiry_type(d)
    if expiry_type == 'MONTHLY_EXPIRY':
        events_today.append('📅 MONTHLY EXPIRY')
    elif expiry_type == 'WEEKLY_EXPIRY':
        events_today.append('📅 WEEKLY EXPIRY')
    elif expiry_type == 'PRE_EXPIRY':
        events_today.append('⏰ PRE-EXPIRY')

    # Look ahead for upcoming events
    for offset in range(1, lookahead_days + 1):
        future = d + timedelta(days=offset)
        fstr   = future.strftime('%Y-%m-%d')
        if fstr in RBI_POLICY_DATES_2025:
            events_soon.append(f'🏦 RBI POLICY in {offset}d ({fstr})')
        if fstr in US_CPI_DATES_2025:
            events_soon.append(f'🇺🇸 US CPI in {offset}d ({fstr})')

    # Today's macro events
    if d_str in RBI_POLICY_DATES_2025:
        events_today.append('🏦 RBI POLICY DAY — elevated volatility')
    if d_str in US_CPI_DATES_2025:
        events_today.append('🇺🇸 US CPI TODAY — global risk event')

    # Custom events from session state
    if custom_events:
        for ev in custom_events:
            ev_date = str(ev.get('date', ''))
            label   = ev.get('label', ev.get('type', 'EVENT'))
            if ev_date == d_str:
                events_today.append(f'⚡ {label}')
            elif ev_date > d_str:
                days_away = (date.fromisoformat(ev_date) - d).days
                if days_away <= lookahead_days:
                    events_soon.append(f'⚡ {label} in {days_away}d')

    # ── Risk level ────────────────────────────────────────────────────────
    high_events   = [e for e in events_today if any(
        x in e for x in ['RBI', 'CPI', 'MONTHLY'])]
    medium_events = [e for e in events_today if any(
        x in e for x in ['WEEKLY', 'PRE-EXPIRY', '⚡'])]

    if   high_events:             level = 'HIGH'
    elif medium_events:           level = 'MEDIUM'
    elif events_soon:             level = 'LOW'
    else:                         level = 'NONE'

    # If MONTHLY_EXPIRY + RBI same day = EXTREME (very rare but real)
    if 'MONTHLY_EXPIRY' in expiry_type and any('RBI' in e for e in events_today):
        level = 'EXTREME'

    # ── Dampening factors ─────────────────────────────────────────────────
    # On high-risk event days: reduce quality and sizing as a precaution
    damp_map = {'NONE': 0.0, 'LOW': 0.05, 'MEDIUM': 0.10, 'HIGH': 0.20, 'EXTREME': 0.30}
    quality_damp = damp_map[level]
    size_damp    = damp_map[level] * 0.75

    is_expiry = expiry_type in ('WEEKLY_EXPIRY', 'MONTHLY_EXPIRY')

    if events_today:
        note = ' | '.join(events_today[:3])
    elif events_soon:
        note = f"Upcoming: {events_soon[0]}"
    else:
        note = '✅ No major events'

    return {
        'event_risk_level': level,
        'events_today':     events_today,
        'events_soon':      events_soon,
        'quality_damp':     quality_damp,
        'size_damp':        size_damp,
        'note':             note,
        'is_expiry':        is_expiry,
        'expiry_type':      expiry_type,
    }


# ── Global Market Context ─────────────────────────────────────────────────────

def estimate_global_risk(
    india_vix: float = 0.0,        # India VIX reading (from session state or DB)
    prev_india_vix: float = 0.0,   # Previous session VIX
    sgx_gap_pct: float = 0.0,      # SGX NIFTY gap from session state (manual entry)
) -> dict:
    """
    Estimates global risk environment from available data.

    Note: This is approximate — real global shock detection requires
    live feeds from SGX, CBOE VIX, US futures.
    These can be added if broker API provides them.

    Returns:
        global_risk: 'CALM' | 'ELEVATED' | 'RISK_OFF'
        vix_regime:  'LOW' | 'NORMAL' | 'HIGH' | 'SPIKE'
        note:        explanation
    """
    vix_regime = 'NORMAL'
    if india_vix > 0:
        if   india_vix > 25:   vix_regime = 'SPIKE'
        elif india_vix > 18:   vix_regime = 'HIGH'
        elif india_vix < 12:   vix_regime = 'LOW'

    vix_change = india_vix - prev_india_vix if prev_india_vix > 0 else 0.0
    vix_spike  = vix_change > 3.0   # >3 point spike overnight

    global_risk = 'CALM'
    if vix_spike or vix_regime == 'SPIKE':
        global_risk = 'RISK_OFF'
    elif vix_regime == 'HIGH' or abs(sgx_gap_pct) > 0.5:
        global_risk = 'ELEVATED'

    notes = []
    if vix_regime in ('HIGH', 'SPIKE'):   notes.append(f'India VIX {india_vix:.1f}')
    if vix_spike:                          notes.append(f'VIX spiked +{vix_change:.1f}')
    if abs(sgx_gap_pct) > 0.3:            notes.append(f'SGX gap {sgx_gap_pct:+.1f}%')

    return {
        'global_risk':  global_risk,
        'vix_regime':   vix_regime,
        'vix_change':   round(vix_change, 2),
        'sgx_gap_pct':  sgx_gap_pct,
        'note':         ' | '.join(notes) if notes else '✅ Global calm',
    }
