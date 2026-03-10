"""
SIGNAL LAYER — directional_bias.py
Responsibility: Convert market state into a directional bias label.
Uses weighted score from signal_score, plus edge count for label.
"""


def compute_directional_bias(
    net_w_oi: float, spot_trend: str, pain: str,
    trapped_ce: bool, trapped_pe: bool,
    skew_25d: float, iv_direction: str, STRONG_OI: float,
    gex_mode: str, vwap_above: bool,
    max_pain: int, spot: float, is_expiry: bool,
    struct: dict, dealer_delta_M: float, flow_state: str,
    ctrl_score: int, weighted_score: float, confidence: float,
) -> str:
    call_score = put_score = 0

    # OI flow
    if net_w_oi < 0:   put_score  += 2
    else:              call_score += 2

    # VWAP
    if vwap_above: call_score += 1
    else:          put_score  += 1

    # Pain trade
    if "SQUEEZE"     in pain: call_score += 2
    if "LIQUIDATION" in pain: put_score  += 2

    # Trapped sellers
    if trapped_ce: call_score += 3
    if trapped_pe: put_score  += 3

    # Dealer delta
    if dealer_delta_M < 0:  call_score += 1
    else:                   put_score  += 1

    # Flow
    if "AGGRESSIVE" in flow_state and net_w_oi < 0: put_score  += 1
    if "AGGRESSIVE" in flow_state and net_w_oi > 0: call_score += 1

    # 25d skew
    if skew_25d > 0.03:    put_score  += 1
    elif skew_25d < -0.03: call_score += 1

    # IV direction
    if "EXPANDING" in iv_direction:
        call_score += 1; put_score += 1
    elif "CRUSHING" in iv_direction:
        call_score = max(call_score - 1, 0)
        put_score  = max(put_score  - 1, 0)

    # GEX
    if "SHORT" in gex_mode:
        if net_w_oi < 0: put_score  += 1
        else:            call_score += 1
    elif "LONG" in gex_mode:
        call_score = max(call_score - 1, 0)
        put_score  = max(put_score  - 1, 0)

    # Max pain gravity (expiry only)
    if is_expiry and max_pain > 0:
        dist = spot - max_pain
        if   dist >  100: put_score  += 1
        elif dist < -100: call_score += 1

    # Market structure
    if struct.get('above_pdh'): call_score += 1
    if struct.get('below_pdl'): put_score  += 1
    if struct.get('above_orh'): call_score += 1
    if struct.get('below_orl'): put_score  += 1

    call_score = max(call_score, 0)
    put_score  = max(put_score,  0)
    conf_str   = f" | conf {confidence*100:.0f}%"

    if   call_score >= 5 and call_score > put_score + 1:
        return f"🟢 BUY CALL (edge {call_score}){conf_str}"
    elif put_score  >= 5 and put_score  > call_score + 1:
        return f"🔴 BUY PUT  (edge {put_score}){conf_str}"
    elif call_score > put_score:
        return f"📈 CALL LEAN ({call_score} vs {put_score}){conf_str}"
    elif put_score  > call_score:
        return f"📉 PUT LEAN  ({put_score} vs {call_score}){conf_str}"
    return f"⚪ NO CLEAR EDGE{conf_str}"
