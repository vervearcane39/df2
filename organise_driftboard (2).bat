@echo off
:: ============================================================
:: organise_driftboard.bat
:: Drop into driftboard2 and double-click.
:: ============================================================

echo.
echo [1/4] Creating folders...
mkdir backtest   2>nul
mkdir dashboard  2>nul
mkdir data       2>nul
mkdir features   2>nul
mkdir market     2>nul
mkdir risk       2>nul
mkdir signals    2>nul

echo [2/4] backtest\...
move "backtest___init__.py"       "backtest\__init__.py"
move "backtest_backtester.py"     "backtest\backtester.py"
move "backtest_session_mock.py"   "backtest\session_mock.py"

echo [2/4] dashboard\...
move "dashboard___init__.py"        "dashboard\__init__.py"
move "dashboard_cache.py"           "dashboard\cache.py"
move "dashboard_market_context.py"  "dashboard\market_context.py"
move "dashboard_orchestrator.py"    "dashboard\orchestrator.py"
move "dashboard_panels.py"          "dashboard\panels.py"
move "dashboard_risk_engine.py"     "dashboard\risk_engine.py"
move "dashboard_signal_engine.py"   "dashboard\signal_engine.py"
move "dashboard_state.py"           "dashboard\state.py"

echo [2/4] data\...
move "data___init__.py"   "data\__init__.py"
move "data_db_loader.py"  "data\db_loader.py"
move "data_db_utils.py"   "data\db_utils.py"

echo [2/4] features\...
move "features___init__.py"        "features\__init__.py"
move "features_delta_flow.py"      "features\delta_flow.py"
move "features_flow_features.py"   "features\flow_features.py"
move "features_futures.py"         "features\futures.py"
move "features_greeks.py"          "features\greeks.py"
move "features_structure.py"       "features\structure.py"
move "features_volatility.py"      "features\volatility.py"

echo [2/4] market\...
move "market___init__.py"            "market\__init__.py"
move "market_cascade.py"             "market\cascade.py"
move "market_control_dashboard.py"   "market\control_dashboard.py"
move "market_dealer_position.py"     "market\dealer_position.py"
move "market_dealer_regime.py"       "market\dealer_regime.py"
move "market_event_awareness.py"     "market\event_awareness.py"
move "market_gamma.py"               "market\gamma.py"
move "market_gamma_convexity.py"     "market\gamma_convexity.py"
move "market_hedge_pressure.py"      "market\hedge_pressure.py"
move "market_liquidity.py"           "market\liquidity.py"
move "market_liquidity_map.py"       "market\liquidity_map.py"
move "market_market_phase.py"        "market\market_phase.py"
move "market_migration.py"           "market\migration.py"
move "market_move_probability.py"    "market\move_probability.py"
move "market_sensitivity.py"         "market\sensitivity.py"

echo [2/4] risk\...
move "risk___init__.py"      "risk\__init__.py"
move "risk_trade_filter.py"  "risk\trade_filter.py"

echo [2/4] signals\...
move "signals___init__.py"           "signals\__init__.py"
move "signals_calibration.py"        "signals\calibration.py"
move "signals_compression.py"        "signals\compression.py"
move "signals_directional_bias.py"   "signals\directional_bias.py"
move "signals_flow_memory.py"        "signals\flow_memory.py"
move "signals_hero_zero.py"          "signals\hero_zero.py"
move "signals_probability_model.py"  "signals\probability_model.py"
move "signals_regime_learning.py"    "signals\regime_learning.py"
move "signals_signal_score.py"       "signals\signal_score.py"
move "signals_skew_dynamics.py"      "signals\skew_dynamics.py"
move "signals_stability_filter.py"   "signals\stability_filter.py"

echo [3/4] drift_dash.py and engine3.py stay in root - no move needed.

echo.
echo [4/4] Final structure:
tree /F
echo.
echo ============================================================
echo  Done. Now run:   streamlit run drift_dash.py
echo ============================================================
pause
