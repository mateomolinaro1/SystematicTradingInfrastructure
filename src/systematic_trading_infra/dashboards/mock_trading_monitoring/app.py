import time
import sys
import subprocess
import atexit
import signal
from pathlib import Path

import streamlit as st

from systematic_trading_infra.mock_trading.config import PROJECT_ROOT
from systematic_trading_infra.dashboards.mock_trading_monitoring.data_loader import (
    load_prices,
    load_orders,
    load_portfolio_value,
    load_weights,
)

# ---------------------------------------------------------------------
# Page config (MUST be first)
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="Mock Trading – Live Monitoring",
    layout="wide",
)

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
RUNNER_PATH = PROJECT_ROOT / "mock_trading" / "runner.py"
REFRESH_SECONDS = 15

st.sidebar.caption(f"Runner path: {RUNNER_PATH}")
st.sidebar.caption(f"Exists: {RUNNER_PATH.exists()}")

# ---------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------
if "runner_proc" not in st.session_state:
    st.session_state.runner_proc = None


# ---------------------------------------------------------------------
# Cleanup on dashboard exit
# ---------------------------------------------------------------------
def _shutdown_runner():
    proc = st.session_state.get("runner_proc")
    if proc and proc.poll() is None:
        print("🛑 Stopping runner...")
        proc.send_signal(signal.SIGTERM)


atexit.register(_shutdown_runner)

# ---------------------------------------------------------------------
# Sidebar – Runner control
# ---------------------------------------------------------------------
st.sidebar.header("Mock Trading Control")

if st.sidebar.button("▶ Start Live Mock Trading"):
    if st.session_state.runner_proc is None or st.session_state.runner_proc.poll() is not None:
        proc = subprocess.Popen(
            [sys.executable, str(RUNNER_PATH)],
            cwd=str(PROJECT_ROOT),
        )
        st.session_state.runner_proc = proc
        st.sidebar.success("Mock trading started")
    else:
        st.sidebar.warning("Runner already running")

# ---------------------------------------------------------------------
# Sidebar – Settings
# ---------------------------------------------------------------------
st.sidebar.divider()
refresh = st.sidebar.checkbox("Auto-refresh", value=True)
st.sidebar.caption(f"Refresh every {REFRESH_SECONDS}s")

# ---------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------
st.title("📈 Live Paper Trading – Mock Strategy Monitoring")

prices = load_prices()
orders = load_orders()
portfolio = load_portfolio_value()
weights = load_weights()

# Prices
st.subheader("📊 Historical Prices")
if prices.empty:
    st.info("No prices yet.")
else:
    st.dataframe(prices, width="stretch")

st.divider()

# Orders & Portfolio
left, right = st.columns(2)

with left:
    st.subheader("🧾 Orders")
    if orders.empty:
        st.info("No orders yet.")
    else:
        st.dataframe(orders, width="stretch")

with right:
    st.subheader("💰 Portfolio Value")
    if portfolio.empty:
        st.info("No portfolio value yet.")
    else:
        st.dataframe(portfolio, width="stretch")

st.divider()

# Weights
st.subheader("⚖️ Portfolio Weights")
if weights.empty:
    st.info("No weights yet.")
else:
    st.dataframe(weights, width="stretch")

# ---------------------------------------------------------------------
# Auto refresh
# ---------------------------------------------------------------------
if refresh:
    time.sleep(REFRESH_SECONDS)
    st.rerun()
