import streamlit as st
from systematic_trading_infra.dashboards.paper_trading_monitoring.controller import (
    LiveMonitoringController
)

st.set_page_config(
    page_title="Live Paper Trading Monitoring",
    layout="wide"
)

st.title("📡 Live Paper Trading Strategy Monitoring - LO CSMOM Russell 1000")

# ---------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------

controller = LiveMonitoringController()

@st.cache_data(show_spinner=False)
def load_data():
    return controller.load_all()

# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------

with st.spinner("Loading live data..."):
    data = load_data()

prices = data["prices"]
orders = data["orders"]
portfolio_value = data["portfolio_value"]
weights = data["weights"]

# ---------------------------------------------------------------------
# Prices (full width)
# ---------------------------------------------------------------------

st.subheader("Historical IB Prices")

st.dataframe(
    prices,
    width="stretch",
    height=400
)

# ---------------------------------------------------------------------
# Orders + Portfolio value
# ---------------------------------------------------------------------

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Orders")
    st.dataframe(
        orders,
        width="stretch",
        height=350
    )

with col2:
    st.subheader("Portfolio Value")

    if not portfolio_value.empty:
        st.line_chart(
            portfolio_value,
            width="stretch",
            height=350
        )
    else:
        st.info("No portfolio value data available")

# ---------------------------------------------------------------------
# Weights (full width)
# ---------------------------------------------------------------------

st.subheader("Weights")

st.dataframe(
    weights,
    width="stretch",
    height=350
)
