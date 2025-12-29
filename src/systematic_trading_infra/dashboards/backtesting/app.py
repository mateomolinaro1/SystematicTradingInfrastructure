import streamlit as st
from dotenv import load_dotenv
import numpy as np
import pandas as pd

load_dotenv()

@st.cache_data(show_spinner=False)
def run_backtest_cached(config: dict) -> dict:
    from systematic_trading_infra.dashboards.backtesting.config import BacktestConfig
    from systematic_trading_infra.dashboards.backtesting.controller import BacktestController

    cfg = BacktestConfig(**config)
    controller = BacktestController(cfg)
    return controller.run()

# --- Session state ---
if "results" not in st.session_state:
    st.session_state["results"] = None
if "last_config" not in st.session_state:
    st.session_state["last_config"] = None

# --- Page ---
st.set_page_config(layout="wide")
st.title("Systematic Strategy Backtester")

# --- Sidebar ---
st.sidebar.header("Strategy Parameters")

strategy_name = st.sidebar.selectbox(
    "Strategy",
    options=[
        "LO CSMOM"
    ],
    index=0
)

nb_period_mom = st.sidebar.slider(
    "Momentum lookback (days)",
    min_value=22,
    max_value=504,
    value=252,
    step=22
)

nb_period_to_exclude_mom = st.sidebar.slider(
    "Days to exclude from Momentum",
    min_value=20,
    max_value=502,
    value=22,
    step=1
)

p_low, p_high = st.sidebar.slider(
    "Portfolio percentiles",
    min_value=0,
    max_value=100,
    value=(10, 90),
    step=1
)

rebal_periods = st.sidebar.number_input(
    "rebalancing frequency (days)",
    min_value=1,
    max_value=252,
    value=22,
    step=1
)

tc_bps = st.sidebar.number_input(
    "Transaction costs (bps)",
    min_value=0.0,
    max_value=100.0,
    value=10.0,
    step=1.0
)

n_implementation_lags = st.sidebar.slider(
    "Implementation lag (days)",
    min_value=1,
    max_value=22,
    value=1,
    step=1
)

pw_low, pw_high = st.sidebar.slider(
    "Winsorization percentiles",
    min_value=0,
    max_value=100,
    value=(2, 98),
    step=1
)

if p_low >= p_high:
    st.sidebar.error("Lower percentile must be < upper percentile.")
    run = False
elif nb_period_to_exclude_mom>nb_period_mom-2:
    st.sidebar.error("Days to exclude from Momentum must be < Momentum lookback - 2.")
    run = False
elif pw_low>=pw_high:
    st.sidebar.error("Lower winsorization percentile must be < upper percentile")
    run = False
else:
    run = st.sidebar.button("Run Backtest")

# --- Run ---
if run:
    config = {
        "n_implementation_lags":n_implementation_lags,
        "transaction_costs":tc_bps,
        "strategy_name": strategy_name,
        "nb_period_mom": nb_period_mom,
        "nb_period_to_exclude_mom":nb_period_to_exclude_mom,
        "percentiles_portfolios": (p_low, p_high),
        "percentiles_winsorization": (pw_low, pw_high),
        "rebal_periods":rebal_periods
    }

    with st.spinner("Running backtest..."):
        results = run_backtest_cached(config)

    st.session_state["results"] = results
    st.session_state["last_config"] = config
    st.success("Backtest completed")

# --- Utility function for metrics ---
def compute_metrics(rets: pd.Series) -> dict:
    ann_ret = (1 + rets).prod() ** (252 / len(rets)) - 1
    ann_vol = rets.std() * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else np.nan
    max_dd = (
        (1 + rets).cumprod()
        .div((1 + rets).cumprod().cummax())
        .min() - 1
    )
    return {
        "Ann. Return": ann_ret,
        "Ann. Vol": ann_vol,
        "Sharpe": sharpe,
        "Max Drawdown": max_dd
    }


# --- Display ---
if st.session_state["results"] is not None:
    results = st.session_state["results"]
    rets = results["strategy_net_returns"].iloc[:,0]
    # Benchmark returns
    rets_bench = results["bench_net_returns"].iloc[:,0]

    # Cumperf plot
    perf_df = pd.DataFrame({
        "Strategy": (1 + rets).cumprod(),
        "Benchmark": (1 + rets_bench).cumprod()
    })

    st.subheader("Performance (Cumulative)")
    st.line_chart(perf_df, height=400)

    # Metrics
    metrics_strat = compute_metrics(rets)
    metrics_bench = compute_metrics(rets_bench)

    st.subheader("Performance Metrics")

    col_labels = st.columns(5)
    col_labels[0].markdown("**Portfolio**")
    for i, k in enumerate(metrics_strat.keys(), start=1):
        col_labels[i].markdown(f"**{k}**")

    # Strategy row
    cols = st.columns(5)
    cols[0].markdown("**Strategy**")
    cols[1].metric(label="annret", value=f"{metrics_strat['Ann. Return']:.2%}", label_visibility="hidden")
    cols[2].metric(label="annvol",label_visibility="hidden",value=f"{metrics_strat['Ann. Vol']:.2%}")
    cols[3].metric(label="sr",label_visibility="hidden",value=f"{metrics_strat['Sharpe']:.2f}")
    cols[4].metric(label="mdd",label_visibility="hidden",value=f"{metrics_strat['Max Drawdown']:.2%}")

    # Benchmark row
    cols = st.columns(5)
    cols[0].markdown("**Benchmark**")
    cols[1].metric(label="annret",label_visibility="hidden",value=f"{metrics_bench['Ann. Return']:.2%}")
    cols[2].metric(label="annvol",label_visibility="hidden",value=f"{metrics_bench['Ann. Vol']:.2%}")
    cols[3].metric(label="sr",label_visibility="hidden",value=f"{metrics_bench['Sharpe']:.2f}")
    cols[4].metric(label="mdd",label_visibility="hidden",value=f"{metrics_bench['Max Drawdown']:.2%}")
