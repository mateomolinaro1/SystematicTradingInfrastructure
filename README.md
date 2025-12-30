# Systematic Trading Infrastructure

This project aims to build a **modular, end-to-end systematic trading infrastructure**, covering:

- data acquisition (historical + live),
- strategy backtesting,
- portfolio construction and order generation,
- live and mock trading,
- real-time monitoring dashboards.

The infrastructure is designed to be **production-oriented**, while remaining flexible enough for research and experimentation.

---

## High-level architecture

The project relies on the following external services:

- **Interactive Brokers (IB) API**  
  Used for:
  - live and recent historical market data,
  - order execution (paper trading and live trading).

- **WRDS API**  
  Used to retrieve **point-in-time universes and historical prices**.  
  WRDS data is preferred for backtesting, as IB historical data is not reliable far back in time (IB data is mainly suited for live trading or short lookbacks).

- **AWS S3**  
  Used as a centralized storage layer for:
  - raw data,
  - processed datasets,
  - backtest outputs,
  - trading requirements.

- **Pushover**  
  Used to send **real-time notifications** (orders, warnings, pipeline status) directly to a smartphone.

---

## Services provided by the project

The project exposes **four main services**:

### 1️⃣ Systematic trading infrastructure (core pipeline)

This is the **core production pipeline**, responsible for:
- data ingestion,
- universe updates,
- strategy backtesting,
- portfolio rebalancing,
- order generation,
- order execution via IB.

⚠️ **Advanced usage** — requires multiple external accounts (see below).

---

### 2️⃣ Strategy backtesting dashboard

A **Streamlit dashboard** dedicated to:
- exploring strategy performance,
- visualizing backtest results,
- analyzing risk and returns.

✅ **Recommended for most users**  
❌ Does NOT require IB, WRDS, or Pushover.

---

### 3️⃣ Live strategy monitoring dashboard

A **real-time monitoring dashboard** for:
- tracking the live strategy,
- inspecting current positions,
- monitoring orders and executions.

⚠️ Requires a fully configured trading setup (IB + WRDS).

---

### 4️⃣ Mock trading monitoring dashboard

A **real-time dashboard** that monitors:
- paper trading,
- simulated executions,
- strategy behavior under live market conditions.

✅ **Recommended for users without a funded IB account**  
❌ Does NOT require WRDS  
❌ Does NOT require live market data subscriptions

---

## Recommended usage

For most users, it is **strongly recommended** to use only:

- **2️⃣ Strategy backtesting dashboard**
- **4️⃣ Mock trading monitoring dashboard**

These services allow you to explore and understand the framework **without any external trading accounts**.

---

## Requirements for advanced usage (services 1️⃣ and 3️⃣)

To use the **full systematic trading pipeline**, the following are required:

- An **Interactive Brokers account**  
  - Minimum **$500** to access market data via the API  
  - Access to **paper trading**

- A **WRDS account**

- A **Pushover account** (for notifications)

⚠️ **Important**  
The project is still under active development and is currently **tightly coupled to my own AWS S3 setup** (hardcoded S3 paths).  
Some **code adjustments are required** for external users who want to run the full production pipeline.

---

## Core scripts overview

The main orchestration logic lives in the `scripts/` directory.

### `get_data_first_time.py`

- Pulls historical data from:
  - WRDS (point-in-time universe and prices),
  - Interactive Brokers.
- Pushes all retrieved data to AWS S3.

⚠️ **Must be run only once**  
⚠️ **Do NOT run this script if you only want to use services 2️⃣ and 4️⃣**

---

### `push_trading_requirements_first_time.py`

- Runs the initial backtest,
- Computes:
  - portfolio weights,
  - target allocations,
  - initial trading requirements to be executed by IB.

⚠️ Run only after `get_data_first_time.py`

---

### `run_pipeline.py` (daily job)

This is the **daily production pipeline**, meant to be run **Monday to Friday**.

It performs the following steps:

1. Checks whether new assets enter or leave the universe,
2. Downloads the latest market data from IB,
3. Updates the dataset in S3,
4. Re-runs the backtest with the most recent data,
5. Computes updated portfolio weights and orders,
6. Pre-submits orders to Interactive Brokers,
7. Automatically submits orders at market open.

### ⏰ Recommended schedule

- **Run time:** `22:15` (after market close)
- This ensures:
  - close prices are used for backtesting,
  - orders are ready before the next trading session.

Orders are **pre-submitted** and automatically **executed at 15:30 (market open)**.

---

## Security & credentials

- No credentials are stored in the Docker image.
- All secrets (IB, WRDS, AWS, Pushover) must be provided via environment variables.
- A `.env.example` file is provided for guidance.

---

## Project status

This project is:
- actively developed,
- designed for research and experimentation,
- partially production-oriented,
- not yet fully configurable for external AWS environments.

Feedback, questions, and suggestions are welcome.

---

## Quick start (recommended)

This section explains how to quickly run the project using Docker.

The easiest way to get started is to:
- clone the GitHub repository (to access configs, scripts, and dashboards),
- pull the prebuilt Docker image,
- launch the application locally.

---

## Running the dashboards (step by step for beginners)

This section explains **exactly where and how** to run each command.

All commands below must be executed in a **terminal**:

- **Windows**: Command Prompt (CMD)  
- **macOS / Linux**: Terminal

---

### 0️⃣ Open a terminal

- On **Windows**:
  - Press `Win + R`, type `cmd`, press Enter

- On **macOS**:
  - Open **Terminal** from Applications

- On **Linux**:
  - Open your system terminal

---

### 1️⃣ Choose a working directory

In the terminal, move to a directory where you want to store the project.

Example:

```bash
cd Desktop
git clone https://github.com/mateomolinaro1/SystematicTradingInfrastructure.git
cd SystematicTradingInfrastructure
docker pull mateomolinaro1/systematic-trading-infra:ALL.v2
cp .env.example .env


docker run --rm -p 8501:8501 --env-file .env mateomolinaro1/systematic-trading-infra:ALL.v2 -m streamlit run src/systematic_trading_infra/dashboards/paper_trading_monitoring/app.py --server.address=0.0.0.0 --server.port=8501
[//]: # open in your browser: (http://localhost:8501)
docker run --rm -p 8502:8501 --env-file .env mateomolinaro1/systematic-trading-infra:ALL.v2 -m streamlit run src/systematic_trading_infra/dashboards/backtesting/app.py --server.address=0.0.0.0 --server.port=8501
[//]: # open in your browser: (http://localhost:8502)
docker run --rm -p 8503:8501 --env-file .env mateomolinaro1/systematic-trading-infra:ALL.v2 -m streamlit run src/systematic_trading_infra/dashboards/mock_trading_monitoring/app.py --server.address=0.0.0.0 --server.port=8501
[//]: # open in your browser: (http://localhost:8503)
## Disclaimer

This project is for **educational and research purposes only**.  
It is **not financial advice** and should not be used in production without proper validation, risk controls, and compliance checks.
