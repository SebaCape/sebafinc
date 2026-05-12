# SebaFinc | Quantitative Backtesting Engine

A high-performance, asynchronous backtesting engine built with Python, DuckDB, and yfinance. This system implements a vectorized moving average crossover strategy, maintains a FIFO-matched P&L tracking system, and visualizes performance against industry benchmarks.

## Features

- **Async ETL Pipeline** — Ingests multi-ticker OHLCV data via yfinance into a DuckDB columnar store for high-speed analytical queries.
- **Modular Architecture** — Decoupled strategy logic, portfolio management, and visualization layers for easy extension.
- **FIFO P&L Tracking** — Robust accounting system that matches buy/sell orders chronologically to calculate realized P&L and NAV curves.
- **Benchmark Comparison** — Automatically compares strategy performance against SPY (S&P 500) and raw asset buy-and-hold strategies.
- **Edge Case Handling** — Robust preprocessing to handle IPO date offsets, multi-index column flattening, and missing data integrity.
- **Dual-Panel Visualization** — Renders signal overlays (Buy/Sell) on price charts alongside relative Net Asset Value (NAV) performance metrics.

## Core Components

- **backtest.py** — Contains the Strategy (signal generation/NAV calculation) and Portfolio (P&L accounting) classes.
- **ingest.py** — Handles the async ETL process, schema normalization, and DuckDB persistence.
- **main.py** — The execution entry point that orchestrates data loading, strategy execution, and reporting.
- **visualization.py** — Matplotlib-based engine for rendering performance charts and quantitative metrics.
- **/tests** — Comprehensive pytest suite covering trade pairing, signal detection, and synthetic edge cases.

## Quick Start
1. Install Dependencies

Download all required dependencies from the `requirements.txt` file:
```bash
pip install -r requirements.txt
```

2. Ingest Data

Run the ETL script and follow instructions in the CLI to fetch historical data and initialize the DuckDB database:
```bash
python etl.py
```

3. Run Backtest

Execute the main engine to run the strategy and view the visualization:
```bash
python main.py
```

## Performance Metrics

The system provides a PNL Report in the console and an overlay on the chart including:

- Total Gross Profit (USD)
- Percent Gain (Matched pairs only)
- Strategy NAV vs. SPY Benchmark

## Example Visualization
<img width="1888" height="974" alt="image" src="https://github.com/user-attachments/assets/10ec5a29-8bd6-48fa-b97c-d4af29dc6fab" />

MIT License
