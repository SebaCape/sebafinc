# sebafinc: Quantitative Backtesting & Paper Trading Engine

A modular Python backtesting engine with an Alpaca Markets ETL pipeline, DuckDB columnar storage, configurable trading strategies, NAV curve computation, benchmark-aware visualization, and live paper trading execution via the Alpaca API.

---

## Features

- **Alpaca ETL Pipeline** — ingests OHLCV data via the Alpaca Markets API, normalizes schema, strips timezone metadata at ingestion, and stores into a local DuckDB columnar store alongside a SPY benchmark table
- **Configurable Strategies** — switch between strategies via `config.json` without touching code; supports Moving Average Crossover and Bollinger Bands + RSI
- **FIFO-Matched P&L** — tracks only fully matched buy/sell pairs; ignores unmatched open positions for accurate profit reporting
- **NAV Curve** — computes portfolio value at every timestep, enabling comparison against buy-and-hold and SPY benchmark
- **Dual-Panel Visualization** — price chart with signal overlays (buy/sell markers) and a NAV comparison chart below
- **Live Paper Trading** — detects the latest strategy signal, checks existing positions and buying power, and submits market orders to Alpaca's paper trading environment automatically
- **Autonomous Scheduling** — `scheduler.py` runs the ETL and paper trading pipeline automatically on a configurable interval, respecting market hours
- **Unit Tested** — pytest suite covering P&L logic, crossover detection, Broker method correctness, and trade pairing edge cases

---

## Setup

**Requirements:** Python 3.11+

```bash
# Clone and enter the project
git clone https://github.com/SebaCape/sebafinc.git
cd sebafinc

# Create and activate virtual environment
py -3.14 -m venv .venv
.venv\Scripts\activate       # Windows
source .venv/bin/activate    # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

### Alpaca API Keys

Create a free paper trading account at [alpaca.markets](https://alpaca.markets), generate API keys, and add them to a `.env` file in the project root:

```
ALPACA_API_KEY=your_key_here
ALPACA_SECRET_KEY=your_secret_here
```

Verify connectivity before running anything else:

```bash
python scripts/test_connection.py
```

You should see your paper account status and buying power. If this fails, check your keys before proceeding.

---

## Usage

### 1. Run the ETL pipeline

```bash
python src/etl.py
```

You will be prompted for a ticker. The script fetches daily OHLCV data from the Alpaca API from 2020 to today, stores it in `market.db`, and writes `config.json` with default strategy parameters and `"mode": "backtest"`.

```
Enter stock ticker (e.g. NVDA): AAPL
```

### 2. Configure your strategy and mode

`config.json` controls everything — strategy selection, parameters, and execution mode. Edit it directly between runs:

```json
{
  "ticker": "NVDA",
  "benchmark_ticker": "SPY",
  "start_date": "2020-01-01",
  "end_date": "2025-05-30",
  "database": "market.db",
  "benchmark_table": "benchmark",
  "strategy": "bollinger_rsi",
  "bb_window": 20,
  "bb_std": 2.0,
  "rsi_period": 14,
  "rsi_oversold": 35,
  "rsi_overbought": 65,
  "mode": "backtest"
}
```

Set `"mode"` to `"paper"` to switch from historical analysis to live paper order execution. Set `"strategy"` to `"moving_averages"` to use the MA crossover strategy:

```json
{
  "strategy": "moving_averages",
  "short_window": 50,
  "long_window": 200
}
```

### 3. Run backtest mode

```bash
# config.json: "mode": "backtest"
python src/main.py
```

Produces a dual-panel matplotlib chart and prints a P&L report:

```
----PNL REPORT----
Buy Orders: 4
Sell Orders: 4
Total Gross Profit: 312.47
Percent Gain: 18.43%
```

### 4. Run paper trading mode

```bash
# config.json: "mode": "paper"
python src/main.py
```

The engine fetches the latest strategy signal from the stored data, checks whether a position is already open, calculates order size from available buying power, and submits a market order to Alpaca. Output:

```
Account buying power: $97,430.21
Order submitted: a3f2c1d4-...
```

Verify the order in your Alpaca paper dashboard under the Activity tab.

### 5. Run autonomously

```bash
python scheduler.py
```

Runs the ETL and paper trading pipeline on a fixed interval, checking market hours automatically. Only executes during market hours (Monday–Friday, 9:30 AM–4:00 PM ET). Leave running in a terminal or configure via Task Scheduler for fully hands-off operation.

### 6. Run tests

```bash
pytest tests/ -v
```

---

## Modes

| Mode | `config.json` value | What it does |
|---|---|---|
| Backtest | `"mode": "backtest"` | Runs strategy on historical data, computes P&L and NAV, renders visualization |
| Paper trading | `"mode": "paper"` | Detects latest signal, checks position, submits live market order to Alpaca paper account |
| Autonomous | `"mode": "paper"` + `scheduler.py` | Runs ETL and paper trading on a schedule, market hours only |

---

## Strategies

### Bollinger Bands + RSI (`bollinger_rsi`)

Combines two independent signals. Price must touch the Bollinger Band boundary AND RSI must confirm oversold/overbought conditions before a signal fires. Reduces false positives compared to a single-indicator strategy.

| Parameter | Default | Description |
|---|---|---|
| `bb_window` | 20 | Rolling window for band calculation |
| `bb_std` | 2.0 | Standard deviations for band width |
| `rsi_period` | 14 | RSI lookback period |
| `rsi_oversold` | 35 | RSI threshold to confirm buy signal |
| `rsi_overbought` | 65 | RSI threshold to confirm sell signal |

**Buy signal:** price ≤ lower band AND RSI < `rsi_oversold`

**Sell signal:** price ≥ upper band AND RSI > `rsi_overbought`

Signals are enforced to alternate — no consecutive buys or sells.

### Moving Average Crossover (`moving_averages`)

Classic dual-SMA crossover. Buys when the short SMA crosses above the long SMA, sells on the reverse.

| Parameter | Default | Description |
|---|---|---|
| `short_window` | 10 | Short SMA window |
| `long_window` | 20 | Long SMA window |

---

## Paper Trading Behaviour

When running in paper mode, the engine:

1. Runs the configured strategy on stored historical data to generate all signals
2. Takes the most recent signal and checks whether it is within 1 day old
3. If a **Buy** signal: checks for an existing position in the ticker — skips if already holding, otherwise calculates quantity as `floor(buying_power × 0.95 / latest_price)` and submits a market buy
4. If a **Sell** signal: checks for an existing position — skips if none, otherwise submits a market sell for the full held quantity
5. Prints the Alpaca order ID on success

Signals older than 1 day are ignored to prevent acting on stale data.

---

## Output

The backtest visualization produces a two-panel figure:

**Top panel — Price chart**
- Asset close price over the full date range
- Green upward triangles at buy signals
- Red downward triangles at sell signals
- P&L metrics box (buy/sell count, total profit, percent gain)

**Bottom panel — NAV comparison**
- Strategy NAV (blue) — portfolio value following strategy signals
- Buy-and-hold NAV (green) — result of buying on day one and holding
- SPY buy-and-hold NAV (yellow) — benchmark comparison

<img width="1491" height="995" alt="image" src="https://github.com/user-attachments/assets/48064c1a-4902-4de3-bf41-ad91d3e07227" />

---

## Disclaimer

This project is for educational and research purposes only. Nothing in this codebase constitutes financial advice.
## License

MIT License
