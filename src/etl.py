import duckdb
import pandas as pd
import json
import os
import sys
from datetime import date, datetime
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv()

#Get Alpaca API keys from environment variables
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

#Process OHLVC data from Alpaca into a clean DataFrame with required columns and date formatting
def fetch_alpaca(ticker, start_date, end_date):
    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

    request = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Day,
        start=datetime.strptime(start_date, "%Y-%m-%d"),
        end=datetime.strptime(end_date, "%Y-%m-%d"),
    )

    bars = client.get_stock_bars(request)
    df = bars.df

    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
        df = df.drop(columns=['symbol'], errors='ignore')
        df = df.rename(columns={'timestamp': 'Date'})
    else:
        df = df.reset_index().rename(columns={'timestamp': 'Date'})

    df['Date'] = pd.to_datetime(df['Date']).dt.normalize().dt.tz_localize(None)
    df = df.rename(columns={
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume',
    })

    return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].sort_values('Date').reset_index(drop=True)

#Only runs when executed directly, not on module import
if __name__ == "__main__":
    import argparse

    #Configuration and data fetching logic, with error handling for missing config or API issues
    BENCHMARK_TICKER = "SPY"
    END_DATE         = str(date.today())
    DB_PATH          = "market.db"
    CONFIG_FILE      = "config.json"

    parser = argparse.ArgumentParser()
    parser.add_argument('--auto', action='store_true', help='Read ticker from config.json instead of prompting')
    args = parser.parse_args()

    #In auto mode, read ticker from config.json; otherwise prompt user. Validate ticker format and handle missing config gracefully.
    if args.auto:
        if not os.path.exists(CONFIG_FILE):
            print("No config.json found — run etl.py manually first to set a ticker.")
            sys.exit(1)
        with open(CONFIG_FILE) as f:
            existing = json.load(f)
        TICKER     = existing['ticker']
        START_DATE = existing.get('start_date', '2020-01-01')
        print(f"Auto mode: using ticker {TICKER} from config.json")
    else:
        TICKER     = input("Enter stock ticker (e.g. NVDA): ").upper().strip()
        START_DATE = "2020-01-01"
        if not TICKER or len(TICKER.split()) > 1:
            print("Invalid ticker.")
            sys.exit(1)

    #Fetch data from Alpaca, with error handling for API issues or empty results
    print(f"Fetching {TICKER} from Alpaca...")
    try:
        df           = fetch_alpaca(TICKER, START_DATE, END_DATE)
        benchmark_df = fetch_alpaca(BENCHMARK_TICKER, START_DATE, END_DATE)
    except Exception as e:
        print(f"Failed to fetch data: {e}")
        sys.exit(1)

    if df.empty:
        print("No data returned — check ticker or date range.")
        sys.exit(1)

    print(df.head())

    #Store data in DuckDB, with error handling for database issues
    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE OR REPLACE TABLE prices    AS SELECT * FROM df")
    conn.execute("CREATE OR REPLACE TABLE benchmark AS SELECT * FROM benchmark_df")
    conn.close()

    #In auto mode, update end_date in config.json to ensure backtest runs with latest data
    if args.auto:
        with open(CONFIG_FILE) as f:
            existing = json.load(f)
        existing['end_date'] = END_DATE
        with open(CONFIG_FILE, 'w') as f:
            json.dump(existing, f, indent=2)

    #Only write config.json when ran manually, to avoid overwriting existing config in auto mode
    if not args.auto:
        config = {
            "ticker":           TICKER,
            "benchmark_ticker": BENCHMARK_TICKER,
            "start_date":       START_DATE,
            "end_date":         END_DATE,
            "database":         DB_PATH,
            "benchmark_table":  "benchmark",
            "strategy":         "bollinger_rsi",
            "bb_window":        20,
            "bb_std":           2.0,
            "rsi_period":       14,
            "rsi_oversold":     35,
            "rsi_overbought":   65,
            "mode":             "backtest",
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)

    print(f"Stored {len(df)} rows for {TICKER}, {len(benchmark_df)} rows for {BENCHMARK_TICKER}")