import yfinance as yf
import pandas as pd
import duckdb
import json
import os
from datetime import date


if __name__ == "__main__":
    #Stock Data Config
    TICKER = input("Enter stock ticker to evaluate past market data (e.g. NVDA): ")
    START_DATE = "2020-01-01"
    END_DATE = str(date.today())
    DB_PATH = "market.db"
    CONFIG_FILE = "config.json"

    #Download data
    df = yf.download(TICKER, start=START_DATE, end=END_DATE)

    #Flatten yfinance multi-index column names, if present
    df.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in df.columns]
    print(df.head())

    #Preserve the date index as a separate Date column
    df = df.reset_index()

    #Normalize schema to Close_{Ticker name}
    close_col = f'Close_{TICKER}'
    if close_col in df.columns:
        df = df.rename(columns={close_col: 'Close'})

    #Data storage
    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE OR REPLACE TABLE prices AS SELECT * FROM df")
    conn.close()

    #Store configured stock info in json for reference in other scripts
    config = {"ticker": TICKER, "start_date": START_DATE, "end_date": END_DATE, "database": DB_PATH}
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)

    print(f"Data stored for ticker: {TICKER}")

    #Verify
    conn = duckdb.connect(DB_PATH)
    print(conn.execute("SELECT * FROM prices LIMIT 1").fetchone())
    conn.close()