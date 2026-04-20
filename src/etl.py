from matplotlib import ticker
import yfinance as yf
import pandas as pd
import duckdb
import json
import os
from datetime import date
import sys


if __name__ == "__main__":
    #Stock Data Config
    TICKER = input("Enter stock ticker to evaluate past market data (e.g. NVDA): ")
    START_DATE = "2020-01-01"
    END_DATE = str(date.today())
    DB_PATH = "market.db"
    CONFIG_FILE = "config.json"

    #Handle invalid tickers
    info = yf.Ticker(TICKER).history(period='7d',interval='1d')
    if len(info) == 0 or len(TICKER.split()) > 1:
        print("\nInvalid ticker, try again.")
        sys.exit()

    #Handle ipo date later than 2020
    ipo_date = str(yf.Ticker(TICKER).history(period="max").index[0].date())
    ipo_figs, start_date_figs = ipo_date.split('-'), START_DATE.split('-')
    
    if date(*[int(i) for i in ipo_figs]) > date(*[int(i) for i in start_date_figs]):
        START_DATE = ipo_date

    print(START_DATE)


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