import yfinance as yf
import pandas as pd
import duckdb
#TODO: Research other data pipelining sources (real time market data?)

#Download data
df = yf.download("AAPL", start="2023-01-01", end="2024-01-01") #TODO: Pipeline live data from past 5 years, make stock configurable by user

#Flatten yfinance multi-index column names, if present
df.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in df.columns]
print(df.head())

#Preserve the date index as a separate Date column
df = df.reset_index()

#Data storage
conn = duckdb.connect("market.db")
conn.execute("CREATE OR REPLACE TABLE prices AS SELECT * FROM df")
#TODO: Automate data storage reset on fixed interval (daily since we are backtesting based on stock closing price)

#Verify
print(conn.execute("SELECT * FROM prices LIMIT 1").fetchone())