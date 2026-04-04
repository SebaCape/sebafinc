import yfinance as yf
import duckdb

#Download data
df = yf.download("AAPL", start="2023-01-01", end="2024-01-01")
print(df.head())

#Flatten yfinance multi-index column names, if present
df.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in df.columns]

#Preserve the date index as a separate Date column
df = df.reset_index()

#Data storage
conn = duckdb.connect("market.db")
conn.execute("CREATE OR REPLACE TABLE prices AS SELECT * FROM df")

#Verify
print(conn.execute("SELECT * FROM prices LIMIT 1").fetchone())