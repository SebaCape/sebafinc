import yfinance as yf
import duckdb

#Download data
df = yf.download("AAPL", start="2023-01-01", end="2024-01-01")

#Data storage
conn = duckdb.connect("market.db")
conn.execute("CREATE TABLE IF NOT EXISTS prices AS SELECT * FROM df")

#Verify
print(conn.execute("SELECT COUNT(*) FROM prices").fetchone())