import matplotlib.pyplot as plt
import pandas as pd
import duckdb

#Establish db connection
conn = duckdb.connect('market.db')

#Query the stored prices table
query = "SELECT * FROM prices"
df = pd.read_sql(query, conn)

#Use the stored Date column for the x-axis if available
if 'Date' in df.columns:
    x = pd.to_datetime(df['Date'])
else:
    x = pd.to_datetime(df.index)

#Select the Close column
close_cols = [col for col in df.columns if 'Close' in str(col)]
if not close_cols:
    raise KeyError('No Close column found in prices table: ' + ', '.join(map(str, df.columns)))
close_col = close_cols[0]

#Plot apple prices
plt.plot(x, df[close_col])
plt.title("AAPL Prices from 2023-2024")
plt.xlabel("Date")
plt.ylabel("Close Price")
plt.ylim(bottom = 0)
plt.tight_layout()
plt.show()