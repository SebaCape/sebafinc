import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import duckdb
import backtest

#Establish db connection
conn = duckdb.connect('market.db')

#Query the stored prices table
query = "SELECT * FROM prices"
df = pd.read_sql(query, conn)

print(df.head())

#Use the stored Date column for the x-axis if available
if 'Date' in df.columns:
    x = pd.to_datetime(df['Date'])
else:
    x = pd.to_datetime(df.index)

#Gather numpy arrays of orders from backtest module
strategy = backtest.Strategy('market.db')
orders = strategy.moving_averages()

print(orders)

#Plot apple prices
plt.figure(facecolor = "black")


plt.plot(x, df['Close_AAPL'], color = "#E4E9B8")
plt.title("AAPL Prices from 2023-2024", color = "white")
plt.xlabel("Date", color = "white")
plt.ylabel("Close Price (USD$)", color = "white")
plt.margins(x = 0)
plt.xticks(rotation = 45, color = "white")
plt.yticks(np.arange(100, 260, 10), color = "white")
plt.tight_layout()


ax = plt.gca()
ax.set_facecolor("black")
for spine in ax.spines.values():
    spine.set_color('white')
ax.tick_params(axis = "both", color = "white")

plt.show()