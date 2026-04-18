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

#Gather dataframe of orders from backtest module
strategy = backtest.Strategy('market.db')
orders = strategy.moving_averages()

print(orders)

#Ensure Date column in orders array is datetime type, and merge with price data for plotting
orders['Date'] = pd.to_datetime(orders['Date'])
orders_df = pd.DataFrame(orders)

#Deduce buy and sell orders for plotting
buys  = orders[orders['Action'] == 'Buy']
sells = orders[orders['Action'] == 'Sell']

#Close db when done transacting
strategy.close_connection()

#P&L initialization and reporting
portfolio = backtest.Portfolio()
portfolio.pnl_calc(orders)
portfolio.show_metrics()

#Plot apple prices & buy/sell orders
plt.figure(facecolor = "black")
plt.plot(x, df['Close_AAPL'], color = "#E4E9B8", zorder = 1)
plt.scatter(buys['Date'], buys['Close_AAPL'], marker = '^', color = 'green', s = 80, label = 'Buy', zorder = 2)
plt.scatter(sells['Date'], sells['Close_AAPL'], marker = 'v', color = 'red', s = 80, label = 'Sell', zorder = 2)

#TODO: Add P&L metrics to visualization somewhere

#Labeling and layout styling
plt.title("Backtested AAPL Prices from 2023-2024", color = "white", fontname = "serif")
plt.xlabel("Date", color = "white", fontname = "serif")
plt.ylabel("Close Price (USD$)", color = "white", fontname = "serif")
plt.margins(x = 0)
plt.xticks(rotation = 45, color = "white", fontname = "serif")
plt.yticks(np.arange(100, 260, 10), color = "white", fontname = "serif")
plt.rcParams['font.family'] = "serif"
plt.tight_layout()

#Axis Coloring
ax = plt.gca()
ax.set_facecolor("black")
for spine in ax.spines.values():
    spine.set_color('white')
ax.tick_params(axis = "both", color = "white")
ax.legend()

plt.show()