import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import duckdb
import backtest
import json
import os

#Load configuration
CONFIG_FILE = "config.json"
ticker = start_date = end_date = db_path = "UNKNOWN"

config = {}
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
        ticker = config.get('ticker', 'UNKNOWN')
        start_date = config.get('start_date', 'UNKNOWN')
        end_date = config.get('end_date', 'UNKNOWN')
        db_path = config.get('database', 'UNKNOWN')

#Establish db connection
conn = duckdb.connect(db_path)

#Query the stored prices table
query = "SELECT * FROM prices"
df = pd.read_sql(query, conn)

print(df.head())

#Detect the price column dynamically
price_col = 'Close' if 'Close' in df.columns else next((col for col in df.columns if 'Close' in col), 'Close')

#Use the stored Date column for the x-axis if available
if 'Date' in df.columns:
    x = pd.to_datetime(df['Date'])
else:
    x = pd.to_datetime(df.index)

#Gather dataframe of orders from backtest module, pass the existing connection
strategy = backtest.Strategy(conn)
orders = strategy.moving_averages(price_col=price_col)

print(orders)

#Ensure Date column in orders array is datetime type, and merge with price data for plotting
orders['Date'] = pd.to_datetime(orders['Date'])

#Deduce buy and sell orders for plotting
buys  = orders[orders['Action'] == 'Buy']
sells = orders[orders['Action'] == 'Sell']

#DB connection remains open for reference, closed after Portfolio operations

#P&L initialization and reporting
portfolio = backtest.Portfolio()
portfolio.pnl_calc(orders)
buy_orders, sell_orders, total_profit, percent_gain = portfolio.show_metrics()

#Plot prices & buy/sell orders
plt.figure(figsize=(10, 6), facecolor = "black")
plt.plot(x, df[price_col], color = "#E4E9B8", zorder = 1)
plt.scatter(buys['Date'], buys['Close'], marker = '^', color = 'green', s = 80, label = 'Buy', zorder = 2)
plt.scatter(sells['Date'], sells['Close'], marker = 'v', color = 'red', s = 80, label = 'Sell', zorder = 2)

#Add PNL dashboard at the bottom
pnl_text = f'Buy Orders: {buy_orders}\nSell Orders: {sell_orders}\nTotal Profit: ${total_profit:.2f}\nPercent Gain: {percent_gain:.2f}%'
plt.text(0.02, 0.02, pnl_text, transform=plt.gcf().transFigure, fontsize=10, 
         verticalalignment='bottom', horizontalalignment='left',
         bbox=dict(boxstyle='round,pad=0.5', facecolor='black', edgecolor='white', alpha=0.8),
         color='white', fontname='serif')

#Labeling and layout styling
plt.title(f"Backtested {ticker} Prices from {start_date}-{end_date}", color = "white", fontname = "serif")
plt.xlabel("Date", color = "white", fontname = "serif")
plt.ylabel("Close Price (USD$)", color = "white", fontname = "serif")
plt.margins(x = 0)

#Set y-axis range based on min/max stock prices with padding
min_price = df[price_col].min()
max_price = df[price_col].max()
price_range = max_price - min_price
padding = price_range * 0.05  # 5% padding
plt.ylim(min_price - padding, max_price + padding)

plt.xticks(rotation = 45, color = "white", fontname = "serif")
plt.yticks(color = "white", fontname = "serif")
plt.rcParams['font.family'] = "serif"
plt.tight_layout()

#Axis Coloring
ax = plt.gca()
ax.set_facecolor("black")
for spine in ax.spines.values():
    spine.set_color('white')
ax.tick_params(axis = "both", color = "white")
ax.legend()

#Close database connection
conn.close()

plt.show()