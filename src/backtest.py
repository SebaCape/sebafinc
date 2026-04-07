import duckdb
import numpy as np
import pandas as pd
import datetime

class Strategy:
    def __init__(self, db_path):
        #initialize with database connection
        self.conn = duckdb.connect(db_path)
        self.orders = pd.DataFrame()

    def close_connection(self):
        self.conn.close()
        print("DB connection closed.")

    def moving_averages(self, short_window = 5, long_window = 20):
        #Calculate short moving average & long moving average with SQL query
        query = f"SELECT Date, Close_AAPL, AVG(Close_AAPL)" \
        f"OVER (ORDER BY Date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{short_window}, AVG(Close_AAPL) " \
        f"OVER (ORDER BY Date ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{long_window} " \
        f"FROM prices"
        df = self.conn.execute(query).fetchdf()

        #Detect crossover via pandas vectorization, use to populate buy and sell orders
        cross_up   = (df['SMA_5'] > df['SMA_20']) & (df['SMA_5'].shift(1) <= df['SMA_20'].shift(1))
        cross_down = (df['SMA_5'] < df['SMA_20']) & (df['SMA_5'].shift(1) >= df['SMA_20'].shift(1))
    
        buys  = df[cross_up][['Date', 'Close_AAPL']].assign(Action='Buy')
        sells = df[cross_down][['Date', 'Close_AAPL']].assign(Action='Sell')

        self.orders = pd.concat([buys, sells]).sort_values('Date').reset_index(drop=True)

        #Return array of orders with Date and Action (Buy/Sell)
        return self.orders
    
class Portfolio:
    def __init__(self):
        self.buy_orders = 0
        self.sell_orders = 0
        self.total_profit = 0.0
        self.percent_gain = 0

    def pnl_calc(self, order_list):
        self.buy_orders = (order_list['Action'] == 'Buy').sum()
        self.sell_orders = (order_list['Action'] == 'Sell').sum()
        self.total_profit = round((order_list.loc[order_list['Action'] == 'Sell', 'Close_AAPL'].sum() - order_list.loc[order_list['Action'] == 'Buy', 'Close_AAPL'].sum()), 2)

    def show_metrics(self):
        print("\n----PNL REPORT----")
        print(f"Buy Orders: {self.buy_orders}\nSell Orders: {self.sell_orders}\nTotal Gross Profit: {self.total_profit}")
        return (self.buy_orders, self.sell_orders, self.total_profit)