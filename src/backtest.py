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
    
        buys  = df[cross_up][['Date']].assign(Action='Buy')
        sells = df[cross_down][['Date']].assign(Action='Sell')
        self.orders = pd.concat([buys, sells]).sort_values('Date').reset_index(drop=True)

        #Return array of orders with Date and Action (Buy/Sell)
        return self.orders