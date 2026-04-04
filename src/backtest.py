import duckdb
import numpy as np
import pandas as pd
import datetime

class Strategy:
    def __init__(self, db_path):
        #initialize with database connection and numpy orders array
        self.conn = duckdb.connect(db_path)
        self.orders = np.array([], dtype=[('Date', 'datetime64[ns]'), ('Action', 'U4')])

    def moving_averages(self):
        #Calculate small moving average & large moving average with SQL query
        query = f"SELECT Date, Close_AAPL, AVG(Close_AAPL)" \
        f"OVER (ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS SMA_5, AVG(Close_AAPL) " \
        f"OVER (ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS SMA_20 " \
        f"FROM prices"
        df = self.conn.execute(query).fetchdf()

        #Detect crossover and append buy/sell orders to the orders array
        for i in range(1, len(df)):
            if df['SMA_5'][i] > df['SMA_20'][i] and df['SMA_5'][i-1] <= df['SMA_20'][i-1]:
                self.orders = np.append(self.orders, np.array([(df['Date'][i], 'Buy')], dtype=self.orders.dtype))
            elif df['SMA_5'][i] < df['SMA_20'][i] and df['SMA_5'][i-1] >= df['SMA_20'][i-1]:
                self.orders = np.append(self.orders, np.array([(df['Date'][i], 'Sell')], dtype=self.orders.dtype))

        #Return array of orders with Date and Action (Buy/Sell)
        return self.orders