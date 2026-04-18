import duckdb
import numpy as np
import pandas as pd

class Strategy:
    def __init__(self, db_path):
        #Initialize with database connection
        self.conn = duckdb.connect(db_path)
        self.orders = pd.DataFrame()

    def close_connection(self):
        self.conn.close()
        print("DB connection closed.")

    def moving_averages(self, short_window = 5, long_window = 20, price_col = 'Close'):
        #Calculate short moving average & long moving average with SQL query
        query = f"SELECT Date, {price_col}, AVG({price_col})" \
        f"OVER (ORDER BY Date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{short_window}, AVG({price_col}) " \
        f"OVER (ORDER BY Date ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{long_window} " \
        f"FROM prices"
        df = self.conn.execute(query).fetchdf()

        #Detect crossover via pandas vectorization, use to populate buy and sell orders
        cross_up   = (df[f'SMA_{short_window}'] > df[f'SMA_{long_window}']) & (df[f'SMA_{short_window}'].shift(1) <= df[f'SMA_{long_window}'].shift(1))
        cross_down = (df[f'SMA_{short_window}'] < df[f'SMA_{long_window}']) & (df[f'SMA_{short_window}'].shift(1) >= df[f'SMA_{long_window}'].shift(1))
    
        buys  = df[cross_up][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Buy')
        sells = df[cross_down][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Sell')

        self.orders = pd.concat([buys, sells]).sort_values('Date').reset_index(drop=True)

        #Return dataframe of orders with Date and Action (Buy/Sell)
        return self.orders
    
class Portfolio:
    def __init__(self):
        self.buy_orders = 0
        self.sell_orders = 0
        self.total_profit = 0.0
        self.percent_gain = 0.0

    def pnl_calc(self, order_list, price_col='Close'):
        self.buy_orders = (order_list['Action'] == 'Buy').sum()
        self.sell_orders = (order_list['Action'] == 'Sell').sum()
        #TODO: Optimize profit calculator to only include completely executed orders (joint buys and sells)
        self.total_profit = round((order_list.loc[order_list['Action'] == 'Sell', price_col].sum() - order_list.loc[order_list['Action'] == 'Buy', price_col].sum()), 2)

        total_buy_value = order_list.loc[order_list['Action'] == 'Buy', price_col].sum()
        self.percent_gain = (self.total_profit / total_buy_value) * 100 if total_buy_value > 0 else 0.0

    def show_metrics(self):
        print("\n----PNL REPORT----")
        print(f"Buy Orders: {self.buy_orders}\nSell Orders: {self.sell_orders}\nTotal Gross Profit: {self.total_profit}\nPercent Gain: {self.percent_gain:.2f}%")
        return (self.buy_orders, self.sell_orders, self.total_profit, self.percent_gain)