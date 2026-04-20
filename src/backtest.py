import duckdb
import pandas as pd

class Strategy:
    def __init__(self, db_path_or_conn):
        #Initialize with database connection (accept either path string or existing connection)
        if isinstance(db_path_or_conn, str):
            self.conn = duckdb.connect(db_path_or_conn)
        else:
            self.conn = db_path_or_conn
        self.orders = pd.DataFrame()

    def close_connection(self):
        self.conn.close()
        print("DB connection closed.")

    def moving_averages(self, short_window = 10, long_window = 20, price_col = 'Close'):
        #Calculate short moving average & long moving average with SQL query
        query = f"""
            SELECT Date, {price_col},
                   AVG({price_col}) OVER (ORDER BY Date 
                       ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{short_window},
                   AVG({price_col}) OVER (ORDER BY Date 
                       ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{long_window}
            FROM prices
        """
        df = self.conn.execute(query).fetchdf()

        #Detect crossover via pandas vectorization, use to populate buy and sell orders
        cross_up   = (df[f'SMA_{short_window}'] > df[f'SMA_{long_window}']) & (df[f'SMA_{short_window}'].shift(1) <= df[f'SMA_{long_window}'].shift(1))
        cross_down = (df[f'SMA_{short_window}'] < df[f'SMA_{long_window}']) & (df[f'SMA_{short_window}'].shift(1) >= df[f'SMA_{long_window}'].shift(1))
    
        buys  = df[cross_up][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Buy')
        sells = df[cross_down][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Sell')

        self.orders = pd.concat([buys, sells]).sort_values('Date').reset_index(drop=True)

        #Return dataframe of orders with Date and Action (Buy/Sell)
        return self.orders

    def compute_nav(self, prices_df, capital=10_000, price_col='Close'):
        cash = capital
        shares = 0
        nav_history = []

        if self.orders.empty:
            for _, row in prices_df.iterrows():
                nav_history.append({'Date': row['Date'], 'NAV': cash})
            return pd.DataFrame(nav_history)

        for _, row in prices_df.iterrows():
            date_str = str(row['Date'])[:10]
            matched_orders = self.orders[self.orders['Date'].astype(str).str[:10] == date_str]
            for _, order in matched_orders.iterrows():
                if order['Action'] == 'Buy' and cash >= row[price_col]:
                    shares = cash // row[price_col]
                    cash -= shares * row[price_col]
                elif order['Action'] == 'Sell' and shares > 0:
                    cash += shares * row[price_col]
                    shares = 0
            nav = cash + shares * row[price_col]
            nav_history.append({'Date': row['Date'], 'NAV': nav})

        return pd.DataFrame(nav_history)

class Portfolio:
    def __init__(self):
        self.buy_orders = 0
        self.sell_orders = 0
        self.total_profit = 0.0
        self.percent_gain = 0.0

    def pnl_calc(self, order_list, price_col='Close'):
        #Sort orders by date to ensure chronological processing
        order_list = order_list.sort_values('Date').reset_index(drop=True)
        
        self.buy_orders = (order_list['Action'] == 'Buy').sum()
        self.sell_orders = (order_list['Action'] == 'Sell').sum()
        
        #Track unmatched buy orders and measure profit only on matched buy/sell pairs
        unmatched_buys = []
        matched_buy_prices = []
        self.total_profit = 0.0
        matched_buy_count = 0
        matched_sell_count = 0
        
        for _, order in order_list.iterrows():
            if order['Action'] == 'Buy':
                #Add buy order to unmatched list
                unmatched_buys.append(order[price_col])
            elif order['Action'] == 'Sell' and unmatched_buys:
                #Match with the oldest unmatched buy
                buy_price = unmatched_buys.pop(0)
                matched_buy_prices.append(buy_price)
                sell_price = order[price_col]
                self.total_profit += sell_price - buy_price
                matched_buy_count += 1
                matched_sell_count += 1
        
        self.total_profit = round(self.total_profit, 2)
        
        #Calculate percent gain based only on matched buy prices
        total_buy_value = sum(matched_buy_prices)
        if total_buy_value > 0:
            self.percent_gain = (self.total_profit / total_buy_value) * 100
        else:
            self.percent_gain = 0.0
        
        #Update counts to reflect only matched orders for profit calculation
        self.buy_orders = matched_buy_count
        self.sell_orders = matched_sell_count

    def show_metrics(self):
        print("\n----PNL REPORT----")
        print(f"Buy Orders: {self.buy_orders}\nSell Orders: {self.sell_orders}\nTotal Gross Profit: {self.total_profit}\nPercent Gain: {self.percent_gain:.2f}%")
        return (self.buy_orders, self.sell_orders, self.total_profit, self.percent_gain)