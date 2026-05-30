import os

import duckdb
import pandas as pd
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv()

class Strategy:
    #Initialize with database connection, either by path or existing connection object
    def __init__(self, db_path_or_conn):
        if isinstance(db_path_or_conn, str):
            self.conn = duckdb.connect(db_path_or_conn)
        else:
            self.conn = db_path_or_conn
        self.orders = pd.DataFrame()

    #Close database connection when done to free resources
    def close_connection(self):
        self.conn.close()
        print("DB connection closed.")

    #Implement a simple moving average crossover strategy, with buy signals when short-term SMA crosses above long-term SMA, and sell signals when short-term SMA crosses below long-term SMA
    def moving_averages(self, short_window = 10, long_window = 20, price_col = 'Close'):
        query = f"""
            SELECT Date, {price_col},
                   AVG({price_col}) OVER (ORDER BY Date 
                       ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{short_window},
                   AVG({price_col}) OVER (ORDER BY Date 
                       ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS SMA_{long_window}
            FROM prices
        """
        df = self.conn.execute(query).fetchdf()

        cross_up   = (df[f'SMA_{short_window}'] > df[f'SMA_{long_window}']) & (df[f'SMA_{short_window}'].shift(1) <= df[f'SMA_{long_window}'].shift(1))
        cross_down = (df[f'SMA_{short_window}'] < df[f'SMA_{long_window}']) & (df[f'SMA_{short_window}'].shift(1) >= df[f'SMA_{long_window}'].shift(1))
    
        buys  = df[cross_up][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Buy')
        sells = df[cross_down][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Sell')

        self.orders = pd.concat([buys, sells]).sort_values('Date').reset_index(drop=True)
        return self.orders

    #Implement a combined Bollinger Bands and RSI strategy, with buy signals when price is below lower band and RSI is oversold, and sell signals when price is above upper band and RSI is overbought
    def bollinger_rsi(self, window=20, num_std=2.0, rsi_period=14, rsi_oversold=35, rsi_overbought=65, price_col='Close'):
        query = f"""
            SELECT Date, {price_col},
                AVG({price_col}) OVER (
                   ORDER BY Date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
                ) AS sma,
                STDDEV_SAMP({price_col}) OVER (
                   ORDER BY Date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
                ) AS std
            FROM prices
        """
        df = self.conn.execute(query).fetchdf()

        df['upper'] = df['sma'] + num_std * df['std']
        df['lower'] = df['sma'] - num_std * df['std']

        delta = df[price_col].diff()
        gain  = delta.clip(lower=0)
        loss  = -delta.clip(upper=0)

        avg_gain = gain.ewm(com=rsi_period - 1, min_periods=rsi_period).mean()
        avg_loss = loss.ewm(com=rsi_period - 1, min_periods=rsi_period).mean()

        rs = avg_gain / avg_loss.replace(0, float('inf'))
        df['rsi'] = 100 - (100 / (1 + rs))

        buy_signal  = (df[price_col] <= df['lower']) & (df['rsi'] < rsi_oversold)
        sell_signal = (df[price_col] >= df['upper']) & (df['rsi'] > rsi_overbought)

        signals = []
        last_action = None
        for i in df.index:
            if buy_signal[i] and last_action != 'Buy':
                signals.append('Buy')
                last_action = 'Buy'
            elif sell_signal[i] and last_action == 'Buy':
                signals.append('Sell')
                last_action = 'Sell'
            else:
                signals.append(None)
        df['signal'] = signals

        buys  = df[df['signal'] == 'Buy'][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Buy')
        sells = df[df['signal'] == 'Sell'][['Date', price_col]].rename(columns={price_col: 'Close'}).assign(Action='Sell')

        self.orders = pd.concat([buys, sells]).sort_values('Date').reset_index(drop=True)
        return self.orders
    
    #Compute NAV history based on executed orders and price data, with simple logic to buy as many shares as possible on buy signals and sell all shares on sell signals
    def compute_nav(self, prices_df, capital=10000, price_col='Close'):
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

    #Calculate PNL metrics based on order list and price data, with a simple FIFO matching of buys and sells
    def pnl_calc(self, order_list, price_col='Close'):
        order_list = order_list.sort_values('Date').reset_index(drop=True)
        
        self.buy_orders = (order_list['Action'] == 'Buy').sum()
        self.sell_orders = (order_list['Action'] == 'Sell').sum()
        
        unmatched_buys = []
        matched_buy_prices = []
        self.total_profit = 0.0
        matched_buy_count = 0
        matched_sell_count = 0
        
        for _, order in order_list.iterrows():
            if order['Action'] == 'Buy':
                unmatched_buys.append(order[price_col])
            elif order['Action'] == 'Sell' and unmatched_buys:
                buy_price = unmatched_buys.pop(0)
                matched_buy_prices.append(buy_price)
                sell_price = order[price_col]
                self.total_profit += sell_price - buy_price
                matched_buy_count += 1
                matched_sell_count += 1
        
        self.total_profit = round(self.total_profit, 2)
        
        total_buy_value = sum(matched_buy_prices)
        if total_buy_value > 0:
            self.percent_gain = (self.total_profit / total_buy_value) * 100
        else:
            self.percent_gain = 0.0
        
        self.buy_orders = matched_buy_count
        self.sell_orders = matched_sell_count
    #Print PNL metrics in a readable format and return them as a tuple
    def show_metrics(self):
        print("\n----PNL REPORT----")
        print(f"Buy Orders: {self.buy_orders}\nSell Orders: {self.sell_orders}\nTotal Gross Profit: {self.total_profit}\nPercent Gain: {self.percent_gain:.2f}%")
        return (self.buy_orders, self.sell_orders, self.total_profit, self.percent_gain)

class Broker:
    #Simple wrapper around Alpaca API for paper trading
    def __init__(self, api_key=None, secret_key=None, paper=True):
        self.api_key = api_key or os.getenv('ALPACA_API_KEY')
        self.secret_key = secret_key or os.getenv('ALPACA_SECRET_KEY')
        if not self.api_key or not self.secret_key:
            raise ValueError('ALPACA_API_KEY and ALPACA_SECRET_KEY must be set for paper trading')
        self.client = TradingClient(self.api_key, self.secret_key, paper=paper)

    #Place a market order for the given symbol, quantity, and action (buy/sell)
    def place_market_order(self, symbol, qty, action):
        side = OrderSide.BUY if action.lower() == 'buy' else OrderSide.SELL
        order_request = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY,
        )
        return self.client.submit_order(order_request)
    
    #Get current buying power of the account
    def buying_power(self):
        return float(self.client.get_account().buying_power)

    #Get current position for a symbol, or None if no position exists
    def get_position(self, symbol):
        try:
            return self.client.get_open_position(symbol)
        except Exception:
            return None
