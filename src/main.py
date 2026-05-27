import json
import os

import duckdb
import pandas as pd

import backtest
import visualization

CONFIG_FILE = "config.json"

#Load config file for strategy generation
def load_config(config_file=CONFIG_FILE):
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    with open(config_file, 'r') as f:
        return json.load(f)

def main():
    config = load_config()
    db_path = config.get('database')
    if not db_path:
        raise ValueError("Database path missing from config")

    #Create db connection for data analysis
    conn = duckdb.connect(db_path)
    prices_df = pd.read_sql("SELECT * FROM prices ORDER BY Date", conn)
    benchmark_df = pd.read_sql("SELECT * FROM benchmark ORDER BY Date", conn)

    #Determine price column
    price_col = 'Close' if 'Close' in prices_df.columns else next((col for col in prices_df.columns if 'Close' in col), 'Close')
    prices_df['Date'] = pd.to_datetime(prices_df['Date'])
    benchmark_df['Date'] = pd.to_datetime(benchmark_df['Date'])

    strategy = backtest.Strategy(conn)

    #Strategy selection based on config
    STRATEGY = config.get('strategy', 'bollinger_rsi') #Default strategy held in second function parameter
    if STRATEGY == 'moving_averages':
        orders = strategy.moving_averages(
            short_window=config.get('short_window', 10),
            long_window=config.get('long_window', 20),
            price_col=price_col
        )
    elif STRATEGY == 'bollinger_rsi':
        orders = strategy.bollinger_rsi(
            window=config.get('bb_window', 20),
            num_std=config.get('bb_std', 2.0),
            rsi_period=config.get('rsi_period', 14),
            rsi_oversold=config.get('rsi_oversold', 35),
            rsi_overbought=config.get('rsi_overbought', 65),
            price_col=price_col
        )
    else:
        raise ValueError(f"Unknown strategy: {STRATEGY}")

    #Ensure Date column is datetime for backtesting calculations
    orders['Date'] = pd.to_datetime(orders['Date'])

    #Calculate PnL and NAV for strategy, buy-and-hold, and benchmark
    portfolio = backtest.Portfolio()
    portfolio.pnl_calc(orders)

    strategy_nav = strategy.compute_nav(prices_df, capital=10000, price_col=price_col)
    buy_hold_nav = visualization.compute_buy_and_hold_nav(prices_df, capital=10000, price_col=price_col)
    benchmark_nav = visualization.compute_buy_and_hold_nav(benchmark_df, capital=10000, price_col='Close')

    #Generate visualizations of strategy performance vs buy-and-hold and benchmark
    visualization.plot_results(
        prices_df=prices_df,
        benchmark_df=benchmark_df,
        orders=orders,
        portfolio=portfolio,
        config=config,
        strategy_nav=strategy_nav,
        buy_hold_nav=buy_hold_nav,
        benchmark_nav=benchmark_nav,
        capital=10000,
        price_col=price_col,
    )
    strategy.close_connection()

if __name__ == "__main__":
    main()