import json
import os

import duckdb
import pandas as pd

import backtest
import visualization

CONFIG_FILE = "config.json"


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

    conn = duckdb.connect(db_path)

    prices_df = pd.read_sql("SELECT * FROM prices ORDER BY Date", conn)
    benchmark_df = pd.read_sql("SELECT * FROM benchmark ORDER BY Date", conn)

    price_col = 'Close' if 'Close' in prices_df.columns else next((col for col in prices_df.columns if 'Close' in col), 'Close')
    prices_df['Date'] = pd.to_datetime(prices_df['Date'])
    benchmark_df['Date'] = pd.to_datetime(benchmark_df['Date'])

    strategy = backtest.Strategy(conn)
    orders = strategy.moving_averages(price_col=price_col)
    orders['Date'] = pd.to_datetime(orders['Date'])

    portfolio = backtest.Portfolio()
    portfolio.pnl_calc(orders)

    strategy_nav = strategy.compute_nav(prices_df, capital=10_000, price_col=price_col)
    buy_hold_nav = visualization.compute_buy_and_hold_nav(prices_df, capital=10_000, price_col=price_col)
    benchmark_nav = visualization.compute_buy_and_hold_nav(benchmark_df, capital=10_000, price_col='Close')

    visualization.plot_results(
        prices_df=prices_df,
        benchmark_df=benchmark_df,
        orders=orders,
        portfolio=portfolio,
        config=config,
        strategy_nav=strategy_nav,
        buy_hold_nav=buy_hold_nav,
        benchmark_nav=benchmark_nav,
        capital=10_000,
        price_col=price_col,
    )

    strategy.close_connection()


if __name__ == "__main__":
    main()
