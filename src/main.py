import json
import os

import duckdb
import pandas as pd

import backtest
import visualization

CONFIG_FILE = "config.json"

#Load configuration from JSON file, with error handling for missing file or invalid format
def load_config(config_file=CONFIG_FILE):
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    with open(config_file, 'r') as f:
        return json.load(f)

#Run backtest using strategy defined in config, with error handling for missing database or invalid strategy
def run_backtest(config):
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
    try:
        strategy_name = config.get('strategy', 'bollinger_rsi')
        if strategy_name == 'moving_averages':
            orders = strategy.moving_averages(
                short_window=config.get('short_window', 10),
                long_window=config.get('long_window', 20),
                price_col=price_col,
            )
        elif strategy_name == 'bollinger_rsi':
            orders = strategy.bollinger_rsi(
                window=config.get('bb_window', 20),
                num_std=config.get('bb_std', 2.0),
                rsi_period=config.get('rsi_period', 14),
                rsi_oversold=config.get('rsi_oversold', 35),
                rsi_overbought=config.get('rsi_overbought', 65),
                price_col=price_col,
            )
        else:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        orders['Date'] = pd.to_datetime(orders['Date'])

        portfolio = backtest.Portfolio()
        portfolio.pnl_calc(orders)

        strategy_nav = strategy.compute_nav(prices_df, capital=10000, price_col=price_col)
        buy_hold_nav = visualization.compute_buy_and_hold_nav(prices_df, capital=10000, price_col=price_col)
        benchmark_nav = visualization.compute_buy_and_hold_nav(benchmark_df, capital=10000, price_col='Close')

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
    finally:
        strategy.close_connection()

#Run paper trading using latest signal from strategy
def run_paper_trading(config):
    db_path = config.get('database')
    if not db_path:
        raise ValueError("Database path missing from config")

    conn = duckdb.connect(db_path)
    strategy = backtest.Strategy(conn)
    try:
        strategy_name = config.get('strategy', 'bollinger_rsi')
        if strategy_name == 'moving_averages':
            orders = strategy.moving_averages(
                short_window=config.get('short_window', 10),
                long_window=config.get('long_window', 20),
                price_col='Close',
                )
        elif strategy_name == 'bollinger_rsi':
            orders = strategy.bollinger_rsi(
                window=config.get('bb_window', 20),
                num_std=config.get('bb_std', 2.0),
                rsi_period=config.get('rsi_period', 14),
                rsi_oversold=config.get('rsi_oversold', 35),
                rsi_overbought=config.get('rsi_overbought', 65),
                price_col='Close',
                )
        else:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        if orders.empty:
            print("No paper trading signals generated.")
            return

        #Check if latest signal is recent enough to act on, with a threshold of 1 day
        latest_signal = orders.sort_values('Date').iloc[-1]
        days_old = (pd.Timestamp('today') - latest_signal['Date']).days
        if days_old > 20:
            print(f"Latest signal is {days_old} days old — no action.")
            return

        #Extract action and symbol from latest signal and submit paper trade
        action = latest_signal['Action']
        symbol = config.get('ticker')
        broker = backtest.Broker()

        #Risk management logic to avoid overtrading
        if action == 'Buy':
            position = broker.get_position(symbol)
            if position:
                print(f"Already holding {symbol} — skipping buy")
                return
            qty = int(broker.buying_power() * 0.95 // latest_signal['Close'])
            order = broker.place_market_order(symbol=symbol, qty=qty, action='buy')

        #Sell logic to close entire position on sell signal
        elif action == 'Sell':
            position = broker.get_position(symbol)
            if not position:
                print(f"No position in {symbol} to sell")
                return
            qty = int(float(position.qty))
            order = broker.place_market_order(symbol=symbol, qty=qty, action='sell')

        #Handle unexpected action values gracefully
        else:
            print(f"Unexpected action: {action} — no trade submitted")
            return

        print(f"Order submitted: {order.id}")

    finally:
        strategy.close_connection()

#Main function to load config and run backtest or paper trading based on mode specified in config
def main():
    config = load_config()
    mode = config.get('mode', 'backtest')

    if mode == 'paper':
        run_paper_trading(config)
    else:
        run_backtest(config)


if __name__ == "__main__":
    main()