import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import duckdb
import pandas as pd
import pytest

from backtest import Portfolio, Strategy

def create_price_db(rows, db_path):
    conn = duckdb.connect(db_path)
    df = pd.DataFrame(rows)
    conn.execute("CREATE OR REPLACE TABLE prices AS SELECT * FROM df")
    conn.close()

def test_pnl_single_buy_sell():
    orders = pd.DataFrame([
        {"Date": "2020-01-01", "Close": 100.0, "Action": "Buy"},
        {"Date": "2020-01-02", "Close": 120.0, "Action": "Sell"},
    ])

    portfolio = Portfolio()
    portfolio.pnl_calc(orders)

    assert portfolio.buy_orders == 1
    assert portfolio.sell_orders == 1
    assert portfolio.total_profit == 20.0
    assert pytest.approx(portfolio.percent_gain, rel=1e-6) == 20.0

def test_pnl_losing_trade():
    orders = pd.DataFrame([
        {"Date": "2020-01-01", "Close": 150.0, "Action": "Buy"},
        {"Date": "2020-01-02", "Close": 120.0, "Action": "Sell"},
    ])

    portfolio = Portfolio()
    portfolio.pnl_calc(orders)

    assert portfolio.buy_orders == 1
    assert portfolio.sell_orders == 1
    assert portfolio.total_profit == -30.0
    assert pytest.approx(portfolio.percent_gain, rel=1e-6) == -20.0

def test_pnl_no_orders():
    orders = pd.DataFrame(columns=["Date", "Close", "Action"])

    portfolio = Portfolio()
    portfolio.pnl_calc(orders)

    assert portfolio.buy_orders == 0
    assert portfolio.sell_orders == 0
    assert portfolio.total_profit == 0.0
    assert portfolio.percent_gain == 0.0

def test_pnl_unmatched_buy():
    orders = pd.DataFrame([
        {"Date": "2020-01-01", "Close": 100.0, "Action": "Buy"},
    ])

    portfolio = Portfolio()
    portfolio.pnl_calc(orders)

    assert portfolio.buy_orders == 0
    assert portfolio.sell_orders == 0
    assert portfolio.total_profit == 0.0
    assert portfolio.percent_gain == 0.0

def test_pnl_respects_capital():
    orders = pd.DataFrame([
        {"Date": "2020-01-01", "Close": 110.0, "Action": "Sell"},
        {"Date": "2020-01-02", "Close": 100.0, "Action": "Buy"},
        {"Date": "2020-01-03", "Close": 120.0, "Action": "Sell"},
    ])

    portfolio = Portfolio()
    portfolio.pnl_calc(orders)

    assert portfolio.buy_orders == 1
    assert portfolio.sell_orders == 1
    assert portfolio.total_profit == 20.0
    assert pytest.approx(portfolio.percent_gain, rel=1e-6) == 20.0

def test_moving_averages_returns_df(tmp_path):
    db_path = tmp_path / "prices.db"
    rows = [
        {"Date": "2020-01-01", "Close": 1.0},
        {"Date": "2020-01-02", "Close": 2.0},
        {"Date": "2020-01-03", "Close": 3.0},
    ]
    create_price_db(rows, str(db_path))

    conn = duckdb.connect(str(db_path))
    strategy = Strategy(conn)
    orders = strategy.moving_averages(short_window=2, long_window=3, price_col='Close')
    conn.close()

    assert isinstance(orders, pd.DataFrame)
    assert set(['Date', 'Close', 'Action']).issubset(orders.columns)

def test_moving_averages_crossover_detected(tmp_path):
    db_path = tmp_path / "prices.db"
    rows = [
        {"Date": "2020-01-01", "Close": 1.0},
        {"Date": "2020-01-02", "Close": 2.0},
        {"Date": "2020-01-03", "Close": 3.0},
        {"Date": "2020-01-04", "Close": 2.0},
        {"Date": "2020-01-05", "Close": 1.0},
    ]
    create_price_db(rows, str(db_path))

    conn = duckdb.connect(str(db_path))
    strategy = Strategy(conn)
    orders = strategy.moving_averages(short_window=2, long_window=3, price_col='Close')
    conn.close()

    actions = orders['Action'].value_counts().to_dict()
    assert actions.get('Buy', 0) >= 1
    assert actions.get('Sell', 0) >= 1

def test_moving_averages_flat_prices(tmp_path):
    db_path = tmp_path / "prices.db"
    rows = [
        {"Date": f"2020-01-{str(i).zfill(2)}", "Close": 5.0} for i in range(1, 6)
    ]
    create_price_db(rows, str(db_path))

    conn = duckdb.connect(str(db_path))
    strategy = Strategy(conn)
    orders = strategy.moving_averages(short_window=2, long_window=3, price_col='Close')
    conn.close()

    assert orders.empty


def test_strategy_compute_nav(tmp_path):
    db_path = tmp_path / "prices.db"
    rows = [
        {"Date": "2020-01-01", "Close": 10.0},
        {"Date": "2020-01-02", "Close": 12.0},
        {"Date": "2020-01-03", "Close": 11.0},
    ]
    create_price_db(rows, str(db_path))

    conn = duckdb.connect(str(db_path))
    strategy = Strategy(conn)
    strategy.orders = pd.DataFrame([
        {"Date": "2020-01-01", "Close": 10.0, "Action": "Buy"},
        {"Date": "2020-01-03", "Close": 11.0, "Action": "Sell"},
    ])

    prices_df = pd.DataFrame(rows)
    nav = strategy.compute_nav(prices_df, capital=1000, price_col='Close')
    conn.close()

    assert list(nav['NAV']) == [1000.0, 1200.0, 1100.0]
