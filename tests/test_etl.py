import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

def test_etl_has_main_guard():
    etl_path = SRC / "etl.py"
    source = etl_path.read_text()
    assert 'if __name__ == "__main__":' in source

def test_etl_import_does_not_execute_main():
    etl_path = SRC / "etl.py"
    spec = importlib.util.spec_from_file_location("etl_test_module", etl_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.__name__ == "etl_test_module"
    assert not hasattr(module, "TICKER")


def test_fetch_alpaca_returns_required_columns(monkeypatch):
    from etl import fetch_alpaca

    mock_df = pd.DataFrame([{
        'Date': pd.Timestamp('2023-01-03'),
        'Open': 130.0,
        'High': 133.0,
        'Low': 129.0,
        'Close': 131.0,
        'Volume': 1000000
    }])

    class MockBars:
        def __init__(self, df):
            self.df = df

    class MockClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_stock_bars(self, request):
            return MockBars(mock_df)

    monkeypatch.setattr("etl.StockHistoricalDataClient", lambda *args, **kwargs: MockClient())

    result = fetch_alpaca('FAKE', '2023-01-01', '2023-01-05')
    assert set(['Date', 'Open', 'High', 'Low', 'Close', 'Volume']).issubset(result.columns)
