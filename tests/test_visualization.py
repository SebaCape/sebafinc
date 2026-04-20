from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
VISUALIZATION_FILE = SRC / "visualization.py"


def test_visualization_file_exists():
    assert VISUALIZATION_FILE.exists()


def test_visualization_uses_existing_connection():
    content = VISUALIZATION_FILE.read_text()
    assert "backtest.Strategy(conn)" in content


def test_visualization_avoids_redundant_dataframe_conversion():
    content = VISUALIZATION_FILE.read_text()
    assert "orders_df = pd.DataFrame(orders)" not in content


def test_visualization_closes_database_connection():
    content = VISUALIZATION_FILE.read_text()
    assert "conn.close()" in content
