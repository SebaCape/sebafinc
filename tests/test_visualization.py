from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
VISUALIZATION_FILE = SRC / "visualization.py"

def test_visualization_file_exists():
    assert VISUALIZATION_FILE.exists()

def test_visualization_exposes_plot_results():
    content = VISUALIZATION_FILE.read_text()
    assert "def plot_results" in content

def test_visualization_exposes_compute_buy_and_hold_nav():
    content = VISUALIZATION_FILE.read_text()
    assert "def compute_buy_and_hold_nav" in content

def test_visualization_no_top_level_database_connection():
    content = VISUALIZATION_FILE.read_text()
    assert "duckdb.connect" not in content

def test_visualization_avoids_redundant_dataframe_conversion():
    content = VISUALIZATION_FILE.read_text()
    assert "orders_df = pd.DataFrame(orders)" not in content
