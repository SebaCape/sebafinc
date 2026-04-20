import importlib.util
from pathlib import Path

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
