from pathlib import Path

def get_catalog_path():
    current_file = Path(__file__).resolve()
    project_root = current_file.parent
    catalog_path = project_root / "catalog"
    return catalog_path
CATALOG_PATH = get_catalog_path()


def get_results_path():
    current_file = Path(__file__).resolve()
    project_root = current_file.parent
    results_path = project_root / "results"
    results_path.mkdir(parents=True, exist_ok=True)
    return results_path
RESULTS_PATH = get_results_path()


class Venues:
    NYSE = "NYSE"
    SYNTH = "SYNTH"
    BINANCE = "BINANCE"
    TRADINGVIEW = "TRADINGVIEW"


class Columns:
    TIMESTAMP = "timestamp"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"
