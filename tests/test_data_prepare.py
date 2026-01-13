from nautilus_trader.persistence.catalog import ParquetDataCatalog
from sinly_quant.data_prepare.data_loaders import load_to_catalog
from sinly_quant.constants import Venues


def test_load_to_catalog_tradingview(mocker, tmp_path):
    """
    Integration-style test for loading GLD TradingView data.
    Uses a temporary directory to avoid polluting the real catalog.
    """
    symbol = "GLD"
    venue = Venues.NYSE
    interval = "1-DAY"

    # 1. Patch CATALOG_PATH in the data_loaders module to use the temporary test directory
    mocker.patch("sinly_quant.data_prepare.data_loaders.CATALOG_PATH", tmp_path)

    # 2. Run the function
    load_to_catalog(symbol, venue, interval, data_provider="tradingview")

    # 3. Verify the temporary catalog was created
    assert tmp_path.exists()

    # 4. Verify contents robustly
    test_catalog = ParquetDataCatalog(str(tmp_path))
    loaded_instruments = test_catalog.instruments()

    # Check if GLD is present in ANY of the loaded instruments, not just the first one
    instrument_ids = [i.id.value for i in loaded_instruments]
    assert f"{symbol}.{venue}" in instrument_ids
