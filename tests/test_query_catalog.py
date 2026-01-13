import pytest
import pandas as pd
from pathlib import Path
from sinly_quant.data_prepare.data_loaders import query_from_catalog, ParquetDataCatalog, prepare_tradingview_data
from sinly_quant.constants import Columns, Venues

# Path to the real data source files
DATA_SOURCE_DIR = Path(__file__).parent.parent / "src" / "sinly_quant" / "data_prepare" / "data_source"

@pytest.fixture
def temp_catalog(tmp_path, mocker):
    """
    Creates a temporary catalog directory and patches CATALOG_PATH in data_loaders.
    This ensures tests run against a clean, isolated catalog.
    """
    # 1. Create a temp directory for the catalog
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # 2. Patch the CATALOG_PATH constant in the module under test
    # We need to patch it where it is imported/used
    mocker.patch("sinly_quant.data_prepare.data_loaders.CATALOG_PATH", catalog_dir)

    return catalog_dir

def setup_test_data(catalog_dir, symbol="GLD", interval="1-DAY"):
    """
    Helper to load real data into the temp catalog.
    """
    # We use the existing loader function to process the CSV and write to our temp catalog
    # Since we patched CATALOG_PATH, prepare_tradingview_data (via load_to_catalog logic)
    # or manual write needs to target this catalog.

    # However, prepare_tradingview_data returns the data dict, it doesn't write itself unless called by load_to_catalog.
    # But load_to_catalog instantiates ParquetDataCatalog(str(CATALOG_PATH)).
    # Since we patched CATALOG_PATH, calling load_to_catalog should work.

    # But wait, prepare_tradingview_data reads from a fixed path relative to itself.
    # We need to make sure the CSV exists there. The user said "use the test files in the data_source folder".
    # prepare_tradingview_data looks in "../data_source/TR_{symbol}_{interval}.csv".
    # This should be fine as long as the source code structure is intact.

    # Let's manually write to the catalog to be safe and explicit, reusing the logic from prepare_tradingview_data

    prepared_data = prepare_tradingview_data(symbol, Venues.NYSE, interval)

    catalog = ParquetDataCatalog(str(catalog_dir))
    catalog.write_data([prepared_data["instrument"]])
    catalog.write_data(prepared_data["bars_list"])

    return prepared_data

def test_query_from_catalog_integration(temp_catalog):
    """
    Integration test using real CSV data and a temporary Parquet catalog.
    """
    # 1. Setup data in the temp catalog
    setup_test_data(temp_catalog, symbol="GLD", interval="1-DAY")

    # 2. Run the query function
    df = query_from_catalog("GLD", "1-DAY", as_dataframe=True)

    # 3. Assertions
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.index.name == Columns.TIMESTAMP

    # Check columns exist
    expected_cols = [Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE, Columns.VOLUME]
    for col in expected_cols:
        assert col in df.columns

    # Check data types (basic check)
    assert pd.api.types.is_float_dtype(df[Columns.CLOSE])

def test_query_from_catalog_with_date_filter(temp_catalog):
    """
    Test querying with date filters.
    """
    # 1. Setup data
    setup_test_data(temp_catalog, symbol="VTI-GLD", interval="1-DAY")

    # 2. Define range (pick a range that likely exists in the sample data)
    # We need to know what's in the file. Let's read the full DF first to pick dates.
    full_df = query_from_catalog("VTI-GLD", "1-DAY", as_dataframe=True)
    start_dt = full_df.index[0]
    end_dt = full_df.index[10] # Take first 10 days

    # 3. Query with filter
    filtered_df = query_from_catalog("VTI-GLD", "1-DAY", start_datetime=start_dt, end_datetime=end_dt, as_dataframe=True)

    # 4. Assertions
    assert len(filtered_df) <= 11 # Inclusive range usually
    assert filtered_df.index[0] >= start_dt
    assert filtered_df.index[-1] <= end_dt

def test_query_from_catalog_ticker_not_found(temp_catalog):
    """
    Test error handling for missing ticker.
    """
    # Catalog is empty or has GLD, but we ask for UNKNOWN
    setup_test_data(temp_catalog, symbol="GLD", interval="1-DAY")

    with pytest.raises(ValueError, match="No instrument found for ticker 'UNKNOWN'"):
        query_from_catalog("UNKNOWN", "1-DAY", as_dataframe=True)

def test_query_from_catalog_no_data_for_interval(temp_catalog):
    """
    Test case where instrument exists but no bars for that interval.
    """
    # 1. Setup GLD 1-DAY
    setup_test_data(temp_catalog, symbol="GLD", interval="1-DAY")

    # 2. Query GLD 1-MINUTE (which shouldn't exist)
    # Note: query_from_catalog constructs BarType from string.
    # If the BarType doesn't exist in catalog, catalog.bars() returns empty generator.

    # However, query_from_catalog logic:
    # 1. Finds instrument (GLD exists)
    # 2. Constructs BarType string "GLD.NYSE-1-MINUTE-LAST-EXTERNAL"
    # 3. Queries catalog

    df = query_from_catalog("GLD", "1-MINUTE", as_dataframe=True)
    assert df.empty



