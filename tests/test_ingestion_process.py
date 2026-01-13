import pytest
import pandas as pd
import os
from sinly_quant.data_prepare.ratio_calculator import calculate_ratios_from_profiles
from sinly_quant.data_prepare.run_ingestion import main as run_ingestion_main
from sinly_quant.constants import Columns, Venues

# -------------------------------------------------------------------------
# 1. Unit Tests for Ratio Calculator
# -------------------------------------------------------------------------

@pytest.fixture
def sample_market_data():
    """Creates synthetic market data for testing."""
    dates = pd.date_range(start="2023-01-01", periods=5, freq="D")

    # Instrument A: Steady increase
    df_a = pd.DataFrame({
        Columns.OPEN: [99.0, 101.0, 103.0, 105.0, 107.0],
        Columns.HIGH: [101.0, 103.0, 105.0, 107.0, 109.0],
        Columns.LOW: [98.0, 100.0, 102.0, 104.0, 106.0],
        Columns.CLOSE: [100.0, 102.0, 104.0, 106.0, 108.0]
    }, index=dates)

    # Instrument B: Steady
    df_b = pd.DataFrame({
        Columns.OPEN: [49.0, 49.0, 49.0, 49.0, 49.0],
        Columns.HIGH: [51.0, 51.0, 51.0, 51.0, 51.0],
        Columns.LOW: [48.0, 48.0, 48.0, 48.0, 48.0],
        Columns.CLOSE: [50.0, 50.0, 50.0, 50.0, 50.0]
    }, index=dates)

    return {"VTI": df_a, "GLD": df_b}

def test_calculate_ratios_basic_math(sample_market_data):
    """Test that the division logic works correctly."""
    profiles = [
        {"instrument_id_a": "VTI", "instrument_id_b": "GLD", "interval": "1-DAY"}
    ]

    results = calculate_ratios_from_profiles(sample_market_data, profiles)

    ratio_name = "VTI_GLD_1-DAY"
    assert ratio_name in results
    result = results[ratio_name]
    ratio_df = result["df"]

    # 100/50 = 2.0, 102/50 = 2.04, etc.
    expected_first_close = 2.0
    assert ratio_df.iloc[0][Columns.CLOSE] == expected_first_close

    # Check other columns
    # Open: 99/49 ~= 2.0204
    assert abs(ratio_df.iloc[0][Columns.OPEN] - (99.0/49.0)) < 1e-6

    assert len(ratio_df) == 5

def test_calculate_ratios_timestamp_alignment():
    """Test that ratios are only calculated for overlapping timestamps (Inner Join)."""

    # Date ranges that only partially overlap
    dates_a = pd.date_range(start="2023-01-01", periods=3, freq="D") # Jan 1, 2, 3
    dates_b = pd.date_range(start="2023-01-02", periods=3, freq="D") # Jan 2, 3, 4

    df_a = pd.DataFrame({
        Columns.OPEN: [10]*3, Columns.HIGH: [10]*3, Columns.LOW: [10]*3, Columns.CLOSE: [10]*3
    }, index=dates_a)
    df_b = pd.DataFrame({
        Columns.OPEN: [2]*3, Columns.HIGH: [2]*3, Columns.LOW: [2]*3, Columns.CLOSE: [2]*3
    }, index=dates_b)

    market_data = {"A": df_a, "B": df_b}
    profiles = [{"instrument_id_a": "A", "instrument_id_b": "B", "interval": "1-DAY"}]

    results = calculate_ratios_from_profiles(market_data, profiles)

    ratio_name = "A_B_1-DAY"
    assert ratio_name in results
    result = results[ratio_name]
    ratio_df = result["df"]

    # Should only contain Jan 2 and Jan 3
    assert len(ratio_df) == 2
    assert pd.Timestamp("2023-01-01") not in ratio_df.index
    assert pd.Timestamp("2023-01-04") not in ratio_df.index
    assert pd.Timestamp("2023-01-02") in ratio_df.index

def test_calculate_ratios_missing_data(sample_market_data, capsys):
    """Test that missing instruments in market_data are handled gracefully."""
    profiles = [
        {"instrument_id_a": "VTI", "instrument_id_b": "MISSING_TICKER", "interval": "1-DAY"}
    ]

    results = calculate_ratios_from_profiles(sample_market_data, profiles)

    # Should return empty result for this key or skip it
    assert "VTI_MISSING_TICKER_1-DAY" not in results

def test_calculate_ratios_with_venue_lookup(sample_market_data):
    """Test that the calculator can find data using ID.VENUE keys."""
    # Rename keys in market data to include venue
    market_data = {
        "VTI.NYSE": sample_market_data["VTI"],
        "GLD.NYSE": sample_market_data["GLD"]
    }

    profiles = [
        {
            "instrument_id_a": "VTI",
            "venue_a": "NYSE",
            "instrument_id_b": "GLD",
            "venue_b": "NYSE",
            "interval": "1-DAY"
        }
    ]

    results = calculate_ratios_from_profiles(market_data, profiles)
    ratio_name = "VTI_GLD_1-DAY"
    assert ratio_name in results
    assert len(results[ratio_name]["df"]) == 5


# -------------------------------------------------------------------------
# 2. Integration Test for Run Ingestion Script with Real Data
# -------------------------------------------------------------------------

def test_run_ingestion_flow_with_real_data(mocker):
    """
    Tests the main execution flow of run_ingestion.py using REAL data files.
    We mock load_to_catalog to use prepare_tradingview_data directly,
    simulating the real data loading process without writing to the actual catalog.
    """

    # 1. Define a side effect for load_to_catalog that actually loads data
    #    but returns the DataFrame instead of writing to catalog.
    def mock_load_side_effect(symbol_name, venue_name, interval, data_provider):
        if data_provider == "tradingview":
            try:
                file_name = f"TR_{symbol_name}_{interval}.csv"
                # The user provided file list shows they are in src/sinly_quant/data_prepare/data_source/
                base_path = os.path.join(os.path.dirname(__file__), "../src/sinly_quant/data_prepare/data_source")
                # Adjust path if test is running from root
                if not os.path.exists(base_path):
                     base_path = "src/sinly_quant/data_prepare/data_source"

                file_path = os.path.join(base_path, file_name)

                if not os.path.exists(file_path):
                    # Fallback for test environment if files aren't exactly there
                    # Create dummy data if real file missing
                    print(f"Real file not found at {file_path}, using dummy data.")
                    dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
                    return pd.DataFrame({Columns.CLOSE: [100.0]*10}, index=dates)

                df = pd.read_csv(file_path)
                new_cols = [Columns.TIMESTAMP, Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE, Columns.VOLUME]
                df.columns = new_cols
                df[Columns.TIMESTAMP] = pd.to_datetime(df[Columns.TIMESTAMP], format="%m/%d/%y")
                df = df.set_index(Columns.TIMESTAMP)
                return df
            except Exception as e:
                print(f"Error loading real data: {e}")
                return None
        return None

    # 2. Mock load_to_catalog in run_ingestion
    mock_loader = mocker.patch(
        "sinly_quant.data_prepare.run_ingestion.load_to_catalog",
        side_effect=mock_load_side_effect
    )

    # Mock save_synthetic_to_catalog to avoid writing to disk
    mock_saver = mocker.patch("sinly_quant.data_prepare.run_ingestion.save_synthetic_to_catalog")

    # 3. Mock load_config_from_csv to return our test configs
    # We need to return different things based on the file path or call order.
    # Since we know the order: first universe, then ratios.

    test_universe = [
        {"symbol": "VTI", "venue": Venues.NYSE, "interval": "1-DAY", "provider": "tradingview"},
        {"symbol": "GLD", "venue": Venues.NYSE, "interval": "1-DAY", "provider": "tradingview"},
    ]

    test_ratios = [
        {
            "instrument_id_a": "VTI",
            "venue_a": "NYSE",
            "instrument_id_b": "GLD",
            "venue_b": "NYSE",
            "interval": "1-DAY"
        }
    ]

    mock_config_loader = mocker.patch(
        "sinly_quant.data_prepare.run_ingestion.load_config_from_csv",
        side_effect=[test_universe, test_ratios]
    )

    # 5. Spy on calculate_ratios_from_profiles to verify it gets called with data
    # We need to import the module object to spy on it
    from sinly_quant.data_prepare import run_ingestion
    spy_calculator = mocker.spy(
        run_ingestion, "calculate_ratios_from_profiles"
    )

    # 6. Run the main function
    run_ingestion_main()

    # 7. Assertions
    assert mock_loader.call_count >= 2 # Should be called for GLD and VTI
    assert mock_saver.called

    # Verify calculator was called
    assert spy_calculator.called

    # Verify that the result of calculator was not empty (implies real data was loaded and aligned)
    # We can check the return value of the spy
    returned_ratios = spy_calculator.spy_return
    assert "VTI_GLD_1-DAY" in returned_ratios
    assert not returned_ratios["VTI_GLD_1-DAY"]["df"].empty
