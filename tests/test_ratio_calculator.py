import pytest
import pandas as pd
from sinly_quant.data_prepare.ratio_calculator import calculate_ratios_from_profiles
from sinly_quant.constants import Columns

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

    assert "VTI_GLD_1-DAY" in results
    result = results["VTI_GLD_1-DAY"]
    ratio_df = result["df"]

    # 100/50 = 2.0, 102/50 = 2.04, etc.
    expected_first_close = 2.0
    assert ratio_df.iloc[0][Columns.CLOSE] == expected_first_close

    # Check other columns
    # Open: 99/49 ~= 2.0204
    assert abs(ratio_df.iloc[0][Columns.OPEN] - (99.0/49.0)) < 1e-6

    # High: High_A / Low_B = 101 / 48 = 2.104166...
    assert abs(ratio_df.iloc[0][Columns.HIGH] - (101.0/48.0)) < 1e-6

    # Low: Low_A / High_B = 98 / 51 = 1.921568...
    assert abs(ratio_df.iloc[0][Columns.LOW] - (98.0/51.0)) < 1e-6

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

    result = results["A_B_1-DAY"]
    ratio_df = result["df"]

    # Should only contain Jan 2 and Jan 3
    assert len(ratio_df) == 2
    assert pd.Timestamp("2023-01-01") not in ratio_df.index
    assert pd.Timestamp("2023-01-04") not in ratio_df.index
    assert pd.Timestamp("2023-01-02") in ratio_df.index

def test_calculate_ratios_missing_data(sample_market_data):
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
    assert "VTI_GLD_1-DAY" in results
    assert len(results["VTI_GLD_1-DAY"]["df"]) == 5
