import pytest
from unittest.mock import MagicMock
import pandas as pd

from nautilus_trader.model.data import Bar

from sinly_quant.my_indicators.swing_levels import SwingLevels
from sinly_quant.data_prepare.data_loaders import query_from_catalog
from sinly_quant.util import bars_to_dataframe


# -------------------------------------------------------------------------
# Pytest Fixtures and Tests
# -------------------------------------------------------------------------
@pytest.fixture
def real_data_vti_gld():
    """Fixture to load real market data for VTI and GLD."""
    vti_1d = query_from_catalog("VTI", "1-DAY", pd.Timestamp("2007-01-01"), pd.Timestamp("2025-12-21") )
    gld_d = query_from_catalog("GLD", "1-DAY", pd.Timestamp("2007-01-01"), pd.Timestamp("2025-12-21") )

    vti_1w = query_from_catalog("VTI", "1-WEEK", pd.Timestamp("2007-01-01"), pd.Timestamp("2025-12-21"))
    gld_1w = query_from_catalog("GLD", "1-WEEK", pd.Timestamp("2007-01-01"), pd.Timestamp("2025-12-21"))

    vti_gld_1d = query_from_catalog("VTI-GLD", "1-DAY", pd.Timestamp("2007-01-01"), pd.Timestamp("2025-12-21"))
    vti_gld_1w = query_from_catalog("VTI-GLD", "1-WEEK", pd.Timestamp("2007-01-01"), pd.Timestamp("2025-12-21"))

    return vti_1d, gld_d, vti_1w, gld_1w, vti_gld_1d, vti_gld_1w



@pytest.fixture
def mock_bar_factory():
    """Fixture that returns a function to create mock Bar objects."""

    def _create_mock_bar(high: float, low: float):
        bar = MagicMock(spec=Bar)
        # Mock the chain: bar.high.as_double() -> float
        bar.high.as_double.return_value = float(high)
        bar.low.as_double.return_value = float(low)
        return bar

    return _create_mock_bar


def test_pivot_high_detection(mock_bar_factory):
    """
    Test a standard Pivot High scenario.
    Left=2, Right=2. Window=5.
    Pattern: 10, 20, 50, 20, 10
    Pivot should be detected at the 5th bar (value 50).
    """
    indicator = SwingLevels(swing_size_l=2, swing_size_r=2)

    highs = [
        10, 20, 50, 20, 10, 15, 18, 12, 25, 30,
        28, 22, 19, 21, 24, 20, 15, 10, 12, 14
    ]
    lows = [
        5, 5, 5, 5, 5, 8, 10, 6, 15, 20,
        18, 12, 10, 12, 15, 12, 8, 5, 6, 8
    ]

    # Feed first 4 bars (not enough data for right side yet)
    for i in range(4):
        indicator.handle_bar(mock_bar_factory(highs[i], lows[i]))
        assert indicator.pivot_high is None, f"Bar {i + 1} should not trigger pivot yet"
        assert indicator.pivot_low is None, f"Bar {i + 1} should not trigger pivot yet"

    # Feed 5th bar
    indicator.handle_bar(mock_bar_factory(highs[4], lows[4]))
    assert indicator.pivot_high is not None
    assert indicator.pivot_high == 50.0

    for i in range(5, 12):
        indicator.handle_bar(mock_bar_factory(highs[i], lows[i]))
        if i == 11:
            assert indicator.pivot_high == 30
        if i == 9:
            assert indicator.pivot_low == 6
        if i == 14:
            assert indicator.pivot_low == 10
        if i == 19:
            assert indicator.pivot_low == 9


def test_flat_line_ignored(mock_bar_factory):
    """
    Test that a flat line does not trigger pivots.
    Pine Script logic requires strictly greater/lower than at least one other bar.
    """
    indicator = SwingLevels(swing_size_l=2, swing_size_r=2)

    # Flat line of 100s
    for _ in range(10):
        indicator.handle_bar(mock_bar_factory(100, 100))
        assert indicator.pivot_high is None
        assert indicator.pivot_low is None


def test_future_invalidation(mock_bar_factory):
    """
    Test that if a future bar is higher, the pivot is NOT formed.
    Left=2, Right=2.
    Pattern: 10, 20, 50, 20, 60
    The 50 looks like a pivot until the 60 comes in at the very end of the window.
    """
    indicator = SwingLevels(swing_size_l=2, swing_size_r=2)

    # Window will be: [10, 20, 50, 20, 60]
    # Candidate is 50. Max is 60. 50 != 60, so no pivot.
    highs = [10, 20, 50, 20, 60]

    for h in highs:
        indicator.handle_bar(mock_bar_factory(h, 5))

    assert indicator.pivot_high is None


def test_vti_gld(real_data_vti_gld):
    vti_1d, gld_d, vti_1w, gld_1w, vti_gld_1d, vti_gld_1w = real_data_vti_gld

    indicator = SwingLevels(swing_size_l=15, swing_size_r=3)

    for i in range(len(vti_gld_1w)):
        indicator.handle_bar(vti_gld_1w[i])
    pivot_high_history = bars_to_dataframe(indicator.pivot_high_history)
    pivot_low_history = bars_to_dataframe(indicator.pivot_low_history)

    print(f"\nPivot Highs: \n {pivot_high_history.index.values}")
    print(f"\nPivot Lows: \n {pivot_low_history.index.values}")


