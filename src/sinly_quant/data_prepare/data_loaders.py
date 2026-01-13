import os
from typing import Callable, Optional

import pandas as pd
from nautilus_trader.model.data import Bar
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.model.data import BarType
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from sinly_quant.data_prepare.instruments_providers import InstrumentProvider
from sinly_quant.util import get_absolute_path, bars_to_dataframe
from sinly_quant.constants import CATALOG_PATH, Venues, Columns


# src/sinly_quant/data_prepare/data_loaders.py

def prepare_tradingview_data(symbol_name: str, venue_name: str, interval: str) -> dict:
    """Prepare TradingView CSV data into Nautilus Trader bars and metadata.

    Args:
        symbol_name: Symbol name (e.g. "GLD").
        venue_name: Venue name (e.g. "NYSE").
        interval: Bar interval string (e.g. "1-DAY").

    Returns:
        A dict with keys: "venue_name", "instrument", "bar_type", "bars_list".
    """

    file_path = get_absolute_path(__file__, f"data_source/TR_{symbol_name}_{interval}.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"TradingView data file not found: {file_path}")

    df = pd.read_csv(file_path)

    # Handle cases where Volume is missing (5 columns: Time, Open, High, Low, Close)
    if len(df.columns) == 5:
        df.columns = [Columns.TIMESTAMP, Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE]
        df[Columns.VOLUME] = 0.0
    else:
        # Assume 6 columns: Time, Open, High, Low, Close, Volume
        df.columns = [Columns.TIMESTAMP, Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE, Columns.VOLUME]

    df = df.reindex(columns=[Columns.TIMESTAMP, Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE, Columns.VOLUME])
    df[Columns.TIMESTAMP] = pd.to_datetime(df[Columns.TIMESTAMP], format="%m/%d/%y")
    df = df.set_index(Columns.TIMESTAMP)

    # Convert price columns to numeric and round to 4 decimals
    price_cols = [Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE]
    df[price_cols] = df[price_cols].apply(pd.to_numeric, errors="coerce").round(4)

    # Ensure volume is numeric
    df[Columns.VOLUME] = pd.to_numeric(df[Columns.VOLUME], errors="coerce").fillna(0.0)

    instrument = InstrumentProvider.equity(symbol_name, venue_name)
    # Define BarType: e.g. GLD, daily, LAST
    bar_type = BarType.from_str(f"{instrument.id}-{interval}-LAST-EXTERNAL")

    wrangler = BarDataWrangler(bar_type, instrument)

    bars_list: list[Bar] = wrangler.process(df)
    prepared_data: dict = {
        "venue_name": venue_name,
        "instrument": instrument,
        "bar_type": bar_type,
        "bars_list": bars_list,
        "dataframe": df,
    }

    return prepared_data



def load_to_catalog(symbol_name: str, venue_name: str, interval: str, data_provider: str) -> Optional[pd.DataFrame]:
    """Prepare data using a named provider and persist it to the Parquet catalog.

    Args:
        symbol_name: Symbol name, e.g. "GLD".
        venue_name: Venue name, e.g. "NYSE".
        interval: Bar interval string, e.g. "1-DAY".
        data_provider: A short provider name, e.g. "tradingview" or "yfinance".
            The function name will be resolved as ``prepare_<data_provider>_data``
            in this module.

    Returns:
        The DataFrame containing the loaded data, or None if loading failed.
    """

    # Build the function name dynamically, e.g. "tradingview" -> "prepare_tradingview_data"
    func_name = f"prepare_{data_provider}_data"

    # Look up the function in this module's globals
    provider_func: Optional[Callable] = globals().get(func_name)
    if provider_func is None or not callable(provider_func):
        raise ValueError(f"Data provider function '{func_name}' not found or not callable.")

    # Call the resolved provider function to prepare the data
    prepared_data: dict = provider_func(symbol_name, venue_name, interval)

    catalog = ParquetDataCatalog(str(CATALOG_PATH))

    # Write instrument and bars to the catalog
    try:
        catalog.write_data([prepared_data["instrument"]])
        catalog.write_data(prepared_data["bars_list"])
    except ValueError as e:
        raise RuntimeError(f"Failed to write data to catalog: {e}")

    return prepared_data.get("dataframe")


def save_synthetic_to_catalog(ratio_name: str, df: pd.DataFrame, interval: str) -> None:
    """
    Save synthetic ratio data to catalog.

    Args:
        ratio_name: Name of the ratio, e.g. "vti:gld_1-DAY".
        df: DataFrame containing OHLC data.
        interval: Bar interval string, e.g. "1-DAY".
    """
    # Parse ratio_name to extract symbol and interval
    # Assuming format "symbol_interval" or just "symbol"
    if "_" in ratio_name:
        symbol_str = ratio_name.rsplit("_", 1)[0]
    else:
        symbol_str = ratio_name

    symbol_name = symbol_str.upper()
    venue_name = Venues.SYNTH

    instrument = InstrumentProvider.equity(symbol_name, venue_name)

    # Ensure volume column exists
    if Columns.VOLUME not in df.columns:
        df = df.copy()
        df[Columns.VOLUME] = 0.0

    # Define BarType
    bar_type = BarType.from_str(f"{instrument.id}-{interval}-LAST-EXTERNAL")

    # Process DataFrame into Bars
    wrangler = BarDataWrangler(bar_type, instrument)
    bars_list = wrangler.process(df)

    # Write to catalog
    catalog = ParquetDataCatalog(str(CATALOG_PATH))

    try:
        catalog.write_data([instrument])
        catalog.write_data(bars_list)
    except ValueError as e:
        raise RuntimeError(f"Failed to write synthetic data to catalog: {e}")


def query_from_catalog(
    ticker: str,
    interval: str,
    start_datetime: Optional[str | pd.Timestamp] = None,
    end_datetime: Optional[str | pd.Timestamp] = None,
    as_dataframe: bool = False,
) -> pd.DataFrame | list[Bar]:
    """
    Query bars from the catalog for a given ticker and interval.
    Automatically resolves the venue if not provided.

    Args:
        ticker: The symbol ticker (e.g. "GLD"). Case-insensitive lookup.
        interval: The bar interval (e.g. "1-DAY").
        start_datetime: Filter start time.
        end_datetime: Filter end time.
        as_dataframe: If True, returns a pandas DataFrame. If False, returns a list of Nautilus Bar objects.

    Returns:
        pd.DataFrame with OHLCV data indexed by timestamp, or a list of Bar objects.
    """
    catalog = ParquetDataCatalog(str(CATALOG_PATH))

    # 1. Resolve Instrument (Handle Venue automatically)
    # Get all instruments and filter by symbol
    all_instruments = catalog.instruments()

    # Filter case-insensitive
    matches = [
        i for i in all_instruments
        if i.id.symbol.value.upper() == ticker.upper()
    ]

    if not matches:
        raise ValueError(f"No instrument found for ticker '{ticker}' in catalog.")

    # Heuristic: If multiple, prefer the one that matches the ticker exactly if possible,
    # or just take the first one and warn.
    instrument = matches[0]
    if len(matches) > 1:
        print(f"Warning: Multiple instruments found for '{ticker}': {[i.id for i in matches]}. Using '{instrument.id}'.")

    # 2. Resolve BarType
    # We construct the BarType using the found instrument and requested interval.
    # We assume the standard convention: {instrument_id}-{interval}-LAST-EXTERNAL
    try:
        target_bar_type = BarType.from_str(f"{instrument.id}-{interval}-LAST-EXTERNAL")
    except Exception as e:
        raise ValueError(f"Could not construct BarType for '{instrument.id}' and '{interval}': {e}")

    # 3. Load Data
    start_ns = pd.Timestamp(start_datetime).value if start_datetime else None
    end_ns = pd.Timestamp(end_datetime).value if end_datetime else None

    # catalog.bars returns a generator of Bar objects
    bars_generator = catalog.bars(bar_types=[str(target_bar_type)], start=start_ns, end=end_ns)

    if not as_dataframe:
        return list(bars_generator)

    # 4. Convert to DataFrame
    return bars_to_dataframe(bars_generator)


