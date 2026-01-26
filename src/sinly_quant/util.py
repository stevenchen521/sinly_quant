from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from nautilus_trader.model.data import Bar

from sinly_quant.constants import Columns

def get_absolute_path(file, relative_path):
    return Path(file).parent.joinpath(relative_path).resolve()


def get_timestamp_suffix() -> str:
    """
    Returns a timestamp string suitable for filenames (YYYYMMDD_HHMMSS).
    Precision to seconds as requested.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def unix_to_iso_date(timestamp: int, unit: str = "ns") -> str:
    """Convert Unix timestamp to ISO date string 'YYYY-MM-DD'."""
    if unit == "ms":
        timestamp = timestamp / 1000
    elif unit == "us":
        timestamp = timestamp / 1_000_000
    elif unit == "ns":
        timestamp = timestamp / 1_000_000_000

    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.date().isoformat()


def bars_to_dataframe(bars: list[Bar]) -> "pd.DataFrame":

    records = []
    for bar in bars:
        records.append({
            Columns.TIMESTAMP: pd.Timestamp(bar.ts_event, unit="ns", tz="UTC"),
            Columns.OPEN: bar.open.as_double(),
            Columns.HIGH: bar.high.as_double(),
            Columns.LOW: bar.low.as_double(),
            Columns.CLOSE: bar.close.as_double(),
            Columns.VOLUME: bar.volume.as_double(),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df.set_index(Columns.TIMESTAMP, inplace=True)
    return df
