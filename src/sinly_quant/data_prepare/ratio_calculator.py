import pandas as pd
from typing import Dict, List
from sinly_quant.sinly_logger import get_logger
from sinly_quant.constants import Columns

logger = get_logger(__name__)


def calculate_ratios_from_profiles(
    market_data: Dict[str, pd.DataFrame],
    profiles: List[Dict]
) -> Dict[str, Dict]:
    """
    Generates synthetic ratio DataFrames based on configuration profiles.

    :param market_data: A dictionary where keys are instrument IDs (e.g., 'VTI', 'GLD' or 'VTI.NYSE')
                        and values are pandas DataFrames containing a 'close' column
                        and a DatetimeIndex.
    :param profiles: The list of configuration dictionaries from ratio_profile.py.
    :return: A dictionary where keys are ratio names and values are dicts containing 'df' and 'interval'.
    """
    results = {}

    for config in profiles:
        id_a = config.get("instrument_id_a")
        venue_a = config.get("venue_a")
        id_b = config.get("instrument_id_b")
        venue_b = config.get("venue_b")
        interval = config.get("interval", "1-DAY")

        # Generate ratio name based on instrument IDs
        ratio_base_name = f"{id_a}_{id_b}"
        # Use a unique key for the results dict to handle multiple intervals for the same pair
        # This also helps save_synthetic_to_catalog extract the correct symbol (everything before the last underscore)
        ratio_name = f"{ratio_base_name}_{interval}"

        # Construct potential keys for lookup.
        # We try the exact ID first, then ID.VENUE if venue is present.
        # This handles cases where market_data might be keyed by "VTI" or "VTI.NYSE".
        keys_a = [id_a]
        if venue_a:
            keys_a.append(f"{id_a}.{venue_a}")

        keys_b = [id_b]
        if venue_b:
            keys_b.append(f"{id_b}.{venue_b}")

        df_a = None
        for k in keys_a:
            if k in market_data:
                df_a = market_data[k]
                break

        df_b = None
        for k in keys_b:
            if k in market_data:
                df_b = market_data[k]
                break

        # 1. Validate inputs exist
        if df_a is None:
            logger.warning(f"Missing data for {id_a} (tried keys: {keys_a}). Skipping {ratio_name}.")
            continue
        if df_b is None:
            logger.warning(f"Missing data for {id_b} (tried keys: {keys_b}). Skipping {ratio_name}.")
            continue

        # 2. Align data on Timestamps
        # We use an inner join to ensure we only calculate the ratio
        # when BOTH instruments have a bar at that specific time.
        cols_to_merge: list[str] = [Columns.OPEN, Columns.HIGH, Columns.LOW, Columns.CLOSE]

        # Check if required columns exist
        missing_cols_a = [c for c in cols_to_merge if c not in df_a.columns]
        missing_cols_b = [c for c in cols_to_merge if c not in df_b.columns]

        if missing_cols_a or missing_cols_b:
            logger.warning(f"Missing OHLC columns for {ratio_name}. A missing: {missing_cols_a}, B missing: {missing_cols_b}. Skipping.")
            continue

        aligned = pd.merge(
            df_a[cols_to_merge],
            df_b[cols_to_merge],
            left_index=True,
            right_index=True,
            suffixes=('_a', '_b')
        )

        if aligned.empty:
            logger.warning(f"No overlapping data found for {ratio_name} between {id_a} and {id_b}.")
            continue

        # 3. Calculate Ratio (A / B)
        # Calculate ratios for open, high, low, close
        ratio_df = pd.DataFrame(index=aligned.index)

        # Open and Close are straightforward ratios
        ratio_df[Columns.OPEN] = aligned[f'{Columns.OPEN}_a'] / aligned[f'{Columns.OPEN}_b']

        # High of a ratio is maximized when numerator is highest and denominator is lowest
        ratio_df[Columns.HIGH] = aligned[f'{Columns.HIGH}_a'] / aligned[f'{Columns.LOW}_b']

        # Low of a ratio is minimized when numerator is lowest and denominator is highest
        ratio_df[Columns.LOW] = aligned[f'{Columns.LOW}_a'] / aligned[f'{Columns.HIGH}_b']

        ratio_df[Columns.CLOSE] = aligned[f'{Columns.CLOSE}_a'] / aligned[f'{Columns.CLOSE}_b']

        # Optional: Forward fill if you want to handle slight data gaps differently
        # ratio_df = ratio_df.ffill()
        results[ratio_name] = {
            "df": ratio_df,
            "interval": interval
        }
        logger.info(f"Calculated {ratio_name} with {len(ratio_df)} bars.")

    return results
