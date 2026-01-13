import pandas as pd
import os
from sinly_quant.data_prepare.data_loaders import load_to_catalog, save_synthetic_to_catalog
from sinly_quant.data_prepare.ratio_calculator import calculate_ratios_from_profiles
from sinly_quant.sinly_logger import get_logger
from sinly_quant.util import get_absolute_path

logger = get_logger(__name__)


def load_config_from_csv(file_path: str) -> list[dict]:
    """Generic loader for universe or ratio configs."""
    if not os.path.exists(file_path):
        logger.error(f"Config file not found: {file_path}")
        return []

    # Read CSV and convert to list of dicts
    return pd.read_csv(file_path).to_dict(orient="records")


def main():
    # 1. Configuration for raw instruments
    universe_path = get_absolute_path(__file__, "universe.csv")
    instruments_config = load_config_from_csv(universe_path)

    if not instruments_config:
        logger.warning("No instruments found in universe.csv")
        return

    # Dictionary to hold DataFrames for the ratio calculator
    # key: symbol (e.g., 'VTI'), value: DataFrame
    market_data_cache = {}

    logger.info(f"Starting ingestion for {len(instruments_config)} instruments...")

    # 2. Ingest Raw Data
    for config in instruments_config:
        try:
            logger.info(f"Processing {config['symbol']}...")

            # Assuming load_to_catalog handles fetching and saving to disk/db.
            # Ideally, modify load_to_catalog to RETURN the dataframe it just loaded,
            # or use a separate reader here to load what you just saved.
            df = load_to_catalog(
                symbol_name=config["symbol"],
                venue_name=config["venue"],
                interval=config["interval"],
                data_provider=config["provider"]
            )

            # Store in cache for ratio calculation (ensure keys match what is in ratio_profile.py)
            # If load_to_catalog doesn't return a DF, you must read it back from disk here.
            if isinstance(df, pd.DataFrame):
                market_data_cache[config["symbol"]] = df

            logger.info(f"Successfully loaded {config['symbol']}.")
        except Exception as e:
            logger.error(f"Failed to load {config['symbol']}: {e}")

    # 3. Calculate Ratios
    logger.info("Starting ratio calculations...")
    if not market_data_cache:
        logger.warning("No market data available for ratio calculation.")
        return

    ratio_csv_path = get_absolute_path(__file__, "ratio_universe.csv")
    ratio_profiles = load_config_from_csv(ratio_csv_path)

    if not ratio_profiles:
        logger.warning("No ratio profiles found.")
        return

    synthetic_ratios = calculate_ratios_from_profiles(market_data_cache, ratio_profiles)

    # 4. Save Ratios to Catalog
    for ratio_name, result in synthetic_ratios.items():
        try:
            logger.info(f"Saving synthetic instrument: {ratio_name}...")

            ratio_df = result["df"]
            interval = result["interval"]

            save_synthetic_to_catalog(ratio_name, ratio_df, interval)

            # For now, just printing the head to verify flow
            logger.debug(f"Tail of {ratio_name}:\n{ratio_df.tail()}")

        except Exception as e:
            logger.error(f"Failed to save ratio {ratio_name}: {e}")

    logger.info("Ingestion and processing complete.")



if __name__ == "__main__":
    main()
