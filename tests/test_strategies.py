from decimal import Decimal

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model import Bar, TraderId
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.objects import Money


from sinly_quant.constants import CATALOG_PATH
from sinly_quant.strategies.demo_strategy import DemoStrategy
from sinly_quant.data_prepare.instruments_providers import InstrumentProvider



def test_demo_strategy():
    """Integration-style test for loading GLD TradingView data into the catalog.

    This asserts that calling `load_to_catalog` does not raise and that the
    expected Parquet catalog directory for GLD daily bars is created.
    """
    symbol_name_gld = "GLD"
    venue_name_nyse = "NYSE"

    # Optionally verify what was written
    catalog = ParquetDataCatalog(CATALOG_PATH)

    # Define instruments and timeframes
    # We want: VTI-1-DAY, VTI-1-WEEK, GLD-1-DAY, GLD-1-WEEK

    symbol_name_vti = "VTI"

    # 1. Get VTI and GLD bars (Daily and Weekly)
    bars_gld = catalog.query(
        data_cls=Bar,
        identifiers=[f"{symbol_name_gld}.{venue_name_nyse}"],
        start="2008-01-01T00:00:00Z",
        end="2024-12-31T00:00:00Z",
    )
    bars_vti = catalog.query(
         data_cls=Bar,
        identifiers=[f"{symbol_name_vti}.{venue_name_nyse}"],
        start="2008-01-01T00:00:00Z",
        end="2024-12-31T00:00:00Z",
    )

    # Filter bars by timeframe
    bars_gld_daily = [b for b in bars_gld if "1-DAY" in str(b.bar_type)]
    bars_gld_weekly = [b for b in bars_gld if "1-WEEK" in str(b.bar_type)]

    bars_vti_daily = [b for b in bars_vti if "1-DAY" in str(b.bar_type)]
    bars_vti_weekly = [b for b in bars_vti if "1-WEEK" in str(b.bar_type)]

    # Assert correct data availability
    assert len(bars_gld_daily) > 0, "No GLD daily bars found in catalog."
    assert len(bars_vti_daily) > 0, "No VTI daily bars found in catalog."

    # Per user instruction, weekly bars should exist. Ensure they are loaded.
    assert len(bars_gld_weekly) > 0, "No GLD weekly bars found in catalog."
    assert len(bars_vti_weekly) > 0, "No VTI weekly bars found in catalog."

    all_data = bars_gld_daily + bars_vti_daily + bars_gld_weekly + bars_vti_weekly

    # Collect unique BarTypes for the strategy
    # We expect 4 types: VTI-1-DAY, VTI-1-WEEK, GLD-1-DAY, GLD-1-WEEK
    active_bar_types = list({b.bar_type for b in all_data})

    # ----------------------------------------------------------------------------------
    # 1. Configure and create backtest engine
    # ----------------------------------------------------------------------------------

    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTEST-INDICATOR-001"),  # Unique identifier for this backtest
        logging=LoggingConfig(
            log_level="INFO",  # Set to INFO to see indicator values
        ),
    )
    engine = BacktestEngine(config=engine_config)

    # ----------------------------------------------------------------------------------
    # 2. Prepare market data
    # ----------------------------------------------------------------------------------

    venue_name_nyse: str = venue_name_nyse

    # Instruments
    instrument_gld: Instrument = InstrumentProvider.equity(symbol_name_gld, venue_name_nyse)
    instrument_vti: Instrument = InstrumentProvider.equity(symbol_name_vti, venue_name_nyse)

    # ----------------------------------------------------------------------------------
    # 3. Configure trading environment
    # ----------------------------------------------------------------------------------

    # Set up the trading venue_name with a margin account
    engine.add_venue(
        venue=Venue(venue_name_nyse),
        oms_type=OmsType.NETTING,  # Use a netting order management system
        account_type=AccountType.MARGIN,  # Use a margin trading account
        starting_balances=[Money(1_000_000, USD)],  # Set initial capital
        base_currency=USD,  # Account currency
        default_leverage=Decimal(1),  # No leverage (1:1)
    )

    # Register the trading instruments
    engine.add_instrument(instrument_gld)
    engine.add_instrument(instrument_vti)

    # Load historical market data
    engine.add_data(all_data)

    # ----------------------------------------------------------------------------------
    # 4. Configure and run strategy
    # ----------------------------------------------------------------------------------

    # Create and register the strategy
    # Passing the bar types we actually have data for
    strategy = DemoStrategy(bar_types=active_bar_types)
    engine.add_strategy(strategy)

    # Execute the backtest
    engine.run()

    # Clean up resources
    engine.dispose()
