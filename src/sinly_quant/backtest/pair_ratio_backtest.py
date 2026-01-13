import sys
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
from sinly_quant.strategies.pair_ratio import PairRatioStrategy
from sinly_quant.data_prepare.instruments_providers import InstrumentProvider


def run_backtest():
    """
    Runs the DemoStrategy on GLD and VTI using both Daily and Weekly bars.
    """
    print("Initializing Backtest...")

    symbol_name_gld = "GLD"
    symbol_name_vti = "VTI"
    symbol_name_vti_gld = "VTI-GLD"
    venue_name_abc = "ABC"

    # 1. Load Data
    catalog = ParquetDataCatalog(CATALOG_PATH)

    print("Loading data from catalog...")
    bars_gld = catalog.query(
        data_cls=Bar,
        identifiers=[f"{symbol_name_gld}.{venue_name_abc}"],
        start="2008-01-01T00:00:00Z",
        end="2024-12-31T00:00:00Z",
    )

    bars_vti = catalog.query(
        data_cls=Bar,
        identifiers=[f"{symbol_name_vti}.{venue_name_abc}"],
        start="2008-01-01T00:00:00Z",
        end="2024-12-31T00:00:00Z",
    )

    bars_vti_gld = catalog.query(
        data_cls=Bar,
        identifiers=[f"{symbol_name_vti_gld}.{venue_name_abc}"],
        start="2008-01-01T00:00:00Z",
        end="2024-12-31T00:00:00Z",
    )

    # Filter bars by timeframe
    bars_gld_daily = [b for b in bars_gld if "1-DAY" in str(b.bar_type)]
    bars_gld_weekly = [b for b in bars_gld if "1-WEEK" in str(b.bar_type)]
    bars_vti_daily = [b for b in bars_vti if "1-DAY" in str(b.bar_type)]
    bars_vti_weekly = [b for b in bars_vti if "1-WEEK" in str(b.bar_type)]

    bars_vti_gld_daily = [b for b in bars_vti_gld if "1-DAY" in str(b.bar_type)]
    bars_vti_gld_weekly = [b for b in bars_vti_gld if "1-WEEK" in str(b.bar_type)]

    # Validate data
    if not (bars_gld_daily and bars_gld_weekly and bars_vti_daily and bars_vti_weekly):
        print("Error: Missing required data (GLD/VTI Daily or Weekly bars). Check catalog.")
        sys.exit(1)

    all_data = bars_gld_daily + bars_vti_daily + bars_gld_weekly + bars_vti_weekly + bars_vti_gld_daily + bars_vti_gld_weekly
    active_bar_types = list({b.bar_type for b in all_data})

    print(f"Loaded {len(all_data)} bars across {len(active_bar_types)} bar types.")

    # 2. Configure Engine
    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTEST-PAIR-RATIO-STRATEGY"),
        logging=LoggingConfig(log_level="INFO"),
    )
    engine = BacktestEngine(config=engine_config)

    # 3. Setup Venue & Instruments
    engine.add_venue(
        venue=Venue(venue_name_abc),
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)],
        base_currency=USD,
        default_leverage=Decimal(1),
    )

    instrument_gld: Instrument = InstrumentProvider.equity(symbol_name_gld, venue_name_abc)
    instrument_vti: Instrument = InstrumentProvider.equity(symbol_name_vti, venue_name_abc)
    instrument_vti_gld: Instrument = InstrumentProvider.equity(symbol_name_vti_gld, venue_name_abc)

    engine.add_instrument(instrument_gld)
    engine.add_instrument(instrument_vti)
    engine.add_instrument(instrument_vti_gld)
    engine.add_data(all_data)

    # 4. Register Strategy
    strategy = PairRatioStrategy(
        bar_a_s=bars_vti_daily[0].bar_type,
        bar_a_l=bars_vti_weekly[0].bar_type,
        bar_b_s=bars_gld_daily[0].bar_type,
        bar_b_l=bars_gld_weekly[0].bar_type,
        bar_ratio_s=bars_vti_gld_daily[0].bar_type,
        bar_ratio_l=bars_vti_gld_weekly[0].bar_type,
        swing_size_r=3,
        swing_size_l=15
    )
    engine.add_strategy(strategy)

    # 5. Run
    print("Running backtest...")
    engine.run()

    # 6. Report
    print("Backtest complete.")
    engine.trader.generate_account_report(Venue(venue_name_abc))
    engine.dispose()


if __name__ == "__main__":
    run_backtest()
