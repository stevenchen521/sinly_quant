from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.events import OrderFilled
import pandas as pd


class BaseSinlyStrategy(Strategy):
    """
    A base class for Sinly Quant strategies containing shared utilities.
    """

    def __init__(self, config=None):
        super().__init__(config)
        self.fills_history = []

    def get_available_cash(self, venue: Venue) -> float:

        account = self.cache.account_for_venue(venue)
        if account is None:
            self.log.error(f"Account for venue {venue} not found.")
            return 0.0
        return account.balance_free().as_double()
        # return account.balance_total(currency).as_double()

    def get_quote_qty(self, instrument_id: InstrumentId) -> float:
        """
        Retrieves the quantity currently held for the specified instrument.
        Returns 0.0 if no position exists.
        """
        if self.portfolio.is_flat(instrument_id):
            return 0.0

        positions = self.cache.positions(instrument_id=instrument_id)
        if positions:
            return positions[0].quantity.as_double()
        return 0.0

    def debug_positions(self, instrument_id: InstrumentId) -> None:
        """
        Logs position events with readable timestamps for debugging.
        """
        positions = self.cache.positions(instrument_id=instrument_id)
        if not positions:
            self.log.info(f"No positions found for {instrument_id}")
            return

        self.log.info(f"--- Debug Positions for {instrument_id} ---")
        for i, pos in enumerate(positions):
            self.log.info(f"Position #{i}: {pos}")
            if hasattr(pos, 'events'):
                for event in pos.events:
                    ts_readable = pd.Timestamp(event.ts_event, unit='ns')
                    self.log.info(f"  Event Time: {ts_readable} | Type: {type(event).__name__} | Details: {event}")
        self.log.info("-------------------------------------------")

    def record_fill(self, event: OrderFilled):
        """
        Records the details of a fill event into the strategy's fills history list.
        """
        fill_px = event.last_px.as_double()
        fill_qty = event.last_qty.as_double()
        fill_value = fill_qty * fill_px

        # 2. Get Updated Position for this Instrument (Post-fill state mainly)
        positions = self.cache.positions(instrument_id=event.instrument_id)
        current_pos_qty = positions[0].quantity.as_double() if positions else 0.0

        # 3. Get Available Cash (Free Balance)
        current_cash = self.get_available_cash(event.instrument_id.venue)

        # 4. Append to history (minimal version, subclass can enrich)
        record = {
            'ts_event': pd.Timestamp(event.ts_event, unit='ns'),
            'instrument_id': event.instrument_id.value,
            'order_side': str(event.order_side),
            'fill_qty': fill_qty,
            'fill_px': fill_px,
            'fill_value': fill_value,
            'position_qty': current_pos_qty,
            'available_cash': current_cash,
            'client_order_id': event.client_order_id.value
        }
        self.fills_history.append(record)

    @property
    def fills_df(self) -> pd.DataFrame:
        """
        Returns the fills history as a pandas DataFrame.
        """
        return pd.DataFrame(self.fills_history)
