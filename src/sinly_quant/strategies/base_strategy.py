from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId


class BaseSinlyStrategy(Strategy):
    """
    A base class for Sinly Quant strategies containing shared utilities.
    """

    def get_quote_balance(self, instrument_id: InstrumentId) -> float:
        """
        Retrieves the total balance (Free + Locked) of the quote currency
        associated with the given instrument's venue.
        """
        # 1. Resolve instrument to find the correct currency
        instrument = self.cache.instrument(instrument_id)
        if instrument is None:
            self.log.error(f"Instrument {instrument_id} not found in cache during balance check.")
            return 0.0

        currency = instrument.quote_currency

        # 2. Retrieve the Account object from cache safely (by venue)
        account = self.cache.account_for_venue(instrument_id.venue)
        if account is None:
            self.log.error(f"Account for venue {instrument_id.venue} not found.")
            return 0.0

        # 3. Return total balance
        return account.balance_total(currency).as_double()

