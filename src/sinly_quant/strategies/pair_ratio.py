import pandas as pd

from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import TimeInForce, OrderSide

from sinly_quant.my_indicators.swing_levels import SwingLevels

class PairRatioStrategy(Strategy):
    def __init__(self,
                 bar_a_s: BarType,
                 bar_a_l: BarType,
                 bar_b_s: BarType,
                 bar_b_l: BarType,
                 bar_ratio_s: BarType,
                 bar_ratio_l: BarType,
                 swing_size_r: int = 3,
                 swing_size_l: int = 15
                 ):
        super().__init__()
        self.bar_a_s = bar_a_s
        self.bar_b_s = bar_b_s
        self.bar_ratio_s = bar_ratio_s

        self.bar_a_l = bar_a_l
        self.bar_b_l = bar_b_l
        self.bar_ratio_l = bar_ratio_l

        # State caches for all bars (OHLC)
        self.cache_a_s = {'o': None, 'h': None, 'l': None, 'c': None}
        self.cache_b_s = {'o': None, 'h': None, 'l': None, 'c': None}
        self.cache_ratio_s = {'o': None, 'h': None, 'l': None, 'c': None}
        self.cache_a_l = {'o': None, 'h': None, 'l': None, 'c': None}
        self.cache_b_l = {'o': None, 'h': None, 'l': None, 'c': None}
        self.cache_ratio_l = {'o': None, 'h': None, 'l': None, 'c': None}

        self.df_history = pd.DataFrame()

        # Indicator for the Ratio (Synthetic)
        # We will feed this manually using update_raw()
        # self.ratio_ema = MovingAverageFactory.create(20, MovingAverageType.EXPONENTIAL)
        self.swing_levels_s = SwingLevels(swing_size_r, swing_size_l)
        self.swing_levels_l = SwingLevels(swing_size_r, swing_size_l)

    def on_start(self):
        # 1. Subscribe to all data streams
        self.subscribe_bars(self.bar_a_s)
        self.subscribe_bars(self.bar_b_s)
        self.subscribe_bars(self.bar_ratio_s)

        self.subscribe_bars(self.bar_a_l)
        self.subscribe_bars(self.bar_b_l)
        self.subscribe_bars(self.bar_ratio_l)

        # self.register_indicator_for_bars(self.bar_ratio_s, self.swing_levels_s)
        self.register_indicator_for_bars(self.bar_ratio_l, self.swing_levels_l)

        self.log.info(f"Subscribed to {self.bar_a_s}, {self.bar_b_s}, {self.bar_ratio_s}")

    def on_bar(self, bar: Bar):
        # Update local state cache for dataframe construction
        ohlc = {
            'o': bar.open.as_double(),
            'h': bar.high.as_double(),
            'l': bar.low.as_double(),
            'c': bar.close.as_double()
        }

        if bar.bar_type == self.bar_a_s:
            self.cache_a_s = ohlc
        elif bar.bar_type == self.bar_b_s:
            self.cache_b_s = ohlc
        elif bar.bar_type == self.bar_ratio_s:
            self.cache_ratio_s = ohlc
        elif bar.bar_type == self.bar_a_l:
            self.cache_a_l = ohlc
        elif bar.bar_type == self.bar_b_l:
            self.cache_b_l = ohlc
        elif bar.bar_type == self.bar_ratio_l:
            self.cache_ratio_l = ohlc


        # if not self.swing_levels_s.initialized or not self.swing_levels_l.initialized:
        #     return
        ts = pd.Timestamp(bar.ts_event, unit='ns')
        # debug: pd.Timestamp(bar.ts_event, unit='ns').strftime('%Y-%m-%d') == '2008-05-19' and str(bar.bar_type) == 'VTI-GLD.ABC-1-WEEK-LAST-EXTERNAL'
        data = {
            "weekday": ts.weekday(),

            # Short timeframe bars
            "bar_a_s_o": self.cache_a_s['o'], "bar_a_s_h": self.cache_a_s['h'], "bar_a_s_l": self.cache_a_s['l'], "bar_a_s_c": self.cache_a_s['c'],
            "bar_b_s_o": self.cache_b_s['o'], "bar_b_s_h": self.cache_b_s['h'], "bar_b_s_l": self.cache_b_s['l'], "bar_b_s_c": self.cache_b_s['c'],
            "bar_ratio_s_o": self.cache_ratio_s['o'], "bar_ratio_s_h": self.cache_ratio_s['h'], "bar_ratio_s_l": self.cache_ratio_s['l'], "bar_ratio_s_c": self.cache_ratio_s['c'],

            # Long timeframe bars
            "bar_a_l_o": self.cache_a_l['o'], "bar_a_l_h": self.cache_a_l['h'], "bar_a_l_l": self.cache_a_l['l'], "bar_a_l_c": self.cache_a_l['c'],
            "bar_b_l_o": self.cache_b_l['o'], "bar_b_l_h": self.cache_b_l['h'], "bar_b_l_l": self.cache_b_l['l'], "bar_b_l_c": self.cache_b_l['c'],
            "bar_ratio_l_o": self.cache_ratio_l['o'], "bar_ratio_l_h": self.cache_ratio_l['h'], "bar_ratio_l_l": self.cache_ratio_l['l'], "bar_ratio_l_c": self.cache_ratio_l['c'],

            # Indicators
            "swing_s_low": self.swing_levels_s.pivot_low, "swing_s_high": self.swing_levels_s.pivot_high,
            "swing_l_low": self.swing_levels_l.pivot_low, "swing_l_high": self.swing_levels_l.pivot_high,
        }

        # Check if timestamp exists in history
        if ts in self.df_history.index:
            # Update existing row with latest data/cache
            # Using .loc[ts] = Series(data) to update the row
            self.df_history.loc[ts] = pd.Series(data)
        else:
            # Create new row
            new_row = pd.DataFrame([data], index=[ts])
            new_row.index.name = 'date'
            if self.df_history.empty:
                self.df_history = new_row
            else:
                self.df_history = pd.concat([self.df_history, new_row])

        # Order logic:
        #   If swing_low is not empty:
        #       if previous one is swing_high
        #           if asset B position exists:
        #               close asset B and buy asset A
        #       else if previous one is swing_low:
        #           if daily swing_low is lower than previous swing_low:
        #               if asset A position exists:
        #                   close asset A and buy asset B

        if bar.bar_type == self.bar_ratio_l and not self.df_history.empty:
            # Get the latest row from your dataframe
            latest_row = self.df_history.iloc[-1]

            # access position: self.cache.positions(instrument_id=self.bar_a_s.instrument_id)

            ratio_close = latest_row['bar_ratio_s_c']
            swing_high = latest_row['swing_s_high']
            swing_low = latest_row['swing_s_low']

            # Ensure indicator values are valid numbers before comparing (SwingLevels might be None initially)
            if swing_high and ratio_close > swing_high:
                self.log.info(f"Signal: Ratio Breakout UP. {ratio_close} > {swing_high}")
                # Example: Long Asset A
                self._entry_logic(self.bar_a_s.instrument_id, OrderSide.BUY)

            elif swing_low and ratio_close < swing_low:
                self.log.info(f"Signal: Ratio Breakout DOWN. {ratio_close} < {swing_low}")
                # Example: Short Asset A (or Buy Asset B depending on strategy)
                self._entry_logic(self.bar_a_s.instrument_id, OrderSide.SELL)

    def _entry_logic(self, instrument_id, side):
        # Helper to place orders (simplified)
        if self.portfolio.is_flat(instrument_id):
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=side,
                quantity=10,
                time_in_force=TimeInForce.GTC,
            )
            self.submit_order(order)

    def on_stop(self):
        # Use self.log instead of print to ensure it appears in the engine's output
        self.log.info("Strategy stopping...")

        if not self.df_history.empty:
            # Save to CSV for full inspection
            file_path = "strategy_history.csv"
            self.df_history.to_csv(file_path)
            self.log.info(f"History saved to {file_path}")

            # If you must print to log, convert to string first, but it can be very long
            self.log.info(f"Final Info:\n{self.df_history.tail()}")
        else:
            self.log.info("History DataFrame is empty.")
