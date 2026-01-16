import pandas as pd

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import TimeInForce, OrderSide
from nautilus_trader.model.identifiers import InstrumentId


from sinly_quant.my_indicators.swing_levels import SwingLevels
from sinly_quant.strategies.base_strategy import BaseSinlyStrategy

class PairRatioStrategy(BaseSinlyStrategy):
    def __init__(self,
                 bar_a_s: BarType,
                 bar_a_l: BarType,
                 bar_b_s: BarType,
                 bar_b_l: BarType,
                 bar_ratio_s: BarType,
                 bar_ratio_l: BarType,
                 swing_size_r: int = 3,
                 swing_size_l: int = 15,
                 split_ratio=0.80,
                 thresh_hold=0.02
                 ):
        super().__init__()
        self.bar_a_s = bar_a_s
        self.bar_b_s = bar_b_s
        self.bar_ratio_s = bar_ratio_s

        self.bar_a_l = bar_a_l
        self.bar_b_l = bar_b_l
        self.bar_ratio_l = bar_ratio_l

        self.ratio_h = split_ratio
        self.thresh_hold = thresh_hold

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

        self.register_indicator_for_bars(self.bar_ratio_s, self.swing_levels_s)
        self.register_indicator_for_bars(self.bar_ratio_l, self.swing_levels_l)

        self.log.info(f"Subscribed to {self.bar_a_s}, {self.bar_b_s}, {self.bar_ratio_s}, {self.bar_ratio_l}")

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
        #   If latest_swing_l_low:
        #       if pre_swing_l_high:
        #           if asset B position exists with proper portation:
        #               close asset B and buy asset A with proper portation
        #           else:
        #               buy asset A with proper portation
        #   elif latest_swing_l_high:
        #       if pre_swing_l_low:
        #           if asset A position exists with proper portation:
        #               close asset A and buy asset B with proper portation
        #           else:
        #               buy asset B with proper portation
        #
        #
        #   if pre_swing_l_low and self.cache_ratio_s["l"] < pre_swing_l_low:
        #       if asset A position exists:
        #           close asset A and buy asset B with proper portation
        #       else:
        #           buy asset B with proper portation
        #   elif pre_swing_l_high and self.cache_ratio_s["h"] > pre_swing_l_high:
        #       if asset B position exists:
        #           close asset B and buy asset A with proper portation
        #       else:
        #           buy asset A with proper portation
        #

        # First, let's calculate the equity values of the portfolio
        inst_a = self.bar_a_s.instrument_id
        inst_b = self.bar_b_s.instrument_id

        # get the current quantities held
        qty_a = 0.0
        if not self.portfolio.is_flat(inst_a):
            qty_a = self.portfolio.position(inst_a).quantity.as_double()
        qty_b = 0.0
        if not self.portfolio.is_flat(inst_b):
            qty_b = self.portfolio.position(inst_b).quantity.as_double()

        # get the current close prices
        price_a = self.cache_a_s['c']
        price_b = self.cache_b_s['c']

        # get the current cash balance in quote currency
        cash = self.get_quote_balance(inst_a)


        val_a = qty_a * price_a
        val_b = qty_b * price_b
        total_equity = cash + val_a + val_b

        # get the previous swing long high and low
        pre_swing_l_low = None
        pre_swing_l_high = None

        swing_l_history = self.df_history[['swing_l_high', 'swing_l_low']].dropna(how='all')
        if ts in swing_l_history.index:
            swing_l_history = swing_l_history.drop(ts)
        if not swing_l_history.empty:
            pre_swing_l = swing_l_history.iloc[-1]
            pre_swing_l_high = pre_swing_l['swing_l_high'] if pd.notna(pre_swing_l['swing_l_high']) else None
            pre_swing_l_low = pre_swing_l['swing_l_low'] if pd.notna(pre_swing_l['swing_l_low']) else None

        if not pre_swing_l_high and not pre_swing_l_low:
            return

        # Get the latest row from your dataframe
        latest_row = self.df_history.iloc[-1]
        if bar.bar_type == self.bar_ratio_s and (pre_swing_l_low or pre_swing_l_high):
        # TODO, should we check if the swing_l_low and swing_l_high exist at the same time?
            bar_ratio_s_h = latest_row['bar_ratio_s_h']
            bar_ratio_s_l = latest_row['bar_ratio_s_l']

            if pre_swing_l_low and bar_ratio_s_l < pre_swing_l_low:
                self.log.info(f"Signal: Short Swing Low Breakout detected.")
            # So, we need to sell asset A and buy asset B
                self._normal_rebalance(
                    inst_id_buy=inst_b,
                    inst_id_sell=inst_a,
                    total_position=total_equity,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold,
                    cur_price_buy=price_b,
                    cur_price_sell=price_a,
                    qty_buy=qty_b,
                    qty_sell=qty_a
                )

            elif pre_swing_l_high and bar_ratio_s_h > pre_swing_l_high:
                self.log.info(f"Signal: Short Swing High Breakout detected.")
            # So, we need to sell asset B and buy asset A
                self._normal_rebalance(
                    inst_id_buy=inst_a,
                    inst_id_sell=inst_b,
                    total_position=total_equity,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold,
                    cur_price_sell=price_b,
                    cur_price_buy=price_a,
                    qty_buy=qty_a,
                    qty_sell=qty_b
                )

        if bar.bar_type == self.bar_ratio_l and not self.df_history.empty:

            # access position: self.cache.positions(instrument_id=self.bar_a_s.instrument_id)

            latest_swing_l_high = latest_row['swing_l_high']
            latest_swing_l_low = latest_row['swing_l_low']

            if pd.isna(latest_swing_l_low) and pd.isna(latest_swing_l_high):
                return

            if total_equity == 0:
                if latest_swing_l_low:
                    self._normal_rebalance(
                        inst_id_buy=inst_a,
                        inst_id_sell=inst_b,
                        total_position=total_equity,
                        ratio_h=self.ratio_h,
                        ratio_threshold=self.thresh_hold,
                        cur_price_sell=price_b,
                        cur_price_buy=price_a,
                        qty_buy=qty_a,
                        qty_sell=qty_b
                    )
                elif latest_swing_l_high:
                    self._normal_rebalance(
                        inst_id_buy=inst_b,
                        inst_id_sell=inst_a,
                        total_position=total_equity,
                        ratio_h=self.ratio_h,
                        ratio_threshold=self.thresh_hold,
                        cur_price_sell=price_a,
                        cur_price_buy=price_b,
                        qty_buy=qty_b,
                        qty_sell=qty_a
                    )
                return

            if latest_swing_l_low and not swing_l_history.empty:
                if pre_swing_l_high:
                    self._normal_rebalance(
                        inst_id_buy=inst_a,
                        inst_id_sell=inst_b,
                        total_position=total_equity,
                        ratio_h=self.ratio_h,
                        ratio_threshold=self.thresh_hold,
                        cur_price_sell=price_b,
                        cur_price_buy=price_a,
                        qty_buy=qty_a,
                        qty_sell=qty_b
                    )
                elif pre_swing_l_low:
                    self.log.info(f"Signal: Long Swing Low Breakout detected.")
            elif latest_swing_l_high and not swing_l_history.empty:
                if pre_swing_l_low:
                    self._normal_rebalance(
                        inst_id_buy=inst_b,
                        inst_id_sell=inst_a,
                        total_position=total_equity,
                        ratio_h=self.ratio_h,
                        ratio_threshold=self.thresh_hold,
                        cur_price_sell=price_a,
                        cur_price_buy=price_b,
                        qty_buy=qty_b,
                        qty_sell=qty_a
                    )
                elif pre_swing_l_high:
                    self.log.info(f"Signal: Long Swing High Breakout detected.")

    def _normal_rebalance(self,
                          inst_id_buy: InstrumentId,
                          inst_id_sell: InstrumentId,
                          total_position: float,
                          ratio_h: float,
                          ratio_threshold: float,
                          cur_price_buy: float,
                          cur_price_sell: float,
                          qty_buy: float,
                          qty_sell: float
                          ) -> None:

        ratio_l = 1.0 - ratio_h

        # 1. Calculate Targets in Value
        target_val_h = total_position * ratio_h
        target_val_l = total_position * ratio_l

        # 2. Calculate Exact Quantity Deltas
        # Target for Lower Ratio Asset (Sell side usually)
        target_qty_l = target_val_l / cur_price_sell
        qty_to_sell_l = qty_sell - target_qty_l

        # Target for Higher Ratio Asset (Buy side usually)
        target_qty_h = target_val_h / cur_price_buy
        qty_to_buy_h = target_qty_h - qty_buy

        # 3. Log Condition (Informational)
        val_low = qty_sell * cur_price_sell
        ratio_low = val_low / total_position if total_position > 0 else 0

        if ratio_low > (ratio_h - ratio_threshold):
            self.log.info(
                f"Rebalance Trigger (Flip): Asset {inst_id_sell} is {ratio_low:.2%} of portfolio. Flipping to {inst_id_buy}={ratio_h:.2f}, {inst_id_sell}={ratio_l:.2f}")
        else:
            self.log.info(f"Rebalance Trigger (Adjust): Asset {inst_id_sell} is {ratio_low:.2%}. Adjusting to targets.")

        # 4. Execute Orders (Sells First to free cash)
        if qty_to_sell_l > 0:
            # Use long bar open price from cache as limit, or cur_price_lower as fallback
            limit_price_b = self.cache_b_l.get('o') or cur_price_sell

            self.submit_order(self.order_factory.limit(
                instrument_id=inst_id_sell,
                order_side=OrderSide.SELL,
                quantity=qty_to_sell_l,
                price=limit_price_b,
                time_in_force=TimeInForce.GTC
            ))

        if qty_to_buy_h > 0:
            # Use long bar open price from cache as limit, or cur_price_higher as fallback
            limit_price_a = self.cache_a_l.get('o') or cur_price_buy

            self.submit_order(self.order_factory.limit(
                instrument_id=inst_id_buy,
                order_side=OrderSide.BUY,
                quantity=qty_to_buy_h,
                price=limit_price_a,
                time_in_force=TimeInForce.GTC
            ))

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
