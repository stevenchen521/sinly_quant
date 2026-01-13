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

        self.register_indicator_for_bars(self.bar_ratio_s, self.swing_levels_s)
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

        if bar.bar_type == self.bar_ratio_l and not self.df_history.empty:
            # Get the latest row from your dataframe
            latest_row = self.df_history.iloc[-1]

            # access position: self.cache.positions(instrument_id=self.bar_a_s.instrument_id)

            ratio_close = latest_row['bar_ratio_s_c']
            swing_s_high = latest_row['swing_s_high']
            swing_s_low = latest_row['swing_s_low']

            latest_swing_l_high = latest_row['swing_l_high']
            latest_swing_l_low = latest_row['swing_l_low']

            swing_l_history = self.df_history[['swing_l_high', 'swing_l_low']].dropna(how='all')

            if ts in swing_l_history.index:
                swing_l_history = swing_l_history.drop(ts)

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

            # Calculate Total Equity (Cash + Market Value of A + Market Value of B)
            # Assuming Spot account logic: Cash implies uninvested funds
            cash = self.portfolio.account.balance_total().as_double()

            val_a = qty_a * price_a
            val_b = qty_b * price_b
            total_equity = cash + val_a + val_b

            if total_equity <= 0:
                return

            pre_swing_l_row = swing_l_history.iloc[-1]
            pre_swing_l_high = pre_swing_l_row['swing_l_high'] if pd.notna(pre_swing_l_row['swing_l_high']) else None
            pre_swing_l_low = pre_swing_l_row['swing_l_low'] if pd.notna(pre_swing_l_row['swing_l_low']) else None

            if latest_swing_l_low and not swing_l_history.empty:
                if pre_swing_l_high:
                    self._normal_rebalance(
                        higher_inst_id=inst_a,
                        lower_inst_id=inst_b,
                        total_equity=total_equity,
                        ratio_h=0.80,
                        ratio_threshold=0.02,
                        cur_price_lower=price_b,
                        cur_price_higher=price_a,
                        qty_higher=qty_a,
                        qty_lower=qty_b
                    )
                elif pre_swing_l_low:
                    self.log.info(f"Signal: Long Swing Low Breakout detected.")
            elif latest_swing_l_high and not swing_l_history.empty:
                if pre_swing_l_low:
                    self._normal_rebalance(
                        higher_inst_id=inst_b,
                        lower_inst_id=inst_a,
                        total_equity=total_equity,
                        ratio_h=0.80,
                        ratio_threshold=0.02,
                        cur_price_lower=price_a,
                        cur_price_higher=price_b,
                        qty_higher=qty_b,
                        qty_lower=qty_a
                    )
                elif pre_swing_l_high:
                    self.log.info(f"Signal: Long Swing High Breakout detected.")


    def _normal_rebalance(self,
                          higher_inst_id,
                          lower_inst_id,
                          total_equity,
                          ratio_h,
                          ratio_threshold,
                          cur_price_lower,
                          cur_price_higher,
                          qty_higher,
                          qty_lower
                          ):
        ratio_l = 1-ratio_h

        # Calculate Targets
        target_val_a = total_equity * ratio_h
        target_val_b = total_equity * ratio_l
        # Calculate Quantity Deltas
        # We need to reach target_val_b, so we sell the difference
        target_qty_b = target_val_b / cur_price_lower
        qty_to_sell_b = qty_lower - target_qty_b
        # We need to reach target_val_a, so we buy the difference
        target_qty_a = target_val_a / cur_price_higher
        qty_to_buy_a = target_qty_a - qty_higher

        val_b = qty_lower * cur_price_lower


        # Check Ratio of Asset B
        ratio_b = val_b / total_equity

        # Rebalance Logic: If B is around ratio_h (using > 0.78 as threshold)
        if ratio_b > (ratio_h - ratio_threshold):
            self.log.info(
                f"Rebalance Trigger: Asset B is {ratio_b:.2%} of portfolio. Flipping to A={ratio_h}, B={ratio_l}")
            # Execute Orders
            # Sell B first to free up cash (if spot)
            if qty_to_sell_b > 0:
                limit_price_b = self.cache_b_l['o']  # Use long bar open price as limit
                self.submit_order(self.order_factory.limit(
                    instrument_id=lower_inst_id,
                    order_side=OrderSide.SELL,
                    quantity=qty_to_sell_b,
                    price=limit_price_b,
                    time_in_force=TimeInForce.GTC
                ))

            # Buy A
            if qty_to_buy_a > 0:
                limit_price_a = self.cache_a_l['o']  # Use long bar open price as limit
                self.submit_order(self.order_factory.limit(
                    instrument_id=higher_inst_id,
                    order_side=OrderSide.BUY,
                    quantity=qty_to_buy_a,
                    price=limit_price_a,
                    time_in_force=TimeInForce.GTC
                ))
        else:
            self.log.info(
                f"No Rebalance: Asset B is {ratio_b:.2%} of portfolio. No action taken.")
            # Buy A
            if qty_to_buy_a > 0:
                limit_price_a = self.cache_a_l['o']  # Use long bar open price as limit
                self.submit_order(self.order_factory.limit(
                    instrument_id=lower_inst_id,
                    order_side=OrderSide.BUY,
                    quantity=qty_to_buy_a,
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
