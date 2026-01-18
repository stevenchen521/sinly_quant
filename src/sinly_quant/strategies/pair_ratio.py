import pandas as pd

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import TimeInForce, OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity, Price
from nautilus_trader.model.events import OrderFilled, OrderRejected, OrderDenied

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
                 split_ratio=0.90,
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

        # NEW: Store pending buy orders here
        self.pending_buy_instruction = None

        self.venue = bar_a_s.instrument_id.venue

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

        ts = pd.Timestamp(bar.ts_event, unit='ns')

        l_index = 0

        # Determine the reference row (strictly previous history)
        last_row = None

        if not self.df_history.empty:
            # Check if likely appending (common case)
            if self.df_history.index[-1] < ts:
                last_row = self.df_history.iloc[-1]
            elif self.df_history.index[-1] == ts:
                # Updating the latest row -> look at the one before it
                if len(self.df_history) > 1:
                    last_row = self.df_history.iloc[-2]
            else:
                # Out of order or updating older row -> safe search
                history_before = self.df_history[self.df_history.index < ts]
                if not history_before.empty:
                    last_row = history_before.iloc[-1]

        if last_row is not None:
            last_l_index = int(last_row['l_index'])

            def is_diff(v1, v2):
                n1 = v1 is None or pd.isna(v1)
                n2 = v2 is None or pd.isna(v2)
                if n1 and n2: return False
                if n1 != n2: return True
                return float(v1) != float(v2)

            long_vals = [
                (self.cache_a_l['o'], last_row['bar_a_l_o']), (self.cache_a_l['h'], last_row['bar_a_l_h']),
                (self.cache_a_l['l'], last_row['bar_a_l_l']), (self.cache_a_l['c'], last_row['bar_a_l_c']),
                (self.cache_b_l['o'], last_row['bar_b_l_o']), (self.cache_b_l['h'], last_row['bar_b_l_h']),
                (self.cache_b_l['l'], last_row['bar_b_l_l']), (self.cache_b_l['c'], last_row['bar_b_l_c']),
                (self.cache_ratio_l['o'], last_row['bar_ratio_l_o']), (self.cache_ratio_l['h'], last_row['bar_ratio_l_h']),
                (self.cache_ratio_l['l'], last_row['bar_ratio_l_l']), (self.cache_ratio_l['c'], last_row['bar_ratio_l_c']),
            ]

            if any(is_diff(curr, last) for curr, last in long_vals):
                l_index = last_l_index + 1
            else:
                l_index = last_l_index
        data = {
            "l_index": l_index,

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
        # debug: pd.Timestamp(bar.ts_event, unit='ns').strftime('%Y-%m-%d') == '2008-05-19' and str(bar.bar_type) == 'VTI-GLD.ABC-1-WEEK-LAST-EXTERNAL'

        # get the current quantities held
        qty_a = self.get_quote_qty(inst_a)
        qty_b = self.get_quote_qty(inst_b)

        # get the current close prices
        price_a = self.cache_a_s['c']
        price_b = self.cache_b_s['c']

        if price_a is None or price_b is None:
            return

        # get the current cash balance in quote currency
        cash = self.get_available_cash(inst_a.venue)


        val_a = 0.0
        if qty_a>  0:
            val_a = qty_a * price_a
        val_b = 0.0
        if qty_b > 0:
            val_b = qty_b * price_b
        total_position = cash + val_a + val_b
        total_equity = val_a + val_b

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
            # pre_swing_l_idx = pre_swing_l['l_index'] if pd.notna(pre_swing_l['l_index']) else None

        # Get the latest row from your dataframe
        latest_row = self.df_history.iloc[-1]

        # first order
        if total_equity == 0 and bar.bar_type == self.bar_ratio_l and not self.df_history.empty:
            # access position: self.cache.positions(instrument_id=self.bar_a_s.instrument_id)
            latest_swing_l_high = latest_row['swing_l_high']
            latest_swing_l_low = latest_row['swing_l_low']

            if pd.isna(latest_swing_l_low) and pd.isna(latest_swing_l_high):
                return

            if pd.notna(latest_swing_l_low):
                self._normal_rebalance(
                    asset_h={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    asset_l={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold
                )
            elif pd.notna(latest_swing_l_high):
                self._normal_rebalance(
                    asset_h={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    asset_l={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold
                )
            return


        # There is no previous swing long high or low, return
        if not pre_swing_l_high and not pre_swing_l_low:
            return


        if bar.bar_type == self.bar_ratio_s and (pre_swing_l_low or pre_swing_l_high):
        # TODO, should we check if the swing_l_low and swing_l_high exist at the same time?
            bar_ratio_s_h = latest_row['bar_ratio_s_h']
            bar_ratio_s_l = latest_row['bar_ratio_s_l']

            if pd.notna(pre_swing_l_low) and bar_ratio_s_l < pre_swing_l_low:
                self.log.info(f"Signal: Short Swing Low Breakout detected.")
            # So, we need to sell asset A and buy asset B
                self._normal_rebalance(
                    asset_h={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    asset_l={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold
                )

            elif pd.notna(pre_swing_l_high) and bar_ratio_s_h > pre_swing_l_high:
                self.log.info(f"Signal: Short Swing High Breakout detected.")
            # So, we need to sell asset B and buy asset A
                self._normal_rebalance(
                    asset_h={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    asset_l={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold
                )

        if bar.bar_type == self.bar_ratio_l and not self.df_history.empty:
            # access position: self.cache.positions(instrument_id=self.bar_a_s.instrument_id)
            latest_swing_l_high = latest_row['swing_l_high']
            latest_swing_l_low = latest_row['swing_l_low']

            if pd.isna(latest_swing_l_low) and pd.isna(latest_swing_l_high):
                return

            if pd.notna(latest_swing_l_low) and not swing_l_history.empty:
                if pd.notna(pre_swing_l_high):
                    self._normal_rebalance(
                        asset_h={'id': inst_a, 'price': price_a, 'qty': qty_a},
                        asset_l={'id': inst_b, 'price': price_b, 'qty': qty_b},
                        total_position=total_position,
                        ratio_h=self.ratio_h,
                        ratio_threshold=self.thresh_hold
                    )
                elif pre_swing_l_low:
                    self.log.info(f"Signal: Long Swing Low Breakout detected.")
            elif pd.notna(latest_swing_l_high) and not swing_l_history.empty:
                if pd.notna(pre_swing_l_low):
                    self._normal_rebalance(
                        asset_h={'id': inst_b, 'price': price_b, 'qty': qty_b},
                        asset_l={'id': inst_a, 'price': price_a, 'qty': qty_a},
                        total_position=total_position,
                        ratio_h=self.ratio_h,
                        ratio_threshold=self.thresh_hold
                    )
                elif pre_swing_l_high:
                    self.log.info(f"Signal: Long Swing High Breakout detected.")

    def _normal_rebalance(self,
                          asset_h: dict,
                          asset_l: dict,
                          total_position: float,
                          ratio_h: float,
                          ratio_threshold: float,
                          ) -> None:
        """
        Rebalances the pair to match the target ratio.

        Args:
            asset_h (dict): Group for the asset we want to overweight (Target High Ratio).
                            Expected keys: 'id' (InstrumentId), 'price' (float), 'qty' (float).
            asset_l (dict): Group for the asset we want to underweight (Target Low Ratio).
                            Expected keys: 'id' (InstrumentId), 'price' (float), 'qty' (float).
            total_position (float): Total equity value (cash + positions).
            ratio_h (float): Target allocation ratio for asset_h (e.g. 0.8).
            ratio_threshold (float): Minimum deviation required to trigger rebalance.
        """
        # Unpack
        inst_id_buy = asset_h['id']
        cur_price_buy = asset_h['price']
        qty_buy = asset_h['qty']

        inst_id_sell = asset_l['id']
        cur_price_sell = asset_l['price']
        qty_sell = asset_l['qty']

        ratio_l = 1.0 - ratio_h

        # 1. Calculate Targets in Value
        target_val_h = total_position * ratio_h
        target_val_l = total_position - target_val_h

        # 2. Calculate Exact Quantity Deltas
        # Target for Lower Ratio Asset (Sell side usually)
        target_qty_l = target_val_l // cur_price_sell
        qty_to_sell_l = int(qty_sell - target_qty_l)

        # Target for Higher Ratio Asset (Buy side usually)
        target_qty_h = target_val_h // cur_price_buy
        qty_to_buy_h = int(target_qty_h - qty_buy)

        # 3. Log Condition (Informational)
        val_low = qty_sell * cur_price_sell
        ratio_low = val_low / total_position if total_position > 0 else 0

        if ratio_low > (ratio_h - ratio_threshold):
            self.log.info(
                f"Rebalance Trigger (Flip): Asset {inst_id_sell} is {ratio_low:.2%} of portfolio. Flipping to {inst_id_buy}={ratio_h:.2%}, {inst_id_sell}={ratio_l:.2%}")
        else:
            self.log.info(f"Rebalance Trigger (Adjust): Asset {inst_id_sell} is {ratio_low:.2%}. Adjusting to targets.")

        # 4. Execute Orders with Chain Logic
        sold_something = False
        limit_price_b = format(cur_price_sell, '.4f')
        limit_price_a = format(cur_price_buy, '.4f')

        # --- EXECUTE SELLS (Immediate) ---
        # Check if we need to Sell B (the low ratio asset)
        if qty_to_sell_l > 0:
            sold_something = True
            self.submit_order(self.order_factory.limit(
                instrument_id=inst_id_sell,
                order_side=OrderSide.SELL,
                quantity=Quantity.from_int(qty_to_sell_l),
                price=Price.from_str(str(limit_price_b)),
                time_in_force=TimeInForce.GTC,
                reduce_only=True,
            ))

        # Check if we need to Sell A (the high ratio asset)
        # This occurs if we are heavily overweight A and need to trim it
        if qty_to_buy_h < 0:
            sold_something = True
            self.submit_order(self.order_factory.limit(
                instrument_id=inst_id_buy,
                order_side=OrderSide.SELL,
                quantity=Quantity.from_int(abs(qty_to_buy_h)),
                price=Price.from_str(str(limit_price_a)),
                time_in_force=TimeInForce.GTC,
                reduce_only=True,
            ))

        # --- EXECUTE BUYS (Deferred or Immediate) ---
        def execute_or_defer_buy(inst_id, qty, price_str):
            if qty <= 0: return

            if sold_something:
                # Store instruction for on_order_filled
                self.log.info(f"Deferring BUY {qty} of {inst_id} until SELL fills.")
                self.pending_buy_instruction = {
                    'instrument_id': inst_id,
                    'quantity': qty,
                    'price': price_str
                }
            else:
                # Immediate execution
                self.submit_order(self.order_factory.limit(
                    instrument_id=inst_id,
                    order_side=OrderSide.BUY,
                    quantity=Quantity.from_int(qty),
                    price=Price.from_str(price_str),
                    time_in_force=TimeInForce.GTC
                ))

        # Calculate if we need to buy B (rare, usually we sell B here)
        if qty_to_sell_l < 0:
            execute_or_defer_buy(inst_id_sell, abs(qty_to_sell_l), limit_price_b)

        # Calculate if we need to buy A (standard rebalance target)
        if qty_to_buy_h > 0:
            execute_or_defer_buy(inst_id_buy, abs(qty_to_buy_h), limit_price_a)

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

        # Save Fills/Trade History
        if not self.fills_df.empty:
            fills_path = "fills_history.csv"
            self.fills_df.to_csv(fills_path)
            self.log.info(f"Fills History saved to {fills_path}")
            self.log.info(f"Fills Info:\n{self.fills_df}")
        else:
            self.log.info("No fills recorded.")


    def on_order_filled(self, event: OrderFilled) -> None:
        """
        Triggered immediately when an order is partially or fully filled.
        """
        # 1. Record the execution details to internal history
        self.record_fill(event)

        # 1.5. Enrich the history record with Total Equity (position_all)
        #      Using cached close prices from the strategy state
        #      NOTE: These prices are technically from the LAST RECEIVED BAR, not live ticks.
        #            For daily bars backtest, this is acceptable.
        if self.fills_history:
            inst_a = self.bar_a_s.instrument_id
            inst_b = self.bar_b_s.instrument_id

            # Current quantities
            qty_a = self.get_quote_qty(inst_a)
            qty_b = self.get_quote_qty(inst_b)

            # Cached Prices
            px_a = self.cache_a_s['c'] or 0.0
            px_b = self.cache_b_s['c'] or 0.0

            # If the current fill is for A or B, update the price to reflect the fill price approx?
            # Or just stick to the bar close? User asked to use daily close info stored.
            # We'll use the cached daily close logic as requested.

            # Cash (already fetched in record_fill, but let's grab it or reuse)
            # Accessing the last record created by record_fill
            last_record = self.fills_history[-1]
            cash = last_record['available_cash']

            # Calculate Equity
            val_a = qty_a * px_a
            val_b = qty_b * px_b
            position_all = cash + val_a + val_b

            # Update the record
            self.fills_history[-1]['position_all'] = position_all
            self.fills_history[-1]['price_a'] = px_a
            self.fills_history[-1]['price_b'] = px_b

        # 2. Strategy Specific Logic: Chain Execution (Deferred Buy)
        if self.pending_buy_instruction and event.order_side == OrderSide.SELL:
            instr = self.pending_buy_instruction
            # Verify we are not trying to buy what we just sold (unlikely but safe)
            # and that the sell was actually related to our rebalance logic

            self.log.info(f"SELL confirmed. Executing deferred BUY for {instr['instrument_id']}")

            self.submit_order(self.order_factory.limit(
                instrument_id=instr['instrument_id'],
                order_side=OrderSide.BUY,
                quantity=Quantity.from_int(instr['quantity']),
                price=Price.from_str(str(instr['price'])),
                time_in_force=TimeInForce.GTC
            ))

            self.pending_buy_instruction = None

    def on_order_rejected(self, event: OrderRejected) -> None:
        """
        Catches orders that failed immediately (e.g. Insufficient Funds)
        """
        self.log.error(f"⚠️ ORDER REJECTED: {event.instrument_id} - Reason: {event.reason}")

    def on_order_denied(self, event: OrderDenied) -> None:
        """
        Catches orders that failed immediately (e.g. Insufficient Funds)
        """
        self.log.error(f"⚠️ ORDER DENIED: {event.instrument_id} - Reason: {event.reason}")
