import pandas as pd

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import TimeInForce, OrderSide
from nautilus_trader.model.objects import Quantity, Price
from nautilus_trader.model.events import OrderFilled, OrderRejected, OrderDenied
from pathlib import Path

from sinly_quant.constants import RESULTS_PATH
from sinly_quant.util import get_timestamp_suffix
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
                 thresh_hold=0.02,
                 output_path: str = None
                 ):
        super().__init__()
        # Set output path (default to constants.RESULTS_PATH if not provided)
        self.output_path = Path(output_path) if output_path else RESULTS_PATH

        # Generate a unique run identifier/timestamp for filenames
        self.run_id = get_timestamp_suffix()

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

        # Log for daily fills aggregation
        self.daily_fills_log = {}

        # Track last acted breakout to avoid duplicate actions on the same swing level
        self.last_acted_breakout = None

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

        # First, let's calculate the equity values of the portfolio
        inst_a = self.bar_a_s.instrument_id
        inst_b = self.bar_b_s.instrument_id
        # debug: pd.Timestamp(bar.ts_event, unit='ns').strftime('%Y-%m-%d') == '2009-03-23' and str(bar.bar_type) == 'VTI-GLD.ABC-1-WEEK-LAST-EXTERNAL'
        # debug: pd.Timestamp(bar.ts_event, unit='ns').strftime('%Y-%m-%d') == '2009-07-23' and str(bar.bar_type) == 'VTI-GLD.ABC-1-DAY-LAST-EXTERNAL'

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
        pre_swing_l_ts = None

        swing_l_history = self.df_history[['swing_l_high', 'swing_l_low']].dropna(how='all')
        if ts in swing_l_history.index:
            swing_l_history = swing_l_history.drop(ts)
        if not swing_l_history.empty:
            pre_swing_l = swing_l_history.iloc[-1]
            pre_swing_l_ts = pre_swing_l.name
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
                self._calc_submit_orders(
                    asset_h={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    asset_l={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold
                )
            elif pd.notna(latest_swing_l_high):
                self._calc_submit_orders(
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
            self._bos_allocation(
                latest_row=latest_row,
                pre_swing_l_low=pre_swing_l_low,
                pre_swing_l_high=pre_swing_l_high,
                pre_swing_l_ts=pre_swing_l_ts,
                inst_a=inst_a, price_a=price_a, qty_a=qty_a,
                inst_b=inst_b, price_b=price_b, qty_b=qty_b,
                total_position=total_position
            )

        if bar.bar_type == self.bar_ratio_l and not self.df_history.empty:
            self._normal_allocation(
                latest_swing_l_low=latest_row['swing_l_low'],
                latest_swing_l_high=latest_row['swing_l_high'],
                swing_l_history=swing_l_history,
                pre_swing_l_low=pre_swing_l_low,
                pre_swing_l_high=pre_swing_l_high,
                inst_a=inst_a, price_a=price_a, qty_a=qty_a,
                inst_b=inst_b, price_b=price_b, qty_b=qty_b,
                total_position=total_position
            )

    def _bos_allocation(self, latest_row, pre_swing_l_low, pre_swing_l_high, pre_swing_l_ts,
                        inst_a, price_a, qty_a,
                        inst_b, price_b, qty_b,
                        total_position):
        # TODO, should we check if the swing_l_low and swing_l_high exist at the same time?
        bar_ratio_s_h = latest_row['bar_ratio_s_h']
        bar_ratio_s_l = latest_row['bar_ratio_s_l']

        if pd.notna(pre_swing_l_low) and bar_ratio_s_l < pre_swing_l_low:
            # Check duplication
            if self.last_acted_breakout != (pre_swing_l_ts, 'low'):
                self.log.info(f"Signal: Short Swing Low Breakout detected.")
                # So, we need to sell asset A and buy asset B
                self._calc_submit_orders(
                    asset_h={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    asset_l={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold,
                    allocation_type="BOS"
                )
                self.last_acted_breakout = (pre_swing_l_ts, 'low')

        elif pd.notna(pre_swing_l_high) and bar_ratio_s_h > pre_swing_l_high:
            # Check duplication
            if self.last_acted_breakout != (pre_swing_l_ts, 'high'):
                self.log.info(f"Signal: Short Swing High Breakout detected.")
                # So, we need to sell asset B and buy asset A
                self._calc_submit_orders(
                    asset_h={'id': inst_a, 'price': price_a, 'qty': qty_a},
                    asset_l={'id': inst_b, 'price': price_b, 'qty': qty_b},
                    total_position=total_position,
                    ratio_h=self.ratio_h,
                    ratio_threshold=self.thresh_hold,
                    allocation_type="BOS"
                )
                self.last_acted_breakout = (pre_swing_l_ts, 'high')

    def _normal_allocation(self, latest_swing_l_low, latest_swing_l_high, swing_l_history,
                           pre_swing_l_low, pre_swing_l_high,
                           inst_a, price_a, qty_a,
                           inst_b, price_b, qty_b,
                           total_position):
        if pd.isna(latest_swing_l_low) and pd.isna(latest_swing_l_high):
            return

        if pd.notna(latest_swing_l_low) and not swing_l_history.empty:
            # if pd.notna(pre_swing_l_high):
            self._calc_submit_orders(
                asset_h={'id': inst_a, 'price': price_a, 'qty': qty_a},
                asset_l={'id': inst_b, 'price': price_b, 'qty': qty_b},
                total_position=total_position,
                ratio_h=self.ratio_h,
                ratio_threshold=self.thresh_hold,
                allocation_type="Normal"
            )
            # elif pre_swing_l_low:
            #     self.log.info(f"Signal: Long Swing Low Breakout detected.")
        elif pd.notna(latest_swing_l_high) and not swing_l_history.empty:
            # if pd.notna(pre_swing_l_low):
            self._calc_submit_orders(
                asset_h={'id': inst_b, 'price': price_b, 'qty': qty_b},
                asset_l={'id': inst_a, 'price': price_a, 'qty': qty_a},
                total_position=total_position,
                ratio_h=self.ratio_h,
                ratio_threshold=self.thresh_hold,
                allocation_type="Normal"
            )
            # elif pre_swing_l_high:
            #     self.log.info(f"Signal: Long Swing High Breakout detected.")

    def _calc_submit_orders(self,
                            asset_h: dict,
                            asset_l: dict,
                            total_position: float,
                            ratio_h: float,
                            ratio_threshold: float,
                            allocation_type: str = "Normal"
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
            allocation_type (str): "BOS" or "Normal" to indicate rebalance reason.
        """
        # Set allocation type for record_fill
        self.current_allocation_type = allocation_type

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

        # Ensure the output directory exists
        if not self.output_path.exists():
            try:
                self.output_path.mkdir(parents=True, exist_ok=True)
                self.log.info(f"Created output directory: {self.output_path}")
            except Exception as e:
                self.log.error(f"Could not create output directory {self.output_path}: {e}")
                return

        if not self.df_history.empty:
            # Filename with timestamp tail
            file_name = f"strategy_history_{self.run_id}.xlsx"
            file_path = self.output_path / file_name
            self.df_history.to_excel(file_path)
            self.log.info(f"History saved to {file_path}")

            # If you must print to log, convert to string first, but it can be very long
            self.log.info(f"Final Info:\n{self.df_history.tail()}")
        else:
            self.log.info("History DataFrame is empty.")

        # Save Fills/Trade History
        if not self.fills_df.empty:
            file_name = f"fills_history_{self.run_id}.xlsx"
            fills_path = self.output_path / file_name
            # Sort by date before saving
            df_to_save = self.fills_df.sort_values('date')
            df_to_save.to_excel(fills_path, index=False)
            self.log.info(f"Fills History saved to {fills_path}")
            self.log.info(f"Fills Info:\n{df_to_save}")
        else:
            self.log.info("No fills recorded.")

    def record_fill(self, event: OrderFilled):
        """
        Overrides BaseSinlyStrategy.record_fill to aggregate fills by date (daily rebalancing logic).
        Structure: One row per day with columns for both instruments A and B.
        """
        # 1. Identify context
        ts = pd.Timestamp(event.ts_event, unit='ns')
        date_key = ts.strftime('%Y-%m-%d')
        inst_a_id = self.bar_a_s.instrument_id
        inst_b_id = self.bar_b_s.instrument_id

        # 2. Get or Initialize Daily Record
        if date_key not in self.daily_fills_log:
            # Calculate Previous Total Position
            prev_total_p = 0.0
            if self.daily_fills_log:
                # Use O(1) access to last inserted key (Python 3.7+ dicts preserve insertion order)
                last_key = next(reversed(self.daily_fills_log))
                prev_total_p = self.daily_fills_log[last_key]['total_position']

            self.daily_fills_log[date_key] = {
                'date': date_key,
                'allocation_type': self.current_allocation_type,
                # Instrument A cols
                'instrument_a': inst_a_id.value,
                'order_side_a': None,
                'fill_qty_a': 0.0,
                'fill_price_a': 0.0,
                'fill_value_a': 0.0,
                # Instrument B cols
                'instrument_b': inst_b_id.value,
                'order_side_b': None,
                'fill_qty_b': 0.0,
                'fill_price_b': 0.0,
                'fill_value_b': 0.0,
                # Portfolio State (End of Day/Event)
                'position_a': 0.0,
                'position_b': 0.0,
                'available_cash': 0.0,
                'total_position': 0.0,
                'pos_change': 0.0,  # Percentage change from previous row
                '_prev_total_p': prev_total_p  # Hidden field for calculation
            }

        record = self.daily_fills_log[date_key]

        # 3. Aggregating Fill Details
        fill_qty = event.last_qty.as_double()
        fill_px = event.last_px.as_double()
        fill_val = fill_qty * fill_px
        side = ''
        if event.order_side == OrderSide.BUY:
            side = 'B'
        elif event.order_side == OrderSide.SELL:
            side = 'S'

        if event.instrument_id == inst_a_id:
            record['order_side_a'] = side # Assumes same side for all fills in the day
            record['fill_qty_a'] = round(record['fill_qty_a'] + fill_qty, 2)
            record['fill_value_a'] = round(record['fill_value_a'] + fill_val, 2)
            # Calculate Avg Price if multiple fills
            if record['fill_qty_a'] > 0:
                record['fill_price_a'] = round(record['fill_value_a'] / record['fill_qty_a'], 2)

        elif event.instrument_id == inst_b_id:
            record['order_side_b'] = side
            record['fill_qty_b'] = round(record['fill_qty_b'] + fill_qty, 2)
            record['fill_value_b'] = round(record['fill_value_b'] + fill_val, 2)
            # Calculate Avg Price
            if record['fill_qty_b'] > 0:
                record['fill_price_b'] = round(record['fill_value_b'] / record['fill_qty_b'], 2)

        # 4. Update Portfolio State Snapshot (Reflects state after *this* fill)
        #    Since we update this on every fill, the final record for the day will be the final state.

        # Current Quantities
        qty_a = self.get_quote_qty(inst_a_id)
        qty_b = self.get_quote_qty(inst_b_id)

        # Cached Daily Prices (using daily close prices as requested)
        px_a = self.cache_a_s['c'] if self.cache_a_s['c'] is not None else 0.0
        px_b = self.cache_b_s['c'] if self.cache_b_s['c'] is not None else 0.0

        # Available Cash
        cash = self.get_available_cash(self.venue)

        # Total Equity Calculation
        val_a = qty_a * px_a
        val_b = qty_b * px_b
        total_p = cash + val_a + val_b

        record['position_a'] = round(qty_a, 2)
        record['position_b'] = round(qty_b, 2)
        record['available_cash'] = round(cash, 2)
        record['total_position'] = round(total_p, 2)

        # Calculate Position Change %
        prev_p = record.get('_prev_total_p', 0.0)
        if prev_p and prev_p != 0:
            # Change relative to previous day's total position
            change_pct = ((total_p - prev_p) / prev_p) * 100.0
            record['pos_change'] = round(change_pct, 2)
        else:
            record['pos_change'] = 0.0

    @property
    def fills_df(self) -> pd.DataFrame:
        """
        Returns the daily aggregated fills history as a pandas DataFrame.
        """
        df = pd.DataFrame(list(self.daily_fills_log.values()))
        # Remove helper columns if present
        if not df.empty and '_prev_total_p' in df.columns:
            df = df.drop(columns=['_prev_total_p'])
        return df


    def on_order_filled(self, event: OrderFilled) -> None:
        """
        Triggered immediately when an order is partially or fully filled.
        """
        # 1. Record the execution details (Aggregated Daily Log)
        self.record_fill(event)

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
