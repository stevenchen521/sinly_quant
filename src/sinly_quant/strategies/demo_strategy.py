# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import datetime as dt
import pandas as pd

from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import PositionChanged
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.model.events import PositionClosed
from nautilus_trader.trading.strategy import Strategy
from sinly_quant.my_indicators.my_ema_python import PyExponentialMovingAverage

from typing import List, Dict


class DemoStrategy(Strategy):
    def __init__(self, bar_types: List[BarType], quantity: int = 10):
        super().__init__()

        self.bar_types = bar_types
        self.quantity = quantity

        # Dictionary to hold state and indicators per BarType
        # Key: BarType, Value: Dict with indicators and previous values
        # Renamed from self.state to self.indicators_state to avoid conflict with Component.state
        self.indicators_state: Dict[BarType, dict] = {}

        for bar_type in self.bar_types:
            self.indicators_state[bar_type] = {
                'ema10': PyExponentialMovingAverage(10),
                'ema20': PyExponentialMovingAverage(20),
                'prev_ema10': None,
                'prev_ema20': None
            }

    def on_start(self):
        self.start_time = dt.datetime.now()
        self.log.info(f"Strategy started at: {self.start_time}")

        for bar_type in self.bar_types:
            # 1. Subscribe to the specific bar type (Instrument + Timeframe)
            self.subscribe_bars(bar_type)

            # 2. Register the specific indicators for this bar type
            indicators = self.indicators_state[bar_type]
            self.register_indicator_for_bars(bar_type, indicators['ema10'])
            self.register_indicator_for_bars(bar_type, indicators['ema20'])

            self.log.info(f"Registered indicators for {bar_type}")

    def on_bar(self, bar: Bar):
        # Retrieve the state specific to this bar's type (e.g., VTI-1-DAY)
        state = self.indicators_state.get(bar.bar_type)

        if not state:
            return  # Should not happen if registered correctly

        ema10 = state['ema10']
        ema20 = state['ema20']

        # Ensure indicators are ready for this specific series
        if not ema10.initialized or not ema20.initialized:
            return

        current_ema10 = ema10.value
        current_ema20 = ema20.value

        prev_ema10 = state['prev_ema10']
        prev_ema20 = state['prev_ema20']

        # We need previous values to detect a cross
        if prev_ema10 is not None and prev_ema20 is not None:
            # Golden Cross: 10 crosses above 20
            if prev_ema10 <= prev_ema20 and current_ema10 > current_ema20:
                self._check_buy_signal(bar, state)  # Pass state to access/log specific context

            # Death Cross: 10 crosses below 20
            elif prev_ema10 >= prev_ema20 and current_ema10 < current_ema20:
                self._check_sell_signal(bar, state)

        # Update state for next bar for this specific bar type
        state['prev_ema10'] = current_ema10
        state['prev_ema20'] = current_ema20

    def _check_buy_signal(self, bar: Bar, state: dict):
        instrument_id = bar.bar_type.instrument_id
        ema10 = state['ema10']
        ema20 = state['ema20']

        # Only buy if we don't have a position
        if self.portfolio.is_flat(instrument_id):
            self.log.info(f"Golden Cross ({instrument_id} EMA10={ema10.value:.4f} > EMA20={ema20.value:.4f}). BUYING.", color=LogColor.GREEN)

            instrument = self.cache.instrument(instrument_id)
            if instrument:
                qty = instrument.make_qty(self.quantity)
                order = self.order_factory.market(
                    instrument_id=instrument_id,
                    order_side=OrderSide.BUY,
                    quantity=qty
                )
                self.submit_order(order)
            else:
                self.log.error(f"Instrument {instrument_id} not found in cache.")

    def _check_sell_signal(self, bar: Bar, state: dict):
        instrument_id = bar.bar_type.instrument_id
        ema10 = state['ema10']
        ema20 = state['ema20']

        # Only sell if we have a long position
        if self.portfolio.is_net_long(instrument_id):
            self.log.info(f"Death Cross ({instrument_id} EMA10={ema10.value:.4f} < EMA20={ema20.value:.4f}). SELLING.", color=LogColor.RED)
            self.close_all_positions(instrument_id)

    def on_order_filled(self, event: OrderFilled):
        self.log.info(f"Order Filled: {pd.Timestamp(event.ts_event)} {event.order_side} {event.last_qty} @ {event.last_px}", color=LogColor.BLUE)

    def on_position_opened(self, event: PositionOpened):
        self.log.info(f"Position Opened: {event.instrument_id} {pd.Timestamp(event.ts_event)}  Qty: {event.quantity}", color=LogColor.CYAN)

    def on_position_changed(self, event: PositionChanged):
        self.log.info(f"Position Changed: {event.instrument_id} {pd.Timestamp(event.ts_event)}  Qty: {event.quantity}", color=LogColor.CYAN)

    def on_position_closed(self, event: PositionClosed):
        self.log.info(f"Position Closed: {event.instrument_id} {pd.Timestamp(event.ts_event)} ", color=LogColor.CYAN)

    def on_stop(self):
        self.end_time = dt.datetime.now()
        self.log.info(f"Strategy finished at: {self.end_time}")

        # Close positions for all instruments involved in the strategy
        closed_instruments = set()
        for bar_type in self.bar_types:
            instrument_id = bar_type.instrument_id
            if instrument_id not in closed_instruments:
                self.close_all_positions(instrument_id)
                closed_instruments.add(instrument_id)
