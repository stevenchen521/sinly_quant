from collections import deque
from typing import Optional

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators import Indicator
from nautilus_trader.model.data import Bar


class SwingLevels(Indicator):

    def __init__(self, swing_size_r: int, swing_size_l: int):

        super().__init__(params=[swing_size_r, swing_size_l])

        PyCondition.positive_int(swing_size_r, "Swing Size Right")
        PyCondition.positive_int(swing_size_l, "Swing Size Left")


        self.swing_size_r = swing_size_r
        self.swing_size_l = swing_size_l

        # The window size required to detect a pivot
        # [ left bars ... candidate ... right bars ]
        self._window_size = swing_size_l + swing_size_r + 1

        # Deques to store the sliding window of prices
        self._highs = deque(maxlen=self._window_size)
        self._lows = deque(maxlen=self._window_size)
        self._bars = deque(maxlen=self._window_size)

        # Outputs: These will hold the price if a pivot is confirmed on the current bar
        self.pivot_high: Optional[float] = None
        self.pivot_low: Optional[float] = None

        self.pivot_high_history: list[Bar] = []
        self.pivot_low_history: list[Bar] = []


    def handle_bar(self, bar: Bar):
        """
        Update the indicator with the given bar.
        """
        PyCondition.not_none(bar, "bar")

        # pd.Timestamp(bar.ts_event, unit="ns", tz="UTC") == pd.Timestamp('2025-11-03 00:00:00+0000', tz='UTC')
        import pandas as pd
        # 1. Update buffers with new bar data
        self._highs.append(bar.high.as_double())
        self._lows.append(bar.low.as_double())
        self._bars.append(bar)

        # 2. Reset outputs for the current step
        self.pivot_high = None
        self.pivot_low = None

        # 3. Check if we have enough data to make a decision
        if len(self._highs) < self._window_size:
            return

        # 4. Identify the candidate value
        # The candidate is the bar that occurred 'swing_size_r' bars ago.
        # In our window of size (L + 1 + R), this is at index 'swing_size_l'.
        candidate_idx = self.swing_size_l

        # 5. Check for Pivot High
        # Logic: Candidate must be the maximum in the window
        candidate_high = self._highs[candidate_idx]
        if candidate_high == max(self._highs):
            # Pine Script nuance: strictly greater than at least one other bar
            # to avoid marking every bar as a pivot in a flat line
            if candidate_high > min(self._highs):
                self.pivot_high = candidate_high
                self.pivot_high_history.append(self._bars[-self.swing_size_r-1])

        # 6. Check for Pivot Low
        # Logic: Candidate must be the minimum in the window
        candidate_low = self._lows[candidate_idx]
        if candidate_low == min(self._lows):
            if candidate_low < max(self._lows):
                self.pivot_low = candidate_low
                self.pivot_low_history.append(self._bars[-self.swing_size_r-1])

        # 7. Update Indicator state
        self._set_has_inputs(True)
        if not self.initialized:
            self._set_initialized(True)

    def update_raw(self, value: float):
        pass

    # def _reset(self):
    #     # Override this method to reset stateful values introduced in the class.
    #     # This method will be called by the base when `.reset()` is called.
    #     self.value = 0.0
    #     self.count = 0
