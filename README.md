# algotrader
Algorithmic trading framework

# download dukascopy
```
import sys
from fetch.dukascopy_data import download_dukascopy
import dukascopy_python
from dukascopy_python.instruments import INSTRUMENT_FX_METALS_XAU_USD

start = '2025-01-03'
end = '2025-01-04'
instrument = INSTRUMENT_FX_METALS_XAU_USD
interval = dukascopy_python.INTERVAL_HOUR_1
offer_side = dukascopy_python.OFFER_SIDE_BID

download_dukascopy(
    instrument,
    interval,
    offer_side,
    start,
    end,
)
```

# run backtest

```
import pandas as pd
import numpy as np
from backtesting.test import GOOG
from trade.backtest import StrategyBase, run_backtest

# Example SMA strategy
class SMA_Cross(StrategyBase):
    def __init__(self, n_fast=10, n_slow=20):
        self.n_fast = n_fast
        self.n_slow = n_slow

    def generate_signal(self, df: pd.DataFrame):
        df['sma_fast'] = df['Close'].rolling(self.n_fast).mean()
        df['sma_slow'] = df['Close'].rolling(self.n_slow).mean()

        if df['sma_fast'].iloc[-1] > df['sma_slow'].iloc[-1]:
            return "buy"
        elif df['sma_fast'].iloc[-1] < df['sma_slow'].iloc[-1]:
            return "sell"
        return None

# Backtest
stats, bt = run_backtest(GOOG, SMA_Cross, cash=100000000000)
print(stats)
bt.plot()
```
