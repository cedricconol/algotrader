import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy

# ------------------------------
# Strategy Base
# ------------------------------
class StrategyBase:
    """All strategies must implement this."""
    def generate_signal(self, df: pd.DataFrame, position: dict) -> dict:
        raise NotImplementedError("You must implement generate_signal()")


# ------------------------------
# Backtest Engine
# ------------------------------

def run_backtest(df: pd.DataFrame, generate_signal, **kwargs):
    """
    Run a backtest with backtesting.py given OHLC DataFrame and strategy class.
    """
    # Create a wrapper Strategy class for backtesting.py
    class StrategyWrapper(Strategy):
        def init(self):
            self.custom_data = {}

        def next(self):
            # Build a DataFrame from self.data (OHLCV arrays up to current step)
            data = pd.DataFrame({
                "Open": self.data.Open[:len(self.data.Open)],
                "High": self.data.High[:len(self.data.High)],
                "Low": self.data.Low[:len(self.data.Low)],
                "Close": self.data.Close[:len(self.data.Close)],
                "Volume": self.data.Volume[:len(self.data.Volume)],
            }, index=self.data.index)

            data.index.name = "timestamp"

            if self.position:
                position_dict = {
                    'is_long': self.position.is_long,
                    'is_short': self.position.is_short,
                    'pl': self.position.pl,
                }
            else:
                position_dict = None

            signal = generate_signal(data, position=position_dict, custom_data=self.custom_data)

            if signal:
                direction = signal['direction']
                size = signal['size']
                limit = signal['limit']
                stop = signal['stop']
                sl = signal['sl']
                tp = signal['tp']
                if 'custom_data' in signal:
                    self.custom_data = signal['custom_data']

                if direction == 'buy':
                    self.buy(size=size, limit=limit, stop=stop, sl=sl, tp=tp)
                elif direction == 'sell':
                    self.sell(size=size, limit=limit, stop=stop, sl=sl, tp=tp)
                elif direction == 'close':
                    self.position.close()

    bt = Backtest(df, StrategyWrapper, **kwargs)
    return bt.run(), bt

