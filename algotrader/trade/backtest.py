import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy

# ------------------------------
# Strategy Base
# ------------------------------
class StrategyBase:
    """All strategies must implement this."""
    def generate_signal(self, df: pd.DataFrame):
        raise NotImplementedError("You must implement generate_signal()")


# ------------------------------
# Backtest Engine
# ------------------------------

def run_backtest(df: pd.DataFrame, strategy_class, cash=10000, commission=0.002):
    """
    Run a backtest with backtesting.py given OHLC DataFrame and strategy class.
    """
    # Create a wrapper Strategy class for backtesting.py
    class StrategyWrapper(Strategy):
        def init(self):
            pass

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

            signal = strategy_class().generate_signal(data, self.position)

            direction = signal['direction']
            size = signal['size']
            limit = signal['limit']
            stop = signal['stop']
            sl = signal['sl']
            tp = signal['tp']

            if direction == 'buy':
                self.buy(size=size, limit=limit, stop=stop, sl=sl, tp=tp)
            elif direction == 'sell':
               self.sell(size=size, limit=limit, stop=stop, sl=sl, tp=tp)
            elif direction == 'close':
                self.position.close()

    bt = Backtest(df, StrategyWrapper, cash=cash, commission=commission)
    return bt.run(), bt

