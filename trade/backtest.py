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
            data = pd.DataFrame({
                "Open": self.data.Open[:len(self.data.Open)],
                "High": self.data.High[:len(self.data.High)],
                "Low": self.data.Low[:len(self.data.Low)],
                "Close": self.data.Close[:len(self.data.Close)],
                "Volume": self.data.Volume[:len(self.data.Volume)],
            })

            # ✅ only set timestamp index if column exists
            if hasattr(self.data, "timestamp"):
                data.index = pd.to_datetime(self.data.timestamp[:len(self.data.timestamp)])
                data.index.name = "timestamp"

            signal = strategy_class().generate_signal(data)

            if signal == "buy" and not self.position:
                self.buy()
            elif signal == "sell" and self.position:
                self.position.close()


    bt = Backtest(df, StrategyWrapper, cash=cash, commission=commission)
    return bt.run(), bt

