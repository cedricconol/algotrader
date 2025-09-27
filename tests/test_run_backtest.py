import unittest

import pandas as pd
from backtesting.test import GOOG
from algotrader.trade.backtest import StrategyBase, run_backtest

class SMA_Cross(StrategyBase):
    def __init__(self, n_fast=10, n_slow=20):
        self.n_fast = n_fast
        self.n_slow = n_slow

    def generate_signal(self, df: pd.DataFrame, position):
        df['sma_fast'] = df['Close'].rolling(self.n_fast).mean()
        df['sma_slow'] = df['Close'].rolling(self.n_slow).mean()

        direction = None

        if df['sma_fast'].iloc[-1] > df['sma_slow'].iloc[-1] and not position:
            direction="buy"
        elif df['sma_fast'].iloc[-1] < df['sma_slow'].iloc[-1]:
            direction="close"

        signal = {
            'direction': direction,
            'size': 1,
            'limit': None,
            'stop': None,
            'sl': None,
            'tp': None,
        }
        
        return signal

class TestBacktest(unittest.TestCase):

    def test_trade_count(self):
        stats, _ = run_backtest(GOOG, SMA_Cross)
        self.assertEqual(stats['# Trades'], 47)

    def test_winrate(self):
        stats, _ = run_backtest(GOOG, SMA_Cross)
        self.assertEqual(round(stats['Win Rate [%]'],2), 61.7)

if __name__ == '__main__':
    unittest.main()
