import pandas as pd
import MetaTrader5 as mt5

# ------------------------------
# Strategy Base
# ------------------------------
class StrategyBase:
    """All strategies must implement this."""
    def generate_signal(self, df: pd.DataFrame):
        raise NotImplementedError("You must implement generate_signal()")


# ------------------------------
# Live Trading (MT5) Engine
# ------------------------------
def run_mt5(strategy_class, symbol="XAUUSD", lot=0.1):
    """
    Run a live trading signal via MetaTrader5 using given strategy class.
    """
    if not mt5.initialize():
        raise RuntimeError("MT5 initialization failed")

    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 200)
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.rename(columns={"close": "Close"}, inplace=True)

    signal = strategy_class().generate_signal(df)

    if signal == "buy":
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": mt5.ORDER_TYPE_BUY,
            "deviation": 20,
            "magic": 123456,
            "comment": "Strategy Buy",
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }
        mt5.order_send(request)

    elif signal == "sell":
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": mt5.ORDER_TYPE_SELL,
            "deviation": 20,
            "magic": 123456,
            "comment": "Strategy Sell",
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }
        mt5.order_send(request)

    mt5.shutdown()
