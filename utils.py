import pandas as pd

def strip_string_list_comp(s):
  """Strips a string to keep only alphanumeric characters using a list comprehension."""
  return "".join(char for char in s if char.isalnum())

def prepare_df(df, purpose='backtest', timestamp_col='timestamp'):
    """
    Prepares a DataFrame for backtesting.py with proper OHLCV columns and optional timestamp index.
    """

    df.columns = df.columns.str.lower()

    if purpose == 'backtest':
        data = pd.DataFrame({
            "Open": df['open'].values,
            "High": df['high'].values,
            "Low": df['low'].values,
            "Close": df['close'].values,
            "Volume": df['volume'].values,
        })

        index_name = "timestamp"

        if timestamp_col and timestamp_col in df:
            data.index = pd.to_datetime(df[timestamp_col].values)
            data.index.name = index_name
        elif df.index.name == timestamp_col:
            data.index = df.index
            data.index.name = timestamp_col

    elif purpose == 'chart':
        if timestamp_col and timestamp_col in df:
            time_col = timestamp_col
        elif df.index.name == timestamp_col:
            df = df.reset_index()
            time_col = timestamp_col

        data = pd.DataFrame({
            "time": df[time_col].values,
            "open": df['open'].values,
            "high": df['high'].values,
            "low": df['low'].values,
            "close": df['close'].values,
            "volume": df['volume'].values,
        })

    # else: leave as plain integer index

    return data

def resample_df(df, timeframe='5min'):
    """
    Resamples a DataFrame to a different timeframe using OHLCV aggregation.
    """
    df = df.copy()

    open_col = [c for c in df.columns if c.lower() == 'open'][0]
    high_col = [c for c in df.columns if c.lower() == 'high'][0]
    low_col = [c for c in df.columns if c.lower() == 'low'][0]
    close_col = [c for c in df.columns if c.lower() == 'close'][0]
    volume_col = [c for c in df.columns if c.lower() == 'volume'][0]

    ohlcv_dict = {
        open_col: 'first',
        high_col: 'max',
        low_col: 'min',
        close_col: 'last',
        volume_col: 'sum'
    }

    if df.index.name == 'time' or df.index.name == 'timestamp':
        resampled_df = df.resample(timeframe).agg(ohlcv_dict).dropna()
    else:
        time_col = [c for c in df.columns if any(x in c.lower() for x in ["time", "timestamp"])][0]
        resampled_df = df.resample(timeframe, on=time_col).agg(ohlcv_dict).dropna().reset_index()

    return resampled_df
