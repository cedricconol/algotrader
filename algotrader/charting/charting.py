import pandas as pd

from datetime import datetime, timedelta
from algotrader.utils import prepare_df, resample_df

DATA_PATH = None
START_DATE = None

def get_bar_data(symbol, timeframe, path=None):
    if path is None:
        path = DATA_PATH
    
    df = pd.read_parquet(f'{path}/{symbol}_5min')

    df_clean = prepare_df(df, purpose='chart')

    if START_DATE:
        df_clean = df_clean[df_clean.time >= pd.to_datetime(START_DATE)]
    
    return resample_df(df_clean, timeframe)


def on_search(chart, searched_string):
    new_data = get_bar_data(searched_string, chart.topbar['timeframe'].value)
    if new_data.empty:
        return
    chart.topbar['symbol'].set(searched_string)
    chart.set(new_data)


def on_timeframe_selection(chart):
    new_data = get_bar_data(chart.topbar['symbol'].value, chart.topbar['timeframe'].value)
    if new_data.empty:
        return
    chart.set(new_data, True)
    # Reset date pickers to full range of the new data
    reset_date_pickers(chart, new_data)


def on_horizontal_line_move(chart, line):
    print(f'Horizontal line moved to: {line.price}')


def on_date_change(chart):
    """Callback when either date picker changes."""
    start = chart.topbar['start_date'].value
    end = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    if start and end:
        try:
            s = pd.to_datetime(start)
            e = pd.to_datetime(end)
            chart.set_visible_range(s, e)
        except Exception as exc:
            print("Invalid date range:", exc)


def reset_date_pickers(chart, df):
    """Reset topbar date pickers to cover the entire range of df."""
    if df.empty:
        return
    min_date = df['time'].min().date()
    max_date = df['time'].max().date()
    chart.topbar['start_date'].set(str(min_date))

