from algotrader.charting import charting
from lightweight_charts import Chart
import pandas as pd

import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Example script with input params")

    # Add arguments
    parser.add_argument("--symbol", type=str, required=True, help="Instrument symbol")
    parser.add_argument("--data_path", type=str, required=True, help="Data path")
    parser.add_argument("--start_date", type=str, required=False, help="Start date (YYYY-MM-DD)")

    # Parse the arguments
    args = parser.parse_args()
    charting.DATA_PATH = args.data_path
    symbol = args.symbol
    charting.START_DATE = args.start_date if args.start_date else '2025-01-01'

    chart = Chart(toolbox=True)
    chart.legend(True)

    chart.events.search += charting.on_search

    chart.topbar.textbox('symbol', symbol.upper())
    chart.topbar.switcher('timeframe', ('1min', '5min', '15min', '30min', '1h', '4h', '1d'),
                          default='5min', func=charting.on_timeframe_selection)

    # Date pickers
    chart.topbar.textbox('start_date', charting.START_DATE, func=charting.on_date_change)


    df = charting.get_bar_data(symbol.lower(), '5min')

    chart.set(df)
    charting.reset_date_pickers(chart, df)

    chart.show(block=True)
