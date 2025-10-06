import unittest

import pandas as pd
from pandas.testing import assert_series_equal
from algotrader.fetch.dukascopy_data import download_dukascopy
from dukascopy_python.instruments import INSTRUMENT_FX_METALS_XAU_USD
import dukascopy_python

start = '2024-01-01'
end = '2024-01-07'
instrument = INSTRUMENT_FX_METALS_XAU_USD
interval = dukascopy_python.INTERVAL_MIN_5
offer_side = dukascopy_python.OFFER_SIDE_BID

df = download_dukascopy(
    instrument,
    interval,
    offer_side,
    start,
    end,
    save_mode='dataframe'
)

class TestDukascopyDownload(unittest.TestCase):

    def test_df_shape(self):
        self.assertEqual(df.shape, (1104, 5))

    def test_column_names(self):
        self.assertEqual(df.columns.to_list(), ['Open', 'High', 'Low', 'Close', 'Volume'])
    
    def test_index_name(self):
        self.assertEqual(df.index.name, 'timestamp')
    
    def test_first_value(self):
        data = {
            "Open": 2062.59800,
            "High": 2066.59500,
            "Low": 2062.40500,
            "Close": 2065.21400,
            "Volume": 0.12012
        }

        first_series = pd.Series(data, name=pd.Timestamp("2024-01-02 02:00:00+00:00"))

        assert_series_equal(df.iloc[0], first_series)

if __name__ == '__main__':
    unittest.main()
