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
