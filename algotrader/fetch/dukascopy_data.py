# gold_data_downloader.py

import pandas as pd
from dukascopy_python import fetch
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta

from config import get_conn
import algotrader.utils as utils


def download_dukascopy(symbol: str, timeframe: str, offer_side: str, date_from: str, date_to: str, table_name: str = None, save_mode: str = 'parquet') -> pd.DataFrame:
    """
    Download OHLCV data from Dukascopy and adjust timezone to UTC+3.
    
    :param symbol: Trading symbol (e.g., "XAUUSD")
    :param timeframe: Timeframe string ("m1", "m5", "h1", "d1", etc.)
    :param date_from: Start date (YYYY-MM-DD)
    :param date_to: End date (YYYY-MM-DD)
    :return: Pandas DataFrame with OHLCV data (UTC+3)
    """

    # Parse dates if provided
    utc_from = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    utc_to = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

    # Convert to UTC
    utc_from = utc_from.replace(tzinfo=timezone(timedelta(hours=3))) if date_from else None
    utc_to = utc_to.replace(tzinfo=timezone(timedelta(hours=3))) if date_to else None

    # Convert to UTC
    utc_from = utc_from.astimezone(timezone.utc) if date_from else None
    utc_to = utc_to.astimezone(timezone.utc) if date_to else None

    df = fetch(
        symbol,
        timeframe,
        offer_side,
        utc_from,
        utc_to,
    )

    # Normalize columns
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    # Reset index -> expose time column
    df.reset_index(inplace=True)
    df.rename(columns={"index": "timestamp"}, inplace=True)

    # Convert to timezone-aware UTC, then shift to UTC+3
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True) + pd.Timedelta(hours=3)
    df.set_index("timestamp", inplace=True)

    symbol_stripped = utils.strip_string_list_comp(symbol)

    if table_name is None:
        table_name = f"{symbol_stripped.lower()}_{timeframe.lower()}"

    if save_mode == 'parquet':
        df.to_parquet(table_name, engine="pyarrow")
        return None
    elif save_mode == 'postgres':
        save_to_postgres(df, table_name)
        return None
    elif save_mode == 'dataframe':
        return df
    else:
        return None


def save_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Save OHLCV dataframe into PostgreSQL with UPSERT (insert or update).
    """
    conn = get_conn()
    cur = conn.cursor()

    # Create table if it doesn’t exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMP PRIMARY KEY,
        Open NUMERIC,
        High NUMERIC,
        Low NUMERIC,
        Close NUMERIC,
        Volume BIGINT
    );
    """
    cur.execute(create_table_query)

    # Convert dataframe into list of tuples
    records = df.to_records(index=False)
    values = [tuple(row) for row in records]

    # UPSERT query
    insert_query = f"""
    INSERT INTO {table_name} (timestamp, Open, High, Low, Close, Volume)
    VALUES %s
    ON CONFLICT (time) DO UPDATE SET
        Open = EXCLUDED.Open,
        High = EXCLUDED.High,
        Low = EXCLUDED.Low,
        Close = EXCLUDED.Close,
        Volume = EXCLUDED.Volume;
    """

    execute_values(cur, insert_query, values)
    conn.commit()

    cur.close()
    conn.close()
    print(f"✅ Upserted {len(values)} rows into {table_name}")
