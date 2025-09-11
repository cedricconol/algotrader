# gold_data_downloader.py

import pandas as pd
from dukascopy_python import fetch
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta

from config import get_conn


def download_dukascopy(symbol: str, timeframe: str, offer_side: str, date_from: str, date_to: str) -> pd.DataFrame:
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
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume"
    })

    # Reset index -> expose time column
    df.reset_index(inplace=True)
    df.rename(columns={"index": "timestamp"}, inplace=True)

    # Convert to timezone-aware UTC, then shift to UTC+3
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True) + pd.Timedelta(hours=3)

    symbol_stripped = strip_string_list_comp(symbol)

    table_name = f"{symbol_stripped.lower()}_{timeframe.lower()}"

    df.to_parquet(table_name, engine="pyarrow", index=False)

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
        time TIMESTAMP PRIMARY KEY,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT
    );
    """
    cur.execute(create_table_query)

    # Convert dataframe into list of tuples
    records = df.to_records(index=False)
    values = [tuple(row) for row in records]

    # UPSERT query
    insert_query = f"""
    INSERT INTO {table_name} (time, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (time) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;
    """

    execute_values(cur, insert_query, values)
    conn.commit()

    cur.close()
    conn.close()
    print(f"✅ Upserted {len(values)} rows into {table_name}")

def strip_string_list_comp(s):
  """Strips a string to keep only alphanumeric characters using a list comprehension."""
  return "".join(char for char in s if char.isalnum())
