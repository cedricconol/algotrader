# fetch/mt5.py
import psycopg2
import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta, timezone

# String → MT5 constant
TIMEFRAMES = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1,
    "W1": mt5.TIMEFRAME_W1,
    "MN1": mt5.TIMEFRAME_MN1,
}

# Reverse lookup for naming tables
TIMEFRAME_NAMES = {v: k for k, v in TIMEFRAMES.items()}

# Postgres connection settings (adjust as needed)
DB_CONFIG = {
    "dbname": "trading",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
}


def get_conn():
    """Connect to Postgres."""
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_table(conn, symbol: str, timeframe: str):
    """Ensure schema and table exist for this symbol/timeframe."""
    if symbol.lower() == "gold#":
        schema_name = "gold"
    else:
        schema_name = symbol.lower()
    table_name = f"ohlc_{timeframe}"

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                time TIMESTAMPTZ PRIMARY KEY,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                tick_volume BIGINT,
                spread INTEGER,
                real_volume BIGINT
            )
        """)
    conn.commit()
    return schema_name, table_name


def get_latest_timestamp(conn, schema_name: str, table_name: str):
    """Get the latest timestamp stored for this symbol/timeframe."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(time) FROM {schema_name}.{table_name}")
        result = cur.fetchone()
    return result[0] if result and result[0] else None


def download_mt5(symbol: str, timeframe: str, n=1000, date_from: str = None, date_to: str = None):
    """
    Fetch data from MT5 and UPSERT into Postgres.

    Args:
        symbol (str): Trading symbol (e.g., "XAUUSD")
        timeframe (str): Timeframe as string ("M5", "H1", "D1", etc.)
        n (int): Number of bars to fetch if no date range provided
        date_from (str, optional): Start date in "YYYY-MM-DD"
        date_to (str, optional): End date in "YYYY-MM-DD"
    """
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Must be one of {list(TIMEFRAMES.keys())}")

    tf_const = TIMEFRAMES[timeframe]

    if not mt5.initialize():
        raise RuntimeError("MT5 initialization failed")

    conn = get_conn()
    schema_name, table_name = ensure_schema_and_table(conn, symbol, timeframe)

    # Parse dates if provided
    utc_from = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    utc_to = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

    # Convert to UTC
    utc_from = utc_from.replace(tzinfo=timezone(timedelta(hours=3))) if date_from else None
    utc_to = utc_to.replace(tzinfo=timezone(timedelta(hours=3))) if date_to else None

    # Convert to UTC
    utc_from = utc_from.astimezone(timezone.utc) if date_from else None
    utc_to = utc_to.astimezone(timezone.utc) if date_to else None

    # Case 1: Explicit date range
    if utc_from and utc_to:
        rates = mt5.copy_rates_range(symbol, tf_const, utc_from, utc_to)

    # Case 2: Incremental fetch
    else:
        latest_ts = get_latest_timestamp(conn, schema_name, table_name)
        if latest_ts:
            start = latest_ts + timedelta(seconds=1)
            rates = mt5.copy_rates_from(symbol, tf_const, start, n)
        else:
            rates = mt5.copy_rates_from_pos(symbol, tf_const, 0, n)

    mt5.shutdown()

    if rates is None or len(rates) == 0:
        conn.close()
        return pd.DataFrame()

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df["time"] = df["time"].dt.tz_convert("Etc/GMT-3")

    # UPSERT into Postgres
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(f"""
                INSERT INTO {schema_name}.{table_name}
                    (time, open, high, low, close, tick_volume, spread, real_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    tick_volume = EXCLUDED.tick_volume,
                    spread = EXCLUDED.spread,
                    real_volume = EXCLUDED.real_volume
            """, (
                row["time"].to_pydatetime(),
                row["open"], row["high"], row["low"], row["close"],
                int(row["tick_volume"]), int(row["spread"]), int(row["real_volume"])
            ))
    conn.commit()
    conn.close()

    return None
