import psycopg2

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