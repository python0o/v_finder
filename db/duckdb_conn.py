# =====================================================================
# db/duckdb_conn.py — Shared DuckDB Connection Manager
# =====================================================================

import os
import duckdb

# Base DB path
DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
    "db",
    "v_finder.duckdb"
)

# Global connection singleton
_connection = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Returns a single long-lived DuckDB connection for the entire app.
    Safe for:
        - Streamlit
        - PPP ingestion scripts
        - ACS loaders
        - Map rendering
    """

    global _connection

    if _connection is None:
        _connection = duckdb.connect(DB_PATH, read_only=False)

        # Recommended runtime settings
        _connection.execute("PRAGMA threads=4;")
        _connection.execute("PRAGMA enable_progress_bar;")

    return _connection


def reset_connection():
    """
    Gracefully resets the DB connection (rarely needed).
    """
    global _connection

    if _connection is not None:
        try:
            _connection.close()
        except:
            pass

    _connection = None
