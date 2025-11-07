from __future__ import annotations

import logging
import sqlite3

from harborpi.utils.config import settings

log = logging.getLogger(__name__)

# --- Schema Definition ---

SCHEMA_SAMPLES = """
CREATE TABLE IF NOT EXISTS samples (
    ts_utc      INTEGER PRIMARY KEY NOT NULL, -- UNIX Epoch timestamp
    lat         REAL,
    lon         REAL,
    speed_kn    REAL,
    course_deg  REAL,
    heading_mag REAL,
    pressure_hpa REAL,
    temp_c      REAL
);
"""

INDEX_SAMPLES_TS = "CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples (ts_utc);"

SCHEMA_ENTRIES = """
CREATE TABLE IF NOT EXISTS entries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc        INTEGER NOT NULL UNIQUE, -- UNIX Epoch timestamp
    lat           REAL NOT NULL,
    lon           REAL NOT NULL,
    status        TEXT NOT NULL CHECK(status IN ('arrived', 'anchored', 'underway', 'docked', 'manual')),
    place_name    TEXT,
    wx_json       TEXT,
    notes         TEXT
);
"""

INDEX_ENTRIES_TS = "CREATE INDEX IF NOT EXISTS idx_entries_ts ON entries (ts_utc DESC);"


# --- Connection and Initialization ---


def get_db_connection() -> sqlite3.Connection:
    """
    Establishes a connection to the SQLite database.

    Configures the connection for optimal performance and WAL mode.

    Returns:
        sqlite3.Connection: An active database connection object.
    """
    try:
        # Use check_same_thread=False if used by different threads
        # (e.g., Flask + APScheduler). Proper locking is required.
        conn = sqlite3.connect(
            settings.DATABASE_PATH,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")  # Write-Ahead-Logging
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except sqlite3.Error as e:
        log.critical(f"Failed to connect to database at {settings.DATABASE_PATH}: {e}")
        raise


def create_schema(db_conn: sqlite3.Connection) -> None:
    """
    Executes the DDL statements to create the application schema.

    This function is idempotent and can be run safely on every
    application start.

    Args:
        db_conn: An active database connection.
    """
    try:
        cursor = db_conn.cursor()
        log.info("Initializing database schema...")
        cursor.execute(SCHEMA_SAMPLES)
        cursor.execute(INDEX_SAMPLES_TS)
        cursor.execute(SCHEMA_ENTRIES)
        cursor.execute(INDEX_ENTRIES_TS)
        db_conn.commit()
        log.info("Database schema initialized successfully.")
    except sqlite3.Error as e:
        log.error(f"Failed to create schema: {e}")
        db_conn.rollback()
