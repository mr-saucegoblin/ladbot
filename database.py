"""
SQLite database setup and connection helper.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "ladbot.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist. Safe to call multiple times."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS companies (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker           TEXT UNIQUE NOT NULL,  -- yfinance format e.g. AAUC.TO
                bloomberg_ticker TEXT,                  -- raw Bloomberg e.g. AAUC CT Equity
                name             TEXT NOT NULL,
                sector           TEXT,                  -- from yfinance
                industry         TEXT,                  -- from yfinance
                summary          TEXT,                  -- business summary from yfinance
                price            REAL,                  -- from Bloomberg export
                source           TEXT,                  -- SPTSX | SPTSXSM
                tags             TEXT,                  -- comma-separated thematic tags from Claude
                tagged_at        TEXT                   -- ISO timestamp of last tagging
            );

            CREATE TABLE IF NOT EXISTS weekly_runs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date         TEXT NOT NULL,
                top_theme        TEXT,
                theme_score      REAL,
                theme_rationale  TEXT,
                companies_json   TEXT                   -- JSON array of picked companies
            );
        """)


if __name__ == "__main__":
    init_db()
    print(f"Database initialised at {DB_PATH}")
