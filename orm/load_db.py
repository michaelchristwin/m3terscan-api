"""
Module for loading SQL schema, initializing schema and returning db connection pool.
"""

import pathlib
import sqlite3
import threading
import logging


logging.basicConfig(level=logging.INFO)
current_file_dir = pathlib.Path(__file__).parent

DB_PATH = "m3ters.db"
DDL_PATH = current_file_dir / "schema.sql"


class DBState:
    """State of db."""

    lock = threading.Lock()
    initialized = False


def load_ddl():
    """Load SQL schema file."""
    with open(DDL_PATH, "r", encoding="utf-8") as f:
        return f.read()


def get_db():
    """
    Opens the DB, applies PRAGMAs, and initializes schema once.
    """

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")

    with DBState.lock:
        if not DBState.initialized:
            ddl = load_ddl()
            conn.executescript(ddl)
            DBState.initialized = True
            logging.info("Schema initialized successfully")

    return conn
