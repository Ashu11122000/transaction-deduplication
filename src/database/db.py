
import sqlite3
from pathlib import Path

# Locate the project root directory
BASE_DIR = Path(__file__).resolve().parents[2]

# SQLite database location
DB_PATH = BASE_DIR / "data" / "transactions.db"


def get_connection():
    """Create and return a SQLite database connection."""

    connection = sqlite3.connect(DB_PATH)

    # Return rows as dictionary-like objects
    connection.row_factory = sqlite3.Row

    return connection


def initialize_database():
    """Create the transactions table if it does not exist."""

    connection = get_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                response_json TEXT
            )
        """)

        connection.commit()

        print("Database initialized successfully!")
        print(f"Database location: {DB_PATH}")

    finally:
        connection.close()


if __name__ == "__main__":
    initialize_database()
