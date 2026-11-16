
import sqlite3

from src.database.db import DB_PATH


def validate_database():

    connection = sqlite3.connect(DB_PATH)

    try:
        print("=" * 55)
        print("FINAL DATABASE VALIDATION")
        print("=" * 55)

        # 1. Total transactions
        total = connection.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]

        print("\nTotal transactions:", total)

        # 2. Unique event IDs
        unique_ids = connection.execute(
            "SELECT COUNT(DISTINCT event_id) FROM transactions"
        ).fetchone()[0]

        print("Unique event IDs:", unique_ids)

        # 3. Total transaction amount
        total_amount = connection.execute(
            "SELECT SUM(amount) FROM transactions"
        ).fetchone()[0]

        print("Total transaction amount:", total_amount)

        # 4. Duplicate event IDs
        duplicates = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM transactions
                GROUP BY event_id
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        print("Duplicate event IDs:", duplicates)

        # 5. Transaction status distribution
        print("\nTransaction status distribution:")

        statuses = connection.execute(
            """
            SELECT status, COUNT(*)
            FROM transactions
            GROUP BY status
            """
        ).fetchall()

        for status, count in statuses:
            print(f"{status}: {count}")

        # 6. SQLite integrity check
        integrity = connection.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        print("\nSQLite integrity:", integrity)

        # 7. Final transaction records
        print("\nTransaction records:")

        records = connection.execute(
            """
            SELECT event_id, customer_id, amount, currency, status
            FROM transactions
            ORDER BY event_id
            """
        ).fetchall()

        for record in records:
            print(record)

        print("\n" + "=" * 55)

        if (
            total == unique_ids
            and duplicates == 0
            and integrity == "ok"
        ):
            print("FINAL VALIDATION: PASSED")
        else:
            print("FINAL VALIDATION: FAILED")

        print("=" * 55)

    finally:
        connection.close()


if __name__ == "__main__":
    validate_database()
