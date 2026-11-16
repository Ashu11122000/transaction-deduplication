
import sqlite3
import tempfile
import unittest

from pathlib import Path
from unittest.mock import patch

from src.database.db import initialize_database
from src.worker.ingestion_worker import process_event


class IdempotencyTests(unittest.TestCase):

    def setUp(self):
        """
        Create an isolated temporary database
        before each test.
        """

        self.temp_dir = tempfile.TemporaryDirectory()

        self.db_path = Path(self.temp_dir.name) / "test.db"

        self.db_patch = patch(
            "src.database.db.DB_PATH",
            self.db_path
        )

        self.db_patch.start()

        initialize_database()

    def tearDown(self):
        """Clean up the temporary database."""

        self.db_patch.stop()
        self.temp_dir.cleanup()

    def create_event(self, event_id, amount):

        return {
            "event_id": event_id,
            "customer_id": "CUST_TEST",
            "amount": amount,
            "currency": "INR",
            "created_at": "2026-11-16T10:00:00+00:00"
        }

    def get_transaction_count(self):

        connection = sqlite3.connect(self.db_path)

        try:
            return connection.execute(
                "SELECT COUNT(*) FROM transactions"
            ).fetchone()[0]

        finally:
            connection.close()

    def test_new_event_processing(self):
        """Verify that a new event is processed successfully."""

        event = self.create_event("TEST001", 1000)

        response = process_event(event)

        self.assertEqual(response["status"], "SUCCESS")
        self.assertEqual(self.get_transaction_count(), 1)

    def test_duplicate_event_processing(self):
        """Verify that duplicate events return the original response."""

        event = self.create_event("TEST002", 2000)

        first_response = process_event(event)
        second_response = process_event(event)

        self.assertEqual(first_response, second_response)
        self.assertEqual(self.get_transaction_count(), 1)

    def test_multiple_duplicate_requests(self):
        """Verify that ten repeated requests create only one record."""

        event = self.create_event("TEST003", 3000)

        responses = [
            process_event(event)
            for _ in range(10)
        ]

        self.assertTrue(
            all(response == responses[0] for response in responses)
        )

        self.assertEqual(self.get_transaction_count(), 1)

    def test_different_events(self):
        """Verify that different event IDs create separate records."""

        event1 = self.create_event("TEST004", 1000)
        event2 = self.create_event("TEST005", 2000)

        process_event(event1)
        process_event(event2)

        self.assertEqual(self.get_transaction_count(), 2)

    def test_transaction_amount_integrity(self):
        """Verify that the original transaction amount is preserved."""

        event = self.create_event("TEST006", 5000)

        process_event(event)

        connection = sqlite3.connect(self.db_path)

        try:
            amount = connection.execute(
                """
                SELECT amount
                FROM transactions
                WHERE event_id = ?
                """,
                ("TEST006",)
            ).fetchone()[0]

        finally:
            connection.close()

        self.assertEqual(amount, 5000)

    def test_conflicting_payload_rejected(self):
        """
        Verify that the same event ID with a different
        transaction amount is rejected.
        """

        original_event = self.create_event("TEST007", 3000)

        process_event(original_event)

        conflicting_event = self.create_event("TEST007", 9000)

        with self.assertRaisesRegex(
            ValueError,
            "CONFLICTING EVENT PAYLOAD"
        ):
            process_event(conflicting_event)

        # Only one record should exist
        self.assertEqual(self.get_transaction_count(), 1)

        # Original transaction amount must remain unchanged
        connection = sqlite3.connect(self.db_path)

        try:
            amount = connection.execute(
                """
                SELECT amount
                FROM transactions
                WHERE event_id = ?
                """,
                ("TEST007",)
            ).fetchone()[0]

        finally:
            connection.close()

        self.assertEqual(amount, 3000)


if __name__ == "__main__":
    unittest.main(verbosity=2)
