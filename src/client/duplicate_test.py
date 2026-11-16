
import sqlite3

from src.worker.ingestion_worker import process_event


def test_repeated_duplicates():

    event = {
        "event_id": "TXN004",
        "customer_id": "CUST104",
        "amount": 3000.00,
        "currency": "INR",
        "created_at": "2026-11-16T10:43:45+00:00"
    }

    print("=" * 55)
    print("REPEATED DUPLICATE EVENT TEST")
    print("=" * 55)

    responses = []

    # Send the same event 10 times
    for attempt in range(1, 11):

        print(f"\nRequest number: {attempt}")

        response = process_event(event)

        responses.append(response)

    # Verify all responses are identical
    assert all(
        response == responses[0]
        for response in responses
    ), "Responses are not identical!"

    # Verify database record count
    connection = sqlite3.connect("data/transactions.db")

    try:
        count = connection.execute(
            """
            SELECT COUNT(*)
            FROM transactions
            WHERE event_id = ?
            """,
            ("TXN004",)
        ).fetchone()[0]

    finally:
        connection.close()

    print("\n" + "=" * 55)
    print("TEST RESULTS")
    print("=" * 55)

    print("Total requests:", len(responses))
    print("Database records for TXN004:", count)
    print("All responses identical:", True)

    assert count == 1, "Duplicate records detected!"

    print("\nSUCCESS: Idempotency verified!")


if __name__ == "__main__":
    test_repeated_duplicates()
