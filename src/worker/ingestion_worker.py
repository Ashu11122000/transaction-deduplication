
import json
from datetime import datetime, timezone

from src.database.db import get_connection
from src.producer.event_producer import generate_transaction_events


def process_event(event):
    """
    Process an event idempotently.

    Same event ID and same transaction details:
        Return the original stored response.

    Same event ID but different transaction details:
        Reject the conflicting event.
    """

    connection = get_connection()

    try:
        processed_at = datetime.now(timezone.utc).isoformat()

        response = {
            "event_id": event["event_id"],
            "status": "SUCCESS",
            "message": "Transaction processed successfully",
            "amount": event["amount"],
            "currency": event["currency"],
            "processed_at": processed_at
        }

        response_json = json.dumps(response)

        with connection:

            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO transactions (
                    event_id,
                    customer_id,
                    amount,
                    currency,
                    status,
                    created_at,
                    processed_at,
                    response_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["customer_id"],
                    event["amount"],
                    event["currency"],
                    "PROCESSED",
                    event["created_at"],
                    processed_at,
                    response_json
                )
            )

            if cursor.rowcount == 1:
                print(f"NEW EVENT PROCESSED: {event['event_id']}")
                return response

            existing = connection.execute(
                """
                SELECT customer_id, amount, currency, response_json
                FROM transactions
                WHERE event_id = ?
                """,
                (event["event_id"],)
            ).fetchone()

            if existing is None:
                raise ValueError(
                    f"Could not retrieve event: {event['event_id']}"
                )

            if (
                existing["customer_id"] != event["customer_id"]
                or existing["amount"] != event["amount"]
                or existing["currency"] != event["currency"]
            ):
                raise ValueError(
                    f"CONFLICTING EVENT PAYLOAD: {event['event_id']}"
                )

            print(f"DUPLICATE EVENT DETECTED: {event['event_id']}")

            return json.loads(existing["response_json"])

    finally:
        connection.close()


if __name__ == "__main__":

    events = generate_transaction_events()

    print("\nStarting transaction ingestion...\n")

    for event in events:
        result = process_event(event)
        print("Response:", json.dumps(result, indent=4))
        print("-" * 50)
