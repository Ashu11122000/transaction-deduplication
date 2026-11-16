
import json
from datetime import datetime, timezone


def generate_transaction_events():
    """
    Generate mock transaction events for ingestion.
    """

    current_time = datetime.now(timezone.utc).isoformat()

    events = [
        {
            "event_id": "TXN001",
            "customer_id": "CUST101",
            "amount": 1000.00,
            "currency": "INR",
            "created_at": current_time
        },
        {
            "event_id": "TXN002",
            "customer_id": "CUST102",
            "amount": 2500.00,
            "currency": "INR",
            "created_at": current_time
        },
        {
            "event_id": "TXN003",
            "customer_id": "CUST103",
            "amount": 500.00,
            "currency": "INR",
            "created_at": current_time
        }
    ]

    return events


if __name__ == "__main__":

    transactions = generate_transaction_events()

    print("Transaction events generated successfully!")
    print(f"Total events: {len(transactions)}")

    for event in transactions:
        print(json.dumps(event, indent=4))
