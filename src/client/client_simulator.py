
import time
from datetime import datetime, timezone

from src.worker.ingestion_worker import process_event


CLIENT_TIMEOUT = 1.0
MAX_RETRIES = 3


def send_request(event, attempt):
    """
    Simulate sending an event to the ingestion worker.

    The first response is deliberately delayed.
    Subsequent responses are delivered quickly.
    """

    print(f"\nSending event: {event['event_id']}")
    print(f"Attempt: {attempt}")

    # Worker processes the event
    response = process_event(event)

    # Artificial network delay
    if attempt == 1:
        delay = 2.0
    else:
        delay = 0.2

    print(f"Simulating network delay: {delay} seconds")

    time.sleep(delay)

    return response


def send_with_retries(event):

    for attempt in range(1, MAX_RETRIES + 1):

        start_time = time.time()

        response = send_request(event, attempt)

        elapsed_time = time.time() - start_time

        print(f"Response time: {elapsed_time:.2f} seconds")

        if elapsed_time > CLIENT_TIMEOUT:

            print("CLIENT TIMEOUT: Response arrived too late.")

            if attempt < MAX_RETRIES:
                print("Retrying transaction...")

                continue

            print("Maximum retries reached.")
            return None

        print("CLIENT SUCCESS: Response received.")
        return response

    return None


if __name__ == "__main__":

    transaction_event = {
        "event_id": "TXN004",
        "customer_id": "CUST104",
        "amount": 3000.00,
        "currency": "INR",
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    print("=" * 55)
    print("TRANSACTION RETRY SIMULATION")
    print("=" * 55)

    result = send_with_retries(transaction_event)

    print("\nFINAL CLIENT RESULT:")
    print(result)
