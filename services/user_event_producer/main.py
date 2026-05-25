import json
import time
import uuid
from datetime import datetime, timezone
from random import choice, randint

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "user.events"


def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Produced event to {message.topic()} "
            f"[partition={message.partition()} offset={message.offset()}]"
        )


def build_user_event() -> dict:
    event_type = choice(
        [
            "search_performed",
            "booking_started",
            "payment_completed",
        ]
    )

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": randint(100, 999),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "properties": {
            "route": choice(
                [
                    "Bangkok -> Phuket",
                    "Bangkok -> Chiang Mai",
                    "Hanoi -> Da Nang",
                ]
            )
        },
    }


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )

    for _ in range(10):
        event = build_user_event()

        producer.produce(
            TOPIC,
            key=str(event["user_id"]),
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(1)

    producer.flush()


if __name__ == "__main__":
    main()