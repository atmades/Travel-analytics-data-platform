import json
import time
import uuid
from datetime import datetime, timezone
from random import choice, randint

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema_registry:8081"
# TOPIC = "user.events"
TOPIC = "user.events.avro"
SCHEMA_PATH = "/app/schemas/avro/user_event.avsc"


def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}", flush=True)
    else:
        print(
            f"Produced Avro event to {message.topic()} "
            f"[partition={message.partition()} offset={message.offset()}]",
            flush=True,
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
        "properties": json.dumps(
            {
                "route": choice(
                    [
                        "Bangkok -> Phuket",
                        "Bangkok -> Chiang Mai",
                        "Hanoi -> Da Nang",
                    ]
                )
            },
            ensure_ascii=False,
        ),
        "device_type": choice(["ios", "android", "web"]),
    }


def main():
    schema_registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL,
        }
    )

    with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
        schema_str = schema_file.read()

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )

    producer = SerializingProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "value.serializer": avro_serializer,
        }
    )

    for _ in range(10):
        event = build_user_event()

        producer.produce(
            TOPIC,
            value=event,
            on_delivery=delivery_report,
        )

        producer.poll(0)
        time.sleep(1)

    producer.flush()


if __name__ == "__main__":
    main()