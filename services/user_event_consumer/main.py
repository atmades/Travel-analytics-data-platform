import json
import logging
import time
import os
from datetime import datetime, timezone
from pydantic import ValidationError

from contracts.user_event import UserEvent

import requests

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from prometheus_client import Counter
from prometheus_client import start_http_server


# KAFKA Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

TOPIC = os.getenv(
    "USER_EVENTS_TOPIC",
    "user.events.avro",
)

GROUP_ID = os.getenv(
    "USER_EVENTS_GROUP_ID",
    "user-event-clickhouse-consumer-observability-v4",
)


# CLICKHOUSE Settings
CLICKHOUSE_URL = os.getenv(
    "CLICKHOUSE_URL",
    "http://clickhouse:8123",
)

CLICKHOUSE_USER = os.getenv(
    "CLICKHOUSE_USER",
    "travel_user",
)

CLICKHOUSE_PASSWORD = os.getenv(
    "CLICKHOUSE_PASSWORD",
    "travel_password",
)

# SCHEMA_REGISTRY 
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://schema-registry:8081",
)

# PROMETHEUS counters
EVENTS_RECEIVED = Counter(
    "events_received_total",
    "Events received from Kafka"
)

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Successfully processed events"
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Failed events"
)

EVENTS_DLQ = Counter(
    "events_sent_to_dlq_total",
    "Events routed to DLQ"
)


def normalize_event(event: dict) -> dict:
    raw_event_time = event["event_time"]

    if isinstance(raw_event_time, str):
        event_time = raw_event_time.split(".")[0].replace("T", " ").split("+")[0].split("Z")[0]
    else:
        event_time = raw_event_time.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "event_id": event["event_id"],
        "user_id": event["user_id"],
        "event_type": event["event_type"],
        "event_time": event_time,
        "properties": json.dumps(event.get("properties", {}), ensure_ascii=False),
        "source_topic": TOPIC,
    }


def insert_event_to_clickhouse(event: dict) -> None:
    query = """
    INSERT INTO travel.raw_user_events
    (
        event_id,
        user_id,
        event_type,
        event_time,
        properties,
        source_topic
    )
    FORMAT JSONEachRow
    """

    row = json.dumps(normalize_event(event), ensure_ascii=False)

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=row.encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        print(response.text)
        raise Exception(response.text)


def insert_event_to_dlq(payload: dict) -> None:
    query = """
    INSERT INTO travel.dlq_user_events
    (
        event_id,
        raw_event,
        error_reason,
        source_topic
    )
    FORMAT JSONEachRow
    """

    try:
        event_id = json.loads(payload["raw_event"]).get("event_id")
    except Exception:
        event_id = None

    row = json.dumps(
        {
            "event_id": event_id,
            "raw_event": payload["raw_event"],
            "error_reason": json.dumps(
                {
                    "error_type": payload["error_type"],
                    "error_details": payload["error_details"],
                    "ingested_at": payload["ingested_at"],
                },
                ensure_ascii=False,
            ),
            "source_topic": TOPIC,
        },
        ensure_ascii=False,
    )

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=row.encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        raise Exception(response.text)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def main():
    start_http_server(8001)

    schema_registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL,
        }
    )

    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
    )

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "value.deserializer": avro_deserializer,
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Consumer started. Listening topic: {TOPIC}", flush=True)

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            EVENTS_RECEIVED.inc()

            if message.error():
                print(f"Kafka error: {message.error()}", flush=True)
                continue

            raw_value = ""

            try:
                event_dict = message.value()

                if isinstance(event_dict.get("properties"), str):
                    event_dict["properties"] = json.loads(event_dict["properties"])

                raw_value = json.dumps(event_dict, ensure_ascii=False)

                validated_event = UserEvent(**event_dict)

                insert_event_to_clickhouse(
                    validated_event.model_dump(mode="json")
                )

                print(
                    f"Inserted event_id={validated_event.event_id} into ClickHouse",
                    flush=True,
                )

                consumer.commit(message)
                EVENTS_PROCESSED.inc()

            except (json.JSONDecodeError, ValidationError) as error:
                dlq_payload = {
                    "raw_event": raw_value,
                    "error_type": type(error).__name__,
                    "error_details": (
                        error.errors()
                        if isinstance(error, ValidationError)
                        else str(error)
                    ),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }

                insert_event_to_dlq(dlq_payload)

                print(
                    f"Invalid event sent to DLQ: {error}",
                    flush=True,
                )

                consumer.commit(message)
                EVENTS_DLQ.inc()

            except Exception as error:
                print(
                    f"Unexpected processing error: {error}",
                    flush=True,
                )

                EVENTS_FAILED.inc()

    except KeyboardInterrupt:
        print("Consumer stopped", flush=True)

    finally:
        consumer.close()
        

if __name__ == "__main__":
    main()