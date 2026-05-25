import json
import logging
import time
from datetime import datetime, timezone
from pydantic import ValidationError

from contracts.user_event import UserEvent

import requests
from confluent_kafka import Consumer



KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "user.events"
GROUP_ID = "user-event-clickhouse-consumer-v3"

CLICKHOUSE_URL = "http://clickhouse:8123"
CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"


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
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Consumer started. Listening topic: {TOPIC}", flush=True)

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}", flush=True)
                continue

            raw_value = message.value().decode("utf-8")

            try:
                event_dict = json.loads(raw_value)
                validated_event = UserEvent(**event_dict)

                insert_event_to_clickhouse(
                    validated_event.model_dump(mode="json")
                )

                print(
                    f"Inserted event_id={validated_event.event_id} into ClickHouse",
                    flush=True,
                )

            except (json.JSONDecodeError, ValidationError) as error:
                dlq_payload = {
                    "raw_event": raw_value,
                    "error_type": type(error).__name__,
                    "error_details": error.errors() if isinstance(error, ValidationError) else str(error),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),

                }
                insert_event_to_dlq(dlq_payload)
                
                print(
                    f"Invalid event sent to DLQ: {error}",
                    flush=True,
                )

    except KeyboardInterrupt:
        print("Consumer stopped", flush=True)

    finally:
        consumer.close()
        

if __name__ == "__main__":
    main()