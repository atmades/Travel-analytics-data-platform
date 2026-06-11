import json
import os

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema_registry:8081")
USER_EVENTS_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user.events.avro")
SCHEMA_PATH = os.getenv("USER_EVENT_SCHEMA_PATH", "/app/schemas/avro/user_event.avsc")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "travel_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "travel_password")

BATCH_SIZE = int(os.getenv("DLQ_REPLAY_BATCH_SIZE", "20"))


def run_clickhouse_query(query: str) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=20,
    )

    if not response.ok:
        raise Exception(response.text)

    return response.text.strip()


def fetch_dlq_events() -> list[dict]:
    query = f"""
    SELECT
        event_id,
        raw_event,
        source_topic,
        failed_at
    FROM travel.dlq_user_events
    WHERE recovered = 0
      AND source_topic = 'user.events.avro'
      AND replay_count < 3
    ORDER BY failed_at
    LIMIT {BATCH_SIZE}
    FORMAT JSONEachRow
    """

    result = run_clickhouse_query(query)

    if not result:
        return []

    return [json.loads(line) for line in result.splitlines()]


def normalize_event(raw_event: str) -> dict:
    event = json.loads(raw_event)

    if isinstance(event.get("properties"), dict):
        event["properties"] = json.dumps(
            event["properties"],
            ensure_ascii=False,
        )

    return event


def mark_as_replayed(event_ids: list[str]) -> None:
    if not event_ids:
        return

    ids = ", ".join(f"'{event_id}'" for event_id in event_ids)

    query = f"""
    ALTER TABLE travel.dlq_user_events
    UPDATE
        replayed = 1,
        replayed_at = now64(3),
        replay_count = replay_count + 1,
        last_replay_at = now64(3)
    WHERE event_id IN ({ids})
    """

    run_clickhouse_query(query)


def make_delivery_report(
    event_id: str,
    successfully_replayed_event_ids: list[str],
):
    def delivery_report(error, message):
        if error is not None:
            print(
                f"Replay delivery failed for event_id={event_id}: {error}",
                flush=True,
            )
            return

        successfully_replayed_event_ids.append(event_id)

        print(
            f"Replayed event_id={event_id} to {message.topic()} "
            f"[partition={message.partition()} offset={message.offset()}]",
            flush=True,
        )

    return delivery_report


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

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

    dlq_events = fetch_dlq_events()

    if not dlq_events:
        print("No DLQ events to replay", flush=True)
        return

    successfully_replayed_event_ids: list[str] = []

    for row in dlq_events:
        event_id = row["event_id"]

        try:
            event = normalize_event(row["raw_event"])

            producer.produce(
                USER_EVENTS_TOPIC,
                value=event,
                on_delivery=make_delivery_report(
                    event_id=event_id,
                    successfully_replayed_event_ids=successfully_replayed_event_ids,
                ),
            )

            producer.poll(0)

        except Exception as error:
            print(f"Failed to replay event_id={event_id}: {error}", flush=True)

    producer.flush()

    mark_as_replayed(successfully_replayed_event_ids)

    print(
        f"Successfully replayed {len(successfully_replayed_event_ids)} "
        f"of {len(dlq_events)} DLQ events",
        flush=True,
    )


if __name__ == "__main__":
    main()