import json
import logging

import requests
from confluent_kafka import Consumer, KafkaError

from prometheus_client import Counter
from prometheus_client import start_http_server


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "orders.public.orders"
GROUP_ID = "orders-cdc-clickhouse-consumer"

CLICKHOUSE_URL = "http://clickhouse:8123"
CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"


CDC_EVENTS_RECEIVED = Counter(
    "cdc_events_received_total",
    "CDC events received from Kafka"
)

CDC_EVENTS_PROCESSED = Counter(
    "cdc_events_processed_total",
    "CDC events successfully written to ClickHouse"
)

CDC_EVENTS_FAILED = Counter(
    "cdc_events_failed_total",
    "CDC events failed during processing"
)


def extract_order_id(event: dict):
    after = event.get("after")
    before = event.get("before")

    if after and after.get("order_id") is not None:
        return after["order_id"]

    if before and before.get("order_id") is not None:
        return before["order_id"]

    return None


def insert_cdc_event_to_clickhouse(event: dict) -> None:
    query = """
    INSERT INTO travel.raw_cdc_orders
    (
        order_id,
        op,
        before_record,
        after_record,
        source_topic,
        source_ts_ms
    )
    FORMAT JSONEachRow
    """

    row = {
        "order_id": extract_order_id(event),
        "op": event.get("op"),
        "before_record": json.dumps(event.get("before"), ensure_ascii=False),
        "after_record": json.dumps(event.get("after"), ensure_ascii=False),
        "source_topic": TOPIC,
        "source_ts_ms": event.get("ts_ms"),
    }

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=json.dumps(row, ensure_ascii=False).encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        raise Exception(response.text)


def main():
    start_http_server(8002)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([TOPIC])

    logger.info("Orders CDC consumer started. Listening topic: %s", TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
                continue

            CDC_EVENTS_RECEIVED.inc()

            raw_value = msg.value().decode("utf-8")

            try:
                event = json.loads(raw_value)

                insert_cdc_event_to_clickhouse(event)

                logger.info(
                    "Inserted CDC event order_id=%s op=%s",
                    extract_order_id(event),
                    event.get("op"),
                )

                consumer.commit(msg)
                CDC_EVENTS_PROCESSED.inc()

            except Exception:
                CDC_EVENTS_FAILED.inc()
                logger.exception("Failed to process CDC message. Offset not committed.")

    except KeyboardInterrupt:
        logger.info("Orders CDC consumer stopped")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()