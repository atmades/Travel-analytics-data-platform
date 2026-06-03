import json
import logging
import time
from datetime import datetime, timezone

import requests
from pydantic import ValidationError

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from prometheus_client import start_http_server


from config import (
    GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_INFRA_RETRY_SLEEP_SECONDS,
    SCHEMA_REGISTRY_URL,
    TOPIC,
)
from contracts.user_event import UserEvent
from infrastructure.clickhouse import insert_event_to_clickhouse, insert_event_to_dlq
from infrastructure.dlq import send_to_dlq_fallback, write_dlq_fallback_file
from observability.metrics import (
    EVENTS_DLQ,
    EVENTS_FAILED,
    EVENTS_PROCESSED,
    EVENTS_RECEIVED,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def create_consumer() -> DeserializingConsumer:
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

    return consumer


def process_message(message, consumer) -> None:
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

        consumer.commit(message)

        EVENTS_PROCESSED.inc()

        logger.info(
            "Inserted event_id=%s into ClickHouse",
            validated_event.event_id,
        )

    except (json.JSONDecodeError, ValidationError) as error:
        dlq_payload = {
            "raw_event": raw_value,
            "error_type": type(error).__name__,
            "error_details": (
                error.errors()
                if isinstance(error, ValidationError)
                else str(error)
            ),
        }

        insert_event_to_dlq(dlq_payload)

        consumer.commit(message)
        EVENTS_DLQ.inc()

        logger.warning("Invalid event sent to DLQ: %s", error)

    except requests.RequestException:
        raise

    except Exception as error:
        try:
            send_to_dlq_fallback(raw_value, error)

            consumer.commit(message)
            EVENTS_DLQ.inc()

            logger.exception(
                "Unexpected non-retryable error sent to DLQ: %s",
                error,
            )

        except Exception as dlq_error:
            EVENTS_FAILED.inc()

            write_dlq_fallback_file(raw_value, dlq_error)

            logger.exception(
                "Failed to send unexpected error to DLQ. "
                "Written to fallback file. Offset not committed: %s",
                dlq_error,
            )

            time.sleep(5)


def main():
    start_http_server(8001)

    consumer = create_consumer()

    logger.info("Consumer started. Listening topic: %s", TOPIC)

    infra_retry_attempt = 0

    try:
        while True:
            try:
                message = consumer.poll(1.0)
            except Exception as error:
                EVENTS_FAILED.inc()
                logger.exception("Poll/deserialization error: %s", error)
                continue

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                EVENTS_FAILED.inc()
                logger.error("Kafka error: %s", message.error())
                continue

            EVENTS_RECEIVED.inc()

            try:
                process_message(message, consumer)
                infra_retry_attempt = 0

            except requests.RequestException as error:
                infra_retry_attempt += 1

                sleep_seconds = min(
                    2 ** infra_retry_attempt,
                    MAX_INFRA_RETRY_SLEEP_SECONDS,
                )

                EVENTS_FAILED.inc()

                logger.exception(
                    "Infrastructure error. Offset not committed. "
                    "Retry attempt=%s, sleep=%ss: %s",
                    infra_retry_attempt,
                    sleep_seconds,
                    error,
                )

                time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info("Consumer stopped")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()