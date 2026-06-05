import json
import logging

from confluent_kafka import KafkaError
from prometheus_client import start_http_server

from config import TOPIC
from domain.extractor import extract_order_id
from infrastructure.clickhouse import insert_cdc_event_to_clickhouse
from infrastructure.kafka import create_consumer
from observability.metrics import (
    CDC_EVENTS_FAILED,
    CDC_EVENTS_PROCESSED,
    CDC_EVENTS_RECEIVED,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    start_http_server(8002)

    consumer = create_consumer()

    logger.info("Orders CDC consumer started. Listening topic: %s", TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    CDC_EVENTS_FAILED.inc()
                    logger.error("Kafka error: %s", msg.error())
                continue

            CDC_EVENTS_RECEIVED.inc()

            raw_value = msg.value().decode("utf-8")

            try:
                event = json.loads(raw_value)

                insert_cdc_event_to_clickhouse(event)

                consumer.commit(msg)
                CDC_EVENTS_PROCESSED.inc()

                logger.info(
                    "Inserted CDC event order_id=%s op=%s",
                    extract_order_id(event),
                    event.get("op"),
                )

            except Exception:
                CDC_EVENTS_FAILED.inc()
                logger.exception(
                    "Failed to process CDC message. Offset not committed."
                )

    except KeyboardInterrupt:
        logger.info("Orders CDC consumer stopped")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()