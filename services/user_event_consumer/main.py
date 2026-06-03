import logging
import time

import requests

from confluent_kafka import KafkaError
from prometheus_client import start_http_server

from config import (
    MAX_INFRA_RETRY_SLEEP_SECONDS,
    TOPIC,
)

from domain.processor import process_message

from infrastructure.kafka import create_consumer

from observability.metrics import (
    EVENTS_FAILED,
    EVENTS_RECEIVED,
)

# -----------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    start_http_server(8001)

    consumer = create_consumer()

    logger.info(
        "Consumer started. Listening topic: %s",
        TOPIC,
    )

    infra_retry_attempt = 0

    try:
        while True:
            try:
                message = consumer.poll(1.0)

            except Exception as error:
                EVENTS_FAILED.inc()

                logger.exception(
                    "Poll/deserialization error: %s",
                    error,
                )

                continue

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                EVENTS_FAILED.inc()

                logger.error(
                    "Kafka error: %s",
                    message.error(),
                )

                continue

            EVENTS_RECEIVED.inc()

            try:
                process_message(
                    message=message,
                    consumer=consumer,
                )

                infra_retry_attempt = 0

            except requests.RequestException as error:
                infra_retry_attempt += 1

                sleep_seconds = min(
                    2 ** infra_retry_attempt,
                    MAX_INFRA_RETRY_SLEEP_SECONDS,
                )

                EVENTS_FAILED.inc()

                logger.exception(
                    "Infrastructure error. "
                    "Offset not committed. "
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