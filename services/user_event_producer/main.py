import logging
import time

from config import EVENTS_COUNT, EVENTS_DELAY_SECONDS, TOPIC
from domain.event_factory import build_user_event
from infrastructure.kafka import create_producer, delivery_report


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    producer = create_producer()

    logger.info(
        "Producing %s events to topic %s",
        EVENTS_COUNT,
        TOPIC,
    )

    for _ in range(EVENTS_COUNT):
        event = build_user_event()

        producer.produce(
            TOPIC,
            value=event,
            on_delivery=delivery_report,
        )

        producer.poll(0)
        time.sleep(EVENTS_DELAY_SECONDS)

    producer.flush()

    logger.info("Producer finished")


if __name__ == "__main__":
    main()