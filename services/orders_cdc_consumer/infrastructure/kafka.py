from confluent_kafka import Consumer

from config import GROUP_ID, KAFKA_BOOTSTRAP_SERVERS, TOPIC


def create_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([TOPIC])

    return consumer