import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_PATH,
    SCHEMA_REGISTRY_URL,
)


logger = logging.getLogger(__name__)


def delivery_report(error, message):
    if error is not None:
        logger.error("Delivery failed: %s", error)
        return

    logger.info(
        "Produced Avro event to %s [partition=%s offset=%s]",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def create_producer() -> SerializingProducer:
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

    return SerializingProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "value.serializer": avro_serializer,
        }
    )