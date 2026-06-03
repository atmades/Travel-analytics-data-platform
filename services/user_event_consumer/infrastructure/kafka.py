from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config import (
    GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC,
)


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