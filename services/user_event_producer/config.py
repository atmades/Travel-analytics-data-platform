import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema_registry:8081")
TOPIC = os.getenv("USER_EVENTS_TOPIC", "user.events.avro")
SCHEMA_PATH = os.getenv("USER_EVENT_SCHEMA_PATH", "/app/schemas/avro/user_event.avsc")
EVENTS_COUNT = int(os.getenv("USER_EVENTS_PRODUCER_COUNT", "10"))
EVENTS_DELAY_SECONDS = float(os.getenv("USER_EVENTS_PRODUCER_DELAY_SECONDS", "1"))