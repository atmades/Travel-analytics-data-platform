import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("USER_EVENTS_TOPIC", "user.events.avro")
GROUP_ID = os.getenv(
    "USER_EVENTS_GROUP_ID",
    "user-event-clickhouse-consumer-observability-v4",
)

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "travel_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "travel_password")

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://schema_registry:8081",
)

DLQ_FALLBACK_FILE = os.getenv(
    "DLQ_FALLBACK_FILE",
    "/tmp/user_events_dlq_fallback.jsonl",
)

MAX_INFRA_RETRY_SLEEP_SECONDS = int(
    os.getenv("MAX_INFRA_RETRY_SLEEP_SECONDS", "30")
)