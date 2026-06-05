import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

TOPIC = os.getenv(
    "ORDERS_CDC_TOPIC",
    "orders.public.orders",
)

GROUP_ID = os.getenv(
    "ORDERS_CDC_GROUP_ID",
    "orders-cdc-clickhouse-consumer",
)

CLICKHOUSE_URL = os.getenv(
    "CLICKHOUSE_URL",
    "http://clickhouse:8123",
)

CLICKHOUSE_USER = os.getenv(
    "CLICKHOUSE_USER",
    "travel_user",
)

CLICKHOUSE_PASSWORD = os.getenv(
    "CLICKHOUSE_PASSWORD",
    "travel_password",
)