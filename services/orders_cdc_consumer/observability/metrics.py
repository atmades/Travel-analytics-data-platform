from prometheus_client import Counter


CDC_EVENTS_RECEIVED = Counter(
    "cdc_events_received_total",
    "CDC events received from Kafka",
)

CDC_EVENTS_PROCESSED = Counter(
    "cdc_events_processed_total",
    "CDC events successfully written to ClickHouse",
)

CDC_EVENTS_FAILED = Counter(
    "cdc_events_failed_total",
    "CDC events failed during processing",
)