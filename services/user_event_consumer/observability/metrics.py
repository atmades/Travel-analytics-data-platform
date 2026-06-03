from prometheus_client import Counter

EVENTS_RECEIVED = Counter(
    "events_received_total",
    "Events received from Kafka",
)

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Successfully processed events",
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Failed events",
)

EVENTS_DLQ = Counter(
    "events_sent_to_dlq_total",
    "Events routed to DLQ",
)