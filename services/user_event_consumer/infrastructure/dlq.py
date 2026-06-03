import json
from datetime import datetime, timezone

from infrastructure.clickhouse import insert_event_to_dlq
from config import DLQ_FALLBACK_FILE, TOPIC


def send_to_dlq_fallback(raw_value: str, error: Exception) -> None:
    dlq_payload = {
        "raw_event": raw_value or "{}",
        "error_type": type(error).__name__,
        "error_details": str(error),
    }

    insert_event_to_dlq(dlq_payload)


def write_dlq_fallback_file(raw_value: str, error: Exception) -> None:
    fallback_record = {
        "raw_event": raw_value,
        "error_type": type(error).__name__,
        "error_details": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source_topic": TOPIC,
    }

    with open(DLQ_FALLBACK_FILE, "a", encoding="utf-8") as file:
        file.write(json.dumps(fallback_record, ensure_ascii=False) + "\n")