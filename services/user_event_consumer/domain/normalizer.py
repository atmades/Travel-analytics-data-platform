import json
from datetime import datetime, timezone

from config import TOPIC


def parse_event_time(value) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_event(event: dict) -> dict:
    return {
        "event_id": event["event_id"],
        "user_id": event["user_id"],
        "event_type": event["event_type"],
        "event_time": parse_event_time(event["event_time"]),
        "properties": json.dumps(event.get("properties", {}), ensure_ascii=False),
        "source_topic": TOPIC,
    }


def build_error_reason(error_type: str, error_details) -> str:
    return json.dumps(
        {
            "error_type": error_type,
            "error_details": error_details,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        },
        ensure_ascii=False,
    )