import json
import requests

from config import (
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
    TOPIC,
)
from domain.normalizer import build_error_reason, normalize_event


def insert_event_to_clickhouse(event: dict) -> None:
    query = """
    INSERT INTO travel.raw_user_events
    (
        event_id,
        user_id,
        event_type,
        event_time,
        properties,
        source_topic
    )
    FORMAT JSONEachRow
    """

    row = json.dumps(normalize_event(event), ensure_ascii=False)

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=row.encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        raise Exception(response.text)
    

def mark_dlq_event_as_recovered(event_id: str) -> None:
    query = f"""
    ALTER TABLE travel.dlq_user_events
    UPDATE
        recovered = 1,
        recovered_at = now64(3)
    WHERE event_id = '{event_id}'
    """

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        raise Exception(response.text)




def insert_event_to_dlq(payload: dict) -> None:
    query = """
    INSERT INTO travel.dlq_user_events
    (
        event_id,
        raw_event,
        error_reason,
        source_topic
    )
    FORMAT JSONEachRow
    """

    try:
        event_id = json.loads(payload["raw_event"]).get("event_id")
    except Exception:
        event_id = None

    row = json.dumps(
        {
            "event_id": event_id,
            "raw_event": payload["raw_event"],
            "error_reason": build_error_reason(
                payload["error_type"],
                payload["error_details"],
            ),
            "source_topic": TOPIC,
        },
        ensure_ascii=False,
    )

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=row.encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        raise Exception(response.text)