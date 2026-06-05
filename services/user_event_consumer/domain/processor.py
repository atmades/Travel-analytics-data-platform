"""
Message processing workflow for a single Kafka event.

Responsibilities:
- Deserialize and validate event payload
- Route valid events to ClickHouse
- Route invalid events to DLQ
- Handle unexpected errors via DLQ fallback
- Commit Kafka offsets only after successful handling
- Update Prometheus metrics and logs

Infrastructure retries (e.g. ClickHouse outages) are handled
in main.py to preserve at-least-once delivery semantics.
"""


import json
import logging
import time

import requests
from pydantic import ValidationError


from contracts.user_event import UserEvent
from infrastructure.dlq import send_to_dlq_fallback, write_dlq_fallback_file
from infrastructure.clickhouse import (
    insert_event_to_clickhouse,
    insert_event_to_dlq,
    mark_dlq_event_as_recovered,
)
from observability.metrics import EVENTS_DLQ, EVENTS_FAILED, EVENTS_PROCESSED


logger = logging.getLogger(__name__)


def process_message(message, consumer) -> None:
    raw_value = ""

    try:
        event_dict = message.value()

        if isinstance(event_dict.get("properties"), str):
            event_dict["properties"] = json.loads(event_dict["properties"])

        raw_value = json.dumps(event_dict, ensure_ascii=False)

        validated_event = UserEvent(**event_dict)

        insert_event_to_clickhouse(
            validated_event.model_dump(mode="json")
        )

        try:
            mark_dlq_event_as_recovered(str(validated_event.event_id))
        except Exception as error:
            logger.warning(
            "Failed to mark DLQ event as recovered for event_id=%s: %s",
            validated_event.event_id,
            error,
    )

        consumer.commit(message)

        EVENTS_PROCESSED.inc()

        logger.info(
            "Inserted event_id=%s into ClickHouse",
            validated_event.event_id,
        )

    except (json.JSONDecodeError, ValidationError) as error:
        dlq_payload = {
            "raw_event": raw_value,
            "error_type": type(error).__name__,
            "error_details": (
                error.errors()
                if isinstance(error, ValidationError)
                else str(error)
            ),
        }

        insert_event_to_dlq(dlq_payload)

        consumer.commit(message)
        EVENTS_DLQ.inc()

        logger.warning("Invalid event sent to DLQ: %s", error)

    except requests.RequestException:
        raise # send error of any problem with requests to main

    except Exception as error:
        try:
            send_to_dlq_fallback(raw_value, error)

            consumer.commit(message)
            EVENTS_DLQ.inc()

            logger.exception(
                "Unexpected non-retryable error sent to DLQ: %s",
                error,
            )

        except Exception as dlq_error:
            EVENTS_FAILED.inc()

            write_dlq_fallback_file(raw_value, dlq_error)

            logger.exception(
                "Failed to send unexpected error to DLQ. "
                "Written to fallback file. Offset not committed: %s",
                dlq_error,
            )

            time.sleep(5)