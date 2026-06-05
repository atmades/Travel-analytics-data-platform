import json

from config import TOPIC


def extract_order_id(event: dict):
    after = event.get("after")
    before = event.get("before")

    if after and after.get("order_id") is not None:
        return after["order_id"]

    if before and before.get("order_id") is not None:
        return before["order_id"]

    return None


def build_raw_cdc_row(event: dict) -> dict:
    return {
        "order_id": extract_order_id(event),
        "op": event.get("op"),
        "before_record": json.dumps(event.get("before"), ensure_ascii=False),
        "after_record": json.dumps(event.get("after"), ensure_ascii=False),
        "source_topic": TOPIC,
        "source_ts_ms": event.get("ts_ms"),
    }