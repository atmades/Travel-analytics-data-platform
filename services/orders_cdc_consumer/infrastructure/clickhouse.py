import json

import requests

from config import (
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
)
from domain.extractor import build_raw_cdc_row


def insert_cdc_event_to_clickhouse(event: dict) -> None:
    query = """
    INSERT INTO travel.raw_cdc_orders
    (
        order_id,
        op,
        before_record,
        after_record,
        source_topic,
        source_ts_ms
    )
    FORMAT JSONEachRow
    """

    row = build_raw_cdc_row(event)

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=json.dumps(row, ensure_ascii=False).encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        raise Exception(response.text)