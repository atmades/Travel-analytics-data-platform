
"""
Booking API ingestion pipeline.

Purpose:
- Extract booking data from the operational booking system.
- Store booking records in the Raw layer using an append-only pattern.
- Add ingestion metadata for lineage, audit, replay, and recovery workflows.

Input:
- Booking API (mock source)

Output:
- travel.raw_bookings

Business Rules:
- Raw layer is append-only.
- Existing records are never updated or deleted.
- Each ingestion run receives a unique run_id.
- loaded_at represents ingestion time, not business event time.
- Multiple versions of the same booking_id are expected and allowed.

Metadata:
- run_id identifies a specific ingestion execution.
- loaded_at is used for freshness checks and latest-state reconstruction.

Notes:
- Data validation is performed in downstream Data Quality DAGs.
- Deduplication is performed in the Staging layer.
- Latest booking state is reconstructed later using loaded_at.
"""


import json
from datetime import datetime, timezone
from uuid import uuid4

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


BOOKING_API_URL = "http://mock_booking_api:8000/bookings"


def normalize_booking(booking: dict) -> dict:
    raw_date = booking["created_at"]

    clean_date = (
        raw_date
        .split(".")[0]
        .replace("T", " ")
        .split("+")[0]
        .split("Z")[0]
    )

    booking["created_at"] = clean_date.strip()

    return booking


def enrich_booking(
    booking: dict,
    run_id: str,
    loaded_at: str,
) -> dict:
    booking = normalize_booking(booking)

    booking["run_id"] = run_id
    booking["loaded_at"] = loaded_at

    return booking


def load_bookings_to_clickhouse():
    response = requests.get(
        BOOKING_API_URL,
        timeout=10,
    )

    response.raise_for_status()

    bookings = response.json()

    if not bookings:
        print("No bookings received")
        return

    run_id = str(uuid4())

    loaded_at = (
        datetime
        .now(timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )

    rows = "\n".join(
        json.dumps(
            enrich_booking(
                booking.copy(),
                run_id,
                loaded_at,
            ),
            ensure_ascii=False,
        )
        for booking in bookings
    )

    print(
    f"Loaded {len(bookings)} bookings "
    f"(run_id={run_id}, loaded_at={loaded_at}) "
    f"to ClickHouse"
)

    insert_query = """
    INSERT INTO travel.raw_bookings
    (
        booking_id,
        user_id,
        route,
        transport_type,
        status,
        price,
        currency,
        created_at,
        loaded_at,
        run_id
    )
    FORMAT JSONEachRow
    """

    run_clickhouse_query(
        insert_query,
        data=rows.encode("utf-8"),
        timeout=10,
    )

    print(
        f"Loaded {len(bookings)} bookings "
        f"(run_id={run_id}) to ClickHouse"
    )


with DAG(
    dag_id="booking_api_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "bookings"],
) as dag:

    load_bookings = PythonOperator(
        task_id="load_bookings_to_clickhouse",
        python_callable=load_bookings_to_clickhouse,
    )