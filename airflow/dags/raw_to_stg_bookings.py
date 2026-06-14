"""
Purpose:
Build the latest booking state from the append-only raw layer.

Input:
travel.raw_bookings

Output:
travel.stg_bookings_latest

Business Rules:
- booking_id must not be 0
- price must be non-negative
- latest version is determined by loaded_at
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def load_invalid_records_to_dlq():
    query = """
    INSERT INTO travel.dlq_bookings
    (booking_id, raw_record, error_reason)
    SELECT
        booking_id,
        toJSONString(
            map(
                'booking_id', toString(booking_id),
                'user_id', toString(user_id),
                'route', route,
                'transport_type', transport_type,
                'status', status,
                'price', toString(price),
                'currency', currency,
                'created_at', toString(created_at),
                'loaded_at', toString(loaded_at),
                'run_id', run_id
            )
        ) AS raw_record,
        multiIf(
            booking_id = 0, 'booking_id is missing or zero',
            price < 0, 'price is negative',
            'unknown validation error'
        ) AS error_reason
    FROM travel.raw_bookings
    WHERE booking_id = 0
       OR price < 0
    """

    run_clickhouse_query(query)
    print("Invalid records loaded to dlq_bookings")


def load_valid_records_to_staging():
    query = """
    INSERT INTO travel.stg_bookings_latest
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
    SELECT
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
    FROM travel.raw_bookings
    WHERE booking_id != 0
      AND price >= 0
      AND loaded_at > (
          SELECT ifNull(max(loaded_at), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
          FROM travel.stg_bookings_latest
      )
    """

    run_clickhouse_query(query)
    print("New valid records loaded to stg_bookings_latest")


with DAG(
    dag_id="raw_to_stg_bookings",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "staging"],
) as dag:

    invalid_to_dlq = PythonOperator(
        task_id="load_invalid_records_to_dlq",
        python_callable=load_invalid_records_to_dlq,
    )

    valid_to_staging = PythonOperator(
        task_id="load_valid_records_to_staging",
        python_callable=load_valid_records_to_staging,
    )

    invalid_to_dlq >> valid_to_staging