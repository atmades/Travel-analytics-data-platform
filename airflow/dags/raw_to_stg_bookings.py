from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


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
                'loaded_at', toString(loaded_at)
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
    run_clickhouse_query("TRUNCATE TABLE travel.stg_bookings")

    query = """
    INSERT INTO travel.stg_bookings
    (
        booking_id,
        user_id,
        route,
        transport_type,
        status,
        price,
        currency,
        created_at,
        loaded_at
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
        loaded_at
    FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY booking_id
                ORDER BY loaded_at DESC
            ) AS rn
        FROM travel.raw_bookings
        WHERE booking_id != 0
          AND price >= 0
    )
    WHERE rn = 1
    """

    run_clickhouse_query(query)
    print("Valid records loaded to stg_bookings")


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