from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


def build_mart_booking_conversion():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_booking_conversion")

    query = """
    INSERT INTO travel.mart_booking_conversion
    (
        metric,
        value
    )
    WITH
        searches AS (
            SELECT count() AS cnt
            FROM travel.stg_user_events
            WHERE event_type = 'search_performed'
        ),
        bookings AS (
            SELECT count() AS cnt
            FROM travel.stg_user_events
            WHERE event_type = 'booking_started'
        ),
        payments AS (
            SELECT count() AS cnt
            FROM travel.stg_user_events
            WHERE event_type = 'payment_completed'
        )
    SELECT 'searches' AS metric, toFloat64((SELECT cnt FROM searches)) AS value
    UNION ALL
    SELECT 'bookings', toFloat64((SELECT cnt FROM bookings))
    UNION ALL
    SELECT 'payments', toFloat64((SELECT cnt FROM payments))
    UNION ALL
    SELECT
        'search_to_booking_rate',
        if((SELECT cnt FROM searches) = 0, 0,
           (SELECT cnt FROM bookings) / (SELECT cnt FROM searches))
    UNION ALL
    SELECT
        'booking_to_payment_rate',
        if((SELECT cnt FROM bookings) = 0, 0,
           (SELECT cnt FROM payments) / (SELECT cnt FROM bookings))
    """

    run_clickhouse_query(query)
    print("mart_booking_conversion built successfully")


with DAG(
    dag_id="stg_user_events_to_mart_booking_conversion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "events", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_booking_conversion",
        python_callable=build_mart_booking_conversion,
    )