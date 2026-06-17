"""
Build Booking Conversion mart.
Conversion rates are calculated by unique users, not raw event counts.

Purpose:
- Calculate key conversion metrics across the booking funnel.
- Measure user progression from search to booking and payment.
- Provide product and business KPIs for conversion analysis.

Input:
- travel.stg_user_events

Output:
- travel.mart_booking_conversion

Metrics:
- searches
    Unique users who performed a search.

- bookings
    Unique users who started a booking.

- payments
    Unique users who completed a payment.

- search_to_booking_rate
    bookings / searches

- booking_to_payment_rate
    payments / bookings

Business Rules:
- Conversion rates are calculated using unique users.
- A user is counted once per funnel step.
- Division-by-zero scenarios return 0.

Funnel:
search_performed
    ↓
booking_started
    ↓
payment_completed

Notes:
- This mart provides high-level funnel conversion KPIs.
- Event-level investigation should use travel.stg_user_events.
- The mart is intended for BI dashboards and product analytics reporting.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


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
            SELECT uniqExact(user_id) AS cnt
            FROM travel.stg_user_events
            WHERE event_type = 'search_performed'
        ),
        bookings AS (
            SELECT uniqExact(user_id) AS cnt
            FROM travel.stg_user_events
            WHERE event_type = 'booking_started'
        ),
        payments AS (
            SELECT uniqExact(user_id) AS cnt
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