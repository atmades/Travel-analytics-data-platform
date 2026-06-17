"""
Build Booking Analytics marts.

Purpose:
- Create business-facing booking analytics marts.
- Provide operational, revenue, and booking lifecycle KPIs.
- Support reporting and dashboarding for the booking domain.

Business Questions:

Daily Bookings:
- How many bookings are created each day?
- How many bookings are paid or cancelled?
- How much revenue is generated per day?

Route Revenue:
- Which routes generate the most revenue?
- Which transport types perform best?
- How many paid bookings exist by route?

Booking Status:
- What is the current distribution of booking statuses?
- How many bookings are created, paid, or cancelled?

Input:
- travel.stg_bookings_latest

Outputs:
- travel.mart_daily_bookings
- travel.mart_route_revenue
- travel.mart_booking_status

Business Rules:
- Only the latest booking version is used.
- ReplacingMergeTree deduplication is applied through FINAL.
- Revenue is calculated from paid bookings only.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def build_mart_daily_bookings():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_daily_bookings")

    query = """
    INSERT INTO travel.mart_daily_bookings
    (
        booking_date,
        total_bookings,
        paid_bookings,
        created_bookings,
        cancelled_bookings,
        total_revenue
    )
    SELECT
        toDate(created_at) AS booking_date,
        count() AS total_bookings,
        countIf(status = 'paid') AS paid_bookings,
        countIf(status = 'created') AS created_bookings,
        countIf(status = 'cancelled') AS cancelled_bookings,
        sumIf(price, status = 'paid') AS total_revenue
    FROM travel.stg_bookings_latest FINAL
    GROUP BY booking_date
    """

    run_clickhouse_query(query)
    print("mart_daily_bookings built")


def build_mart_route_revenue():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_route_revenue")

    query = """
    INSERT INTO travel.mart_route_revenue
    (
        route,
        transport_type,
        total_bookings,
        paid_bookings,
        total_revenue
    )
    SELECT
        route,
        transport_type,
        count() AS total_bookings,
        countIf(status = 'paid') AS paid_bookings,
        sumIf(price, status = 'paid') AS total_revenue
    FROM travel.stg_bookings_latest FINAL
    GROUP BY
        route,
        transport_type
    """

    run_clickhouse_query(query)
    print("mart_route_revenue built")


def build_mart_booking_status():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_booking_status")

    query = """
    INSERT INTO travel.mart_booking_status
    (
        status,
        total_bookings
    )
    SELECT
        status,
        count() AS total_bookings
    FROM travel.stg_bookings_latest FINAL
    GROUP BY status
    """

    run_clickhouse_query(query)
    print("mart_booking_status built")


with DAG(
    dag_id="stg_bookings_to_marts",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "marts"],
) as dag:

    daily_bookings = PythonOperator(
        task_id="build_mart_daily_bookings",
        python_callable=build_mart_daily_bookings,
    )

    route_revenue = PythonOperator(
        task_id="build_mart_route_revenue",
        python_callable=build_mart_route_revenue,
    )

    booking_status = PythonOperator(
        task_id="build_mart_booking_status",
        python_callable=build_mart_booking_status,
    )

    daily_bookings
    route_revenue
    booking_status