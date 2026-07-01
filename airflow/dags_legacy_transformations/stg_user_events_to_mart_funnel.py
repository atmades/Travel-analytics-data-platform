"""
Build User Event Funnel mart.

Purpose:
- Aggregate user behavior events into a funnel-friendly analytical dataset.
- Measure event volume and user participation for each event type.
- Provide product analytics metrics for reporting and dashboarding.

Input:
- travel.stg_user_events

Output:
- travel.mart_user_event_funnel

Metrics:
- users_count
    Number of unique users who generated the event.

- events_count
    Total number of events generated.

Business Rules:
- User uniqueness is calculated using user_id.
- Event counts are aggregated by event_type.
- One row is produced per event_type.

Example Event Types:
- app_open
- search
- booking_started
- booking_completed

Notes:
- This mart provides a high-level funnel view of user activity.
- Detailed event-level analysis should use stg_user_events.
- The mart is rebuilt from staging data on each execution.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def build_mart_user_event_funnel():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_user_event_funnel")

    query = """
    INSERT INTO travel.mart_user_event_funnel
    (
        event_type,
        users_count,
        events_count
    )
    SELECT
        event_type,
        uniqExact(user_id) AS users_count,
        count() AS events_count
    FROM travel.stg_user_events
    GROUP BY event_type
    """

    run_clickhouse_query(query)
    print("mart_user_event_funnel built successfully")


with DAG(
    dag_id="stg_user_events_to_mart_funnel",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "kafka", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_user_event_funnel",
        python_callable=build_mart_user_event_funnel,
    )