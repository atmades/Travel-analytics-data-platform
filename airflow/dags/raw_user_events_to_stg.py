"""
Refactor stg_user_events to ReplacingMergeTree(event_id, ingested_at)

Build User Events staging layer.

Purpose:
- Transform raw Kafka user events into a deduplicated staging table.
- Keep the latest version of each event_id based on ingested_at.
- Prepare user behavior data for funnel and conversion marts.

Input:
- travel.raw_user_events

Output:
- travel.stg_user_events

Business Rules:
- event_id is the deduplication key.
- If the same event is ingested more than once, the latest ingested_at wins.
- Staging exposes one clean record per event_id.

Notes:
- Raw user events are produced through Kafka and stored in ClickHouse.
- This DAG rebuilds the staging table from raw data.
- Downstream marts should consume stg_user_events, not raw_user_events.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def build_stg_user_events():
    run_clickhouse_query("TRUNCATE TABLE travel.stg_user_events")

    query = """
    INSERT INTO travel.stg_user_events
    (
        event_id,
        user_id,
        event_type,
        event_time,
        properties,
        source_topic,
        ingested_at
    )
    SELECT
        event_id,
        user_id,
        event_type,
        event_time,
        properties,
        source_topic,
        ingested_at
    FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY event_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM travel.raw_user_events
    )
    WHERE rn = 1
    """

    run_clickhouse_query(query)
    print("stg_user_events built successfully")


with DAG(
    dag_id="raw_user_events_to_stg",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "kafka", "staging"],
) as dag:

    build_stg = PythonOperator(
        task_id="build_stg_user_events",
        python_callable=build_stg_user_events,
    )