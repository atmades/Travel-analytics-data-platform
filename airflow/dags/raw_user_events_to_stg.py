from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


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