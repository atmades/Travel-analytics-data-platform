from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


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