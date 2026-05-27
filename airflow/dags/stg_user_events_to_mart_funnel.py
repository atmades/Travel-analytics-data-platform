from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


CLICKHOUSE_URL = "http://clickhouse:8123"
CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"


def run_clickhouse_query(query: str) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=20,
    )

    if not response.ok:
        print(response.text)
        raise Exception(response.text)

    return response.text.strip()


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