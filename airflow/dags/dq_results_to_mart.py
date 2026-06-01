from datetime import datetime
import os
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "travel_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "travel_password")


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


def build_mart_dq_latest_results():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_dq_latest_results")

    query = """
    INSERT INTO travel.mart_dq_latest_results
    (
        check_name,
        check_status,
        check_value,
        check_threshold,
        checked_at
    )
    SELECT
        check_name,
        check_status,
        check_value,
        check_threshold,
        checked_at
    FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY check_name
                ORDER BY checked_at DESC
            ) AS rn
        FROM travel.dq_results
    )
    WHERE rn = 1
    """

    run_clickhouse_query(query)
    print("mart_dq_latest_results built successfully")


with DAG(
    dag_id="dq_results_to_mart",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "data-quality", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_dq_latest_results",
        python_callable=build_mart_dq_latest_results,
    )