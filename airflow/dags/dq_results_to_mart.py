from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


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