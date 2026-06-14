"""
Build Data Quality monitoring mart.

Purpose:
- Aggregate the latest result of each Data Quality check.
- Provide a single source of truth for DQ dashboards.
- Simplify monitoring in Grafana and operational reviews.

Input:
- travel.dq_results

Output:
- travel.mart_dq_latest_results

Business Rules:
- Each DQ check may run many times over time.
- Only the most recent result for each check_name is kept in the mart.
- Historical DQ executions remain available in dq_results.

Examples:
- stg_orders_not_empty
- stg_orders_valid_status
- stg_bookings_freshness
- raw_bookings_completeness

Notes:
- dq_results is the historical audit table.
- mart_dq_latest_results is the operational monitoring table.
- Dashboards should query the mart instead of scanning the full history table.
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


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