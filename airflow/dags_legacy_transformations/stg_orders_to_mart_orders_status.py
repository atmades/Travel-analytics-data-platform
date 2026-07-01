"""
Build Orders Status mart.

Purpose:
- Aggregate orders by lifecycle status.
- Provide operational and revenue KPIs for order monitoring.

Business Questions:
- How many orders are in each status?
- How much revenue is associated with each status?
- What share of orders are completed, cancelled, or still in progress?

Input:
- travel.stg_orders

Output:
- travel.mart_orders_status

Business Rules:
- Orders are grouped by status.
- Revenue is aggregated from order price.
- All order statuses are included in the analysis.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def build_mart_orders_status():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_orders_status")

    query = """
    INSERT INTO travel.mart_orders_status
    (
        status,
        orders_count,
        total_revenue
    )
    SELECT
        status,
        count() AS orders_count,
        sum(price) AS total_revenue
    FROM travel.stg_orders
    GROUP BY status
    """

    run_clickhouse_query(query)
    print("mart_orders_status built successfully")


with DAG(
    dag_id="stg_orders_to_mart_orders_status",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "orders", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_orders_status",
        python_callable=build_mart_orders_status,
    )