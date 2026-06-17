"""
Build Route Revenue mart.

Purpose:
- Calculate revenue metrics by travel route.
- Provide route-level sales performance KPIs.

Business Questions:
- Which routes generate the most revenue?
- How many orders were placed for each route?
- What is the average order value by route?

Input:
- travel.stg_orders

Output:
- travel.mart_route_revenue_from_orders

Business Rules:
- Only paid and completed orders contribute to revenue.
- Metrics are aggregated by route.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def build_mart_route_revenue():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_route_revenue_from_orders")

    query = """
    INSERT INTO travel.mart_route_revenue_from_orders
    (
        route,
        orders_count,
        total_revenue,
        avg_order_value
    )
    SELECT
        route,
        count() AS orders_count,
        sum(price) AS total_revenue,
        avg(price) AS avg_order_value
    FROM travel.stg_orders
    WHERE status IN ('paid', 'completed')
    GROUP BY route
    """

    run_clickhouse_query(query)
    print("mart_route_revenue_from_orders built successfully")


with DAG(
    dag_id="stg_orders_to_mart_route_revenue",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "orders", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_route_revenue",
        python_callable=build_mart_route_revenue,
    )