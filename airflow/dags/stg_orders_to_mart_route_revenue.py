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