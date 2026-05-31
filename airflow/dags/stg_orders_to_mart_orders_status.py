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