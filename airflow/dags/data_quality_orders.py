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


def insert_dq_result(
    check_name: str,
    check_status: str,
    check_value: float,
    check_threshold: float,
) -> None:
    query = f"""
    INSERT INTO travel.dq_results
    (
        check_name,
        check_status,
        check_value,
        check_threshold
    )
    FORMAT JSONEachRow
    """

    row = {
        "check_name": check_name,
        "check_status": check_status,
        "check_value": check_value,
        "check_threshold": check_threshold,
    }

    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=f"{row}\n".replace("'", '"'),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=20,
    )

    if not response.ok:
        print(response.text)
        raise Exception(response.text)


def check_stg_orders_not_empty():
    value = float(run_clickhouse_query("SELECT count() FROM travel.stg_orders"))
    status = "PASS" if value > 0 else "FAIL"

    insert_dq_result(
        check_name="stg_orders_not_empty",
        check_status=status,
        check_value=value,
        check_threshold=1,
    )

    if status == "FAIL":
        raise Exception("DQ failed: stg_orders is empty")


def check_stg_orders_duplicates():
    query = """
    SELECT count()
    FROM
    (
        SELECT order_id
        FROM travel.stg_orders
        GROUP BY order_id
        HAVING count() > 1
    )
    """

    value = float(run_clickhouse_query(query))
    status = "PASS" if value == 0 else "FAIL"

    insert_dq_result(
        check_name="stg_orders_duplicate_order_ids",
        check_status=status,
        check_value=value,
        check_threshold=0,
    )

    if status == "FAIL":
        raise Exception("DQ failed: duplicate order_id found")


def check_stg_orders_positive_price():
    query = """
    SELECT count()
    FROM travel.stg_orders
    WHERE status IN ('paid', 'completed')
      AND price <= 0
    """

    value = float(run_clickhouse_query(query))
    status = "PASS" if value == 0 else "FAIL"

    insert_dq_result(
        check_name="stg_orders_positive_price_for_revenue_orders",
        check_status=status,
        check_value=value,
        check_threshold=0,
    )

    if status == "FAIL":
        raise Exception("DQ failed: revenue orders with non-positive price")


def check_stg_orders_valid_status():
    query = """
    SELECT count()
    FROM travel.stg_orders
    WHERE status NOT IN (
        'paid',
        'completed',
        'cancelled',
        'env_config_test',
        'grafana_test',
        'kafka_ui_test'
    )
    """

    value = float(run_clickhouse_query(query))
    status = "PASS" if value == 0 else "FAIL"

    insert_dq_result(
        check_name="stg_orders_valid_status",
        check_status=status,
        check_value=value,
        check_threshold=0,
    )

    if status == "FAIL":
        raise Exception("DQ failed: invalid order status found")


with DAG(
    dag_id="data_quality_orders",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "data-quality", "orders"],
) as dag:

    not_empty = PythonOperator(
        task_id="check_stg_orders_not_empty",
        python_callable=check_stg_orders_not_empty,
    )

    duplicates = PythonOperator(
        task_id="check_stg_orders_duplicates",
        python_callable=check_stg_orders_duplicates,
    )

    positive_price = PythonOperator(
        task_id="check_stg_orders_positive_price",
        python_callable=check_stg_orders_positive_price,
    )

    valid_status = PythonOperator(
        task_id="check_stg_orders_valid_status",
        python_callable=check_stg_orders_valid_status,
    )

    not_empty >> duplicates >> positive_price >> valid_status