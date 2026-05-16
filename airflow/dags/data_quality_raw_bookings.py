import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


CLICKHOUSE_URL = "http://clickhouse:8123"
CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"


def run_clickhouse_query(query: str):
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not response.ok:
        print(response.text)
        raise Exception(response.text)

    return response.text.strip()


def check_completeness():
    result = run_clickhouse_query("""
        SELECT count()
        FROM travel.raw_bookings
    """)

    count = int(result)

    if count == 0:
        raise Exception("Completeness check failed: raw_bookings is empty")

    print(f"Completeness check passed. Rows: {count}")


def check_uniqueness():
    result = run_clickhouse_query("""
        SELECT count()
        FROM
        (
            SELECT booking_id
            FROM travel.raw_bookings
            GROUP BY booking_id
            HAVING count() > 1
        )
    """)

    duplicates = int(result)

    if duplicates > 0:
        raise Exception(f"Uniqueness check failed. Duplicate booking_id count: {duplicates}")

    print("Uniqueness check passed. No duplicate booking_id found")


def check_freshness():
    result = run_clickhouse_query("""
        SELECT dateDiff('minute', max(loaded_at), now())
        FROM travel.raw_bookings
    """)

    minutes_since_last_load = int(result)

    if minutes_since_last_load > 120:
        raise Exception(
            f"Freshness check failed. Last load was {minutes_since_last_load} minutes ago"
        )

    print(f"Freshness check passed. Last load was {minutes_since_last_load} minutes ago")


with DAG(
    dag_id="data_quality_raw_bookings",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "data-quality"],
) as dag:

    completeness = PythonOperator(
        task_id="check_completeness",
        python_callable=check_completeness,
    )

    uniqueness = PythonOperator(
        task_id="check_uniqueness",
        python_callable=check_uniqueness,
    )

    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
    )

    completeness >> uniqueness >> freshness