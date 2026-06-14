
"""
Raw Layer Data Quality Validation.

This DAG validates the health of the raw_bookings ingestion layer.

Checks:
1. Completeness
   Ensures records were loaded into raw_bookings.

2. Freshness
   Ensures the latest ingestion occurred within the expected SLA window.

3. Duplicate Detection
   Detects duplicate booking_id values within a single ingestion run.

Notes:
- The raw layer is append-only.
- Multiple versions of the same booking_id across different run_id values
  are expected and allowed.
- Business-level validation is performed later in the staging layer.
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def check_completeness():
    result = run_clickhouse_query("""
        SELECT count()
        FROM travel.raw_bookings
    """)

    count = int(result)

    if count == 0:
        raise Exception("Completeness check failed: raw_bookings is empty")

    print(f"Completeness check passed. Rows: {count}")


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

    print(
        f"Freshness check passed. "
        f"Last load was {minutes_since_last_load} minutes ago"
    )


def check_duplicates_within_run():
    result = run_clickhouse_query("""
        SELECT count()
        FROM
        (
            SELECT
                booking_id,
                run_id
            FROM travel.raw_bookings
            GROUP BY
                booking_id,
                run_id
            HAVING count() > 1
        )
    """)

    duplicates = int(result)

    if duplicates > 0:
        raise Exception(
            f"Duplicate check failed. "
            f"Found {duplicates} duplicated booking_id values within the same run_id"
        )

    print("Duplicate check passed. No duplicated booking_id within the same run_id")


with DAG(
    dag_id="data_quality_raw_bookings",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "data-quality", "raw"],
) as dag:

    completeness = PythonOperator(
        task_id="check_completeness",
        python_callable=check_completeness,
    )

    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
    )

    duplicates_within_run = PythonOperator(
        task_id="check_duplicates_within_run",
        python_callable=check_duplicates_within_run,
    )

    completeness >> freshness >> duplicates_within_run