
"""
Data Quality checks for the Bookings staging layer.

Purpose:
- Validate that booking records were successfully promoted from Raw to Staging.
- Ensure the latest booking state is available for downstream analytics.
- Verify that deduplication logic is working correctly.
- Detect stale staging data before marts are refreshed.

Input:
- travel.stg_bookings_latest

Checks:
1. Completeness
   Ensures staging contains booking records.

2. Uniqueness
   Ensures there is only one current version per booking_id.

3. Freshness
   Ensures the latest booking state was updated within the expected SLA window.

Business Rules:
- The raw layer is append-only.
- Duplicate booking_id values may exist in raw_bookings.
- Staging must expose a single latest version of each booking.
- ReplacingMergeTree deduplication is validated through FINAL queries.

Notes:
- All checks use FINAL because stg_bookings_latest is implemented with ReplacingMergeTree.
- This DAG validates the analytical staging layer, not the ingestion layer.
- Downstream marts should only consume validated staging data.
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def check_completeness():
    result = run_clickhouse_query("""
        SELECT count()
        FROM travel.stg_bookings_latest FINAL
    """)

    count = int(result)

    if count == 0:
        raise Exception("Completeness check failed: stg_bookings is empty")

    print(f"Completeness check passed. Rows: {count}")


def check_uniqueness():
    result = run_clickhouse_query("""
        SELECT count()
        FROM
        (
            SELECT booking_id
            FROM travel.stg_bookings_latest FINAL
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
        FROM travel.stg_bookings_latest FINAL
    """)

    minutes_since_last_load = int(result)

    if minutes_since_last_load > 120:
        raise Exception(
            f"Freshness check failed. Last staging load was {minutes_since_last_load} minutes ago"
        )

    print(f"Freshness check passed. Last staging load was {minutes_since_last_load} minutes ago")


with DAG(
    dag_id="data_quality_stg_bookings",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "data-quality", "staging"],
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