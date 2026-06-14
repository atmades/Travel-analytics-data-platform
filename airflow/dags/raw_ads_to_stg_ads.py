"""
Build Advertising staging layer.

Purpose:
- Consolidate advertising data from multiple ad platforms.
- Store latest campaign state using a ClickHouse ReplacingMergeTree table.
- Create a unified dataset for downstream advertising marts.

Input:
- travel.raw_google_ads
- travel.raw_meta_ads

Output:
- travel.stg_ads_latest

Business Rules:
- Raw advertising tables are append-only ingestion history.
- Staging stores latest campaign versions.
- Deduplication key is (platform, campaign_id).
- loaded_at is used as the version column.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def load_raw_ads_to_staging():
    query = """
    INSERT INTO travel.stg_ads_latest
    (
        platform,
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend,
        loaded_at
    )
    SELECT
        platform,
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend,
        loaded_at
    FROM
    (
        SELECT * FROM travel.raw_google_ads
        UNION ALL
        SELECT * FROM travel.raw_meta_ads
    )
    WHERE loaded_at > (
        SELECT ifNull(
            max(loaded_at),
            toDateTime64('1970-01-01 00:00:00', 3, 'UTC')
        )
        FROM travel.stg_ads_latest FINAL
    )
    """

    run_clickhouse_query(query)
    print("New advertising records loaded to stg_ads_latest")


with DAG(
    dag_id="raw_ads_to_stg_ads",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "ads", "staging"],
) as dag:

    build_stg_ads = PythonOperator(
        task_id="load_raw_ads_to_staging",
        python_callable=load_raw_ads_to_staging,
    )