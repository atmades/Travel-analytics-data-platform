from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


def load_raw_ads_to_staging():
    run_clickhouse_query("TRUNCATE TABLE travel.stg_ads")

    query = """
    INSERT INTO travel.stg_ads
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
        SELECT
            *,
            row_number() OVER (
                PARTITION BY platform, campaign_id
                ORDER BY loaded_at DESC
            ) AS rn
        FROM
        (
            SELECT * FROM travel.raw_google_ads
            UNION ALL
            SELECT * FROM travel.raw_meta_ads
        )
    )
    WHERE rn = 1
    """

    run_clickhouse_query(query)
    print("stg_ads built successfully")


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