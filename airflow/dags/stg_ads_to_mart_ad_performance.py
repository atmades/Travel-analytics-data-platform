from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.clickhouse_client import run_clickhouse_query


def build_mart_ad_performance():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_ad_performance")

    query = """
    INSERT INTO travel.mart_ad_performance
    (
        platform,
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend,
        ctr,
        cpc
    )
    SELECT
        platform,
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend,
        if(impressions = 0, 0, clicks / impressions) AS ctr,
        if(clicks = 0, 0, spend / clicks) AS cpc
    FROM travel.stg_ads
    """

    run_clickhouse_query(query)
    print("mart_ad_performance built successfully")


with DAG(
    dag_id="stg_ads_to_mart_ad_performance",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "ads", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_ad_performance",
        python_callable=build_mart_ad_performance,
    )