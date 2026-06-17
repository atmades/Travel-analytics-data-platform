"""
Build Advertising Performance mart.

Purpose:
- Calculate campaign-level advertising performance metrics.
- Provide marketing KPIs for campaign monitoring and optimization.
- Support reporting across advertising platforms.

Business Questions:
- Which campaigns generate the most traffic?
- Which campaigns have the highest CTR?
- How much does each click cost?
- Which platforms deliver the best advertising performance?

Input:
- travel.stg_ads_latest

Output:
- travel.mart_ad_performance

Metrics:
- clicks
- impressions
- spend
- ctr (Click-Through Rate)
- cpc (Cost Per Click)

Business Rules:
- Only the latest campaign version is used.
- ReplacingMergeTree deduplication is applied through FINAL.
- CTR = clicks / impressions.
- CPC = spend / clicks.
- Division-by-zero scenarios return 0.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


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
    FROM travel.stg_ads_latest FINAL
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