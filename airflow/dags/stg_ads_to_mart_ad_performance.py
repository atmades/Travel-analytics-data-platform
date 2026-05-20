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