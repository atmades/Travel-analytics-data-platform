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


def build_mart_campaign_performance():
    run_clickhouse_query("TRUNCATE TABLE travel.mart_campaign_performance")

    query = """
    INSERT INTO travel.mart_campaign_performance
    (
        platform,
        campaign_id,
        campaign_name,
        spend,
        clicks,
        impressions,
        bookings,
        revenue,
        cpa,
        roas
    )
    SELECT
        ads.platform,
        ads.campaign_id,
        ads.campaign_name,
        ads.spend,
        ads.clicks,
        ads.impressions,
        countIf(orders.status IN ('paid', 'completed')) AS bookings,
        sumIf(orders.price, orders.status IN ('paid', 'completed')) AS revenue,
        if(countIf(orders.status IN ('paid', 'completed')) = 0, 0,
           ads.spend / countIf(orders.status IN ('paid', 'completed'))) AS cpa,
        if(ads.spend = 0, 0,
           sumIf(orders.price, orders.status IN ('paid', 'completed')) / ads.spend) AS roas
    FROM travel.stg_ads AS ads
    LEFT JOIN travel.stg_orders AS orders
        ON ads.campaign_id = orders.campaign_id
    GROUP BY
        ads.platform,
        ads.campaign_id,
        ads.campaign_name,
        ads.spend,
        ads.clicks,
        ads.impressions
    """

    run_clickhouse_query(query)
    print("mart_campaign_performance built successfully")


with DAG(
    dag_id="stg_ads_orders_to_mart_campaign_performance",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "ads", "orders", "marts"],
) as dag:

    build_mart = PythonOperator(
        task_id="build_mart_campaign_performance",
        python_callable=build_mart_campaign_performance,
    )