from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


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
    FROM
    (
        SELECT *
        FROM travel.stg_ads_latest FINAL
    ) AS ads
    LEFT JOIN travel.stg_orders AS orders
        ON ads.campaign_id = toString(orders.campaign_id)
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