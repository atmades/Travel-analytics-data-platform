from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.clients.clickhouse import run_clickhouse_query


def build_stg_orders():
    run_clickhouse_query("TRUNCATE TABLE travel.stg_orders")

    query = """
    INSERT INTO travel.stg_orders
    (
        order_id,
        user_id,
        route,
        transport_type,
        status,
        price,
        currency,
        campaign_id,
        created_at,
        updated_at,
        source_ts_ms
    )
    SELECT
        JSONExtractUInt(after_record, 'order_id') AS order_id,
        JSONExtractUInt(after_record, 'user_id') AS user_id,
        JSONExtractString(after_record, 'route') AS route,
        JSONExtractString(after_record, 'transport_type') AS transport_type,
        JSONExtractString(after_record, 'status') AS status,
        JSONExtractFloat(after_record, 'price') AS price,
        JSONExtractString(after_record, 'currency') AS currency,
        JSONExtractUInt(after_record, 'campaign_id') AS campaign_id,
        toDateTime(JSONExtractInt(after_record, 'created_at') / 1000000) AS created_at,
        toDateTime(JSONExtractInt(after_record, 'updated_at') / 1000000) AS updated_at,
        source_ts_ms
    FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY order_id
                ORDER BY source_ts_ms DESC, ingested_at DESC
            ) AS rn
        FROM travel.raw_cdc_orders
        WHERE op IN ('c', 'u')
          AND after_record != 'null'
    )
    WHERE rn = 1
    """

    run_clickhouse_query(query)
    print("stg_orders built successfully")


with DAG(
    dag_id="raw_cdc_orders_to_stg",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "cdc", "staging"],
) as dag:

    build_stg = PythonOperator(
        task_id="build_stg_orders",
        python_callable=build_stg_orders,
    )