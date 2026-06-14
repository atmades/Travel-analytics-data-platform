from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

doc_md="""
# Travel Analytics Platform Refresh

Master orchestration DAG for the Travel Analytics Data Platform.

## Purpose

Coordinates platform pipelines and enforces layer dependencies:

Raw → Data Quality → Staging → Data Quality → Marts

## Domains

### Bookings
Booking API → Raw → DQ → Staging → DQ → Marts

### Advertising
Ads APIs → Raw → Staging → Marts

### User Events
User Events → Staging → Funnel & Conversion Marts

### Orders CDC
PostgreSQL CDC → Staging → DQ → Orders Marts

### Cross-Domain Analytics
Ads + Orders → Campaign Performance Mart

DQ Results → DQ Monitoring Mart

## Execution Strategy

- Domain-oriented orchestration
- Data Quality before marts
- Fail-fast execution
- wait_for_completion enabled for dependency enforcement
"""


def trigger(dag_id: str, task_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )


with DAG(
    dag_id="platform_daily_refresh",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "orchestration"],
    doc_md="""
    Master orchestration DAG that chains domain pipelines in dependency order.

    Run this after `make init` to refresh bookings, ads, user events, CDC orders,
    and data quality marts in one workflow.
    """,
) as dag:
    booking_ingest = trigger("booking_api_to_clickhouse", "booking_ingest")
    dq_raw_bookings = trigger("data_quality_raw_bookings", "dq_raw_bookings")
    raw_to_stg_bookings = trigger("raw_to_stg_bookings", "raw_to_stg_bookings")
    dq_stg_bookings = trigger("data_quality_stg_bookings", "dq_stg_bookings")
    stg_bookings_marts = trigger("stg_bookings_to_marts", "stg_bookings_marts")

    ads_ingest = trigger("ads_api_to_clickhouse", "ads_ingest")
    raw_to_stg_ads = trigger("raw_ads_to_stg_ads", "raw_to_stg_ads")
    mart_ad_performance = trigger(
        "stg_ads_to_mart_ad_performance",
        "mart_ad_performance",
    )

    raw_to_stg_events = trigger("raw_user_events_to_stg", "raw_to_stg_events")
    mart_event_funnel = trigger(
        "stg_user_events_to_mart_funnel",
        "mart_event_funnel",
    )
    mart_booking_conversion = trigger(
        "stg_user_events_to_mart_booking_conversion",
        "mart_booking_conversion",
    )

    raw_to_stg_cdc = trigger("raw_cdc_orders_to_stg", "raw_to_stg_cdc")
    dq_orders = trigger("data_quality_orders", "dq_orders")
    mart_orders_status = trigger(
        "stg_orders_to_mart_orders_status",
        "mart_orders_status",
    )
    mart_route_revenue_orders = trigger(
        "stg_orders_to_mart_route_revenue",
        "mart_route_revenue_orders",
    )
    mart_campaign_performance = trigger(
        "stg_ads_orders_to_mart_campaign_performance",
        "mart_campaign_performance",
    )

    mart_dq_latest = trigger("dq_results_to_mart", "mart_dq_latest")

    (
        booking_ingest
        >> dq_raw_bookings
        >> raw_to_stg_bookings
        >> dq_stg_bookings
        >> stg_bookings_marts
    )

    ads_ingest >> raw_to_stg_ads >> mart_ad_performance

    raw_to_stg_events >> [mart_event_funnel, mart_booking_conversion]

    raw_to_stg_cdc >> dq_orders >> [mart_orders_status, mart_route_revenue_orders]

    [raw_to_stg_ads, raw_to_stg_cdc] >> mart_campaign_performance

    [
        stg_bookings_marts,
        mart_ad_performance,
        mart_event_funnel,
        mart_booking_conversion,
        mart_orders_status,
        mart_route_revenue_orders,
        mart_campaign_performance,
    ] >> mart_dq_latest
