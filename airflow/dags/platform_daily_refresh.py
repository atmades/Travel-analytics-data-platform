"""
# Travel Analytics Platform Refresh

Master orchestration DAG for the Travel Analytics Data Platform.

## Purpose

Coordinates platform refresh across ingestion, data quality, dbt transformations,
and monitoring marts.

## Current Architecture

Airflow is responsible for orchestration and ingestion.

dbt is responsible for:
- staging models
- mart models
- SQL transformations
- model-level data tests

## Execution Strategy

- Trigger raw ingestion DAGs first
- Run raw/business data quality checks where needed
- Run dbt analytics transformations
- Refresh the latest DQ monitoring mart
- Child DAGs are triggered asynchronously for local SequentialExecutor compatibility
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def trigger(dag_id: str, task_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=False,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=10),
    )


with DAG(
    dag_id="platform_daily_refresh",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "orchestration"],
    doc_md="""
    Master orchestration DAG for the Travel Analytics Data Platform.

    This DAG triggers ingestion, data quality, dbt transformations, and
    monitoring mart refreshes in a single workflow.

    In the local Docker setup, child DAGs are triggered asynchronously because
    Airflow runs with SequentialExecutor.
    """,
) as dag:
    booking_ingest = trigger("booking_api_to_clickhouse", "booking_ingest")
    ads_ingest = trigger("ads_api_to_clickhouse", "ads_ingest")

    dq_raw_bookings = trigger("data_quality_raw_bookings", "dq_raw_bookings")
    dq_orders = trigger("data_quality_orders", "dq_orders")

    dbt_transformations = trigger("dbt_travel_analytics", "dbt_transformations")

    mart_dq_latest = trigger("dq_results_to_mart", "mart_dq_latest")

    [booking_ingest, ads_ingest] >> dbt_transformations

    booking_ingest >> dq_raw_bookings
    dbt_transformations >> dq_orders

    [dq_raw_bookings, dq_orders, dbt_transformations] >> mart_dq_latest