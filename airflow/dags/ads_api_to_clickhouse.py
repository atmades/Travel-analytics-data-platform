"""
Advertising API ingestion pipeline.

Purpose:
- Extract advertising campaign metrics from external marketing platforms.
- Store raw API payloads for audit, replay, and troubleshooting.
- Load normalized advertising metrics into ClickHouse raw tables.

Inputs:
- Google Ads API (mock adapter)
- Meta Ads API (mock adapter)

Outputs:
- travel.raw_ads_api_payloads
- travel.raw_google_ads
- travel.raw_meta_ads

Business Rules:
- Raw API payloads are stored unchanged for traceability.
- Advertising metrics are normalized into a common schema.
- Empty API responses are skipped.
- Each platform is ingested independently.

Notes:
- This DAG represents the Raw ingestion layer.
- Data Quality validation and business transformations are performed in downstream DAGs.
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.ads_loader import load_google_ads, load_meta_ads


with DAG(
    dag_id="ads_api_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "ads"],
) as dag:

    google_ads = PythonOperator(
        task_id="load_google_ads",
        python_callable=load_google_ads,
    )

    meta_ads = PythonOperator(
        task_id="load_meta_ads",
        python_callable=load_meta_ads,
    )
