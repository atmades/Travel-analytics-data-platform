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

    google_ads >> meta_ads
