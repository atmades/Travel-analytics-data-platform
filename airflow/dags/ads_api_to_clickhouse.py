from datetime import datetime
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


ADS_API_URL = "http://mock_ads_api:8000"
CLICKHOUSE_URL = "http://clickhouse:8123"

CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"


def load_ads(endpoint: str, table_name: str):
    response = requests.get(f"{ADS_API_URL}/{endpoint}", timeout=10)
    response.raise_for_status()

    records = response.json()

    if not records:
        print(f"No records received from {endpoint}")
        return

    rows = "\n".join(
        json.dumps(record, ensure_ascii=False)
        for record in records
    )

    insert_query = f"""
    INSERT INTO travel.{table_name}
    (
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend,
        platform
    )
    FORMAT JSONEachRow
    """

    clickhouse_response = requests.post(
        CLICKHOUSE_URL,
        params={"query": insert_query},
        data=rows.encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    if not clickhouse_response.ok:
        print(clickhouse_response.text)
        raise Exception(clickhouse_response.text)

    print(f"Loaded {len(records)} records into {table_name}")


def load_google_ads():
    load_ads("google-ads", "raw_google_ads")


def load_meta_ads():
    load_ads("meta-ads", "raw_meta_ads")


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