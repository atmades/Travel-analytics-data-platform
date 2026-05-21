from datetime import datetime
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


ADS_API_URL = "http://mock_ads_api:8000"
CLICKHOUSE_URL = "http://clickhouse:8123"

CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"


def run_clickhouse_query(query: str, data: bytes | None = None) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=data,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=20,
    )

    if not response.ok:
        print(response.text)
        raise Exception(response.text)

    return response.text.strip()


def save_raw_payload(source: str, endpoint: str, records: list[dict]) -> None:
    payload = json.dumps(records, ensure_ascii=False)

    row = json.dumps(
        {
            "source": source,
            "endpoint": endpoint,
            "payload": payload,
        },
        ensure_ascii=False,
    )

    query = """
    INSERT INTO travel.raw_ads_api_payloads
    (
        source,
        endpoint,
        payload
    )
    FORMAT JSONEachRow
    """

    run_clickhouse_query(query, data=row.encode("utf-8"))

    print(f"Saved raw payload for {source}/{endpoint}")


def load_ads(endpoint: str, table_name: str, source: str):
    response = requests.get(f"{ADS_API_URL}/{endpoint}", timeout=10)
    response.raise_for_status()

    records = response.json()

    if not records:
        print(f"No records received from {endpoint}")
        return

    # 1. Save original API response as immutable raw payload
    save_raw_payload(
        source=source,
        endpoint=endpoint,
        records=records,
    )

    # 2. Load normalized records to raw source-specific table
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

    run_clickhouse_query(
        insert_query,
        data=rows.encode("utf-8"),
    )

    print(f"Loaded {len(records)} records into {table_name}")


def load_google_ads():
    load_ads(
        endpoint="google-ads",
        table_name="raw_google_ads",
        source="google_ads",
    )


def load_meta_ads():
    load_ads(
        endpoint="meta-ads",
        table_name="raw_meta_ads",
        source="meta_ads",
    )


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