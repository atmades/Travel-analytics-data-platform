from datetime import datetime
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.clickhouse_client import run_clickhouse_query

BOOKING_API_URL = "http://mock_booking_api:8000/bookings"




def normalize_booking(booking: dict) -> dict:
    raw_date = booking["created_at"]
    clean_date = raw_date.split(".")[0].replace("T", " ").split("+")[0].split("Z")[0]
    booking["created_at"] = clean_date.strip()
    return booking


def load_bookings_to_clickhouse():
    response = requests.get(BOOKING_API_URL, timeout=10)
    response.raise_for_status()
    bookings = response.json()

    if not bookings:
        print("No bookings received")
        return

    rows = "\n".join(
        json.dumps(normalize_booking(booking.copy()), ensure_ascii=False)
        for booking in bookings
    )

    insert_query = """
    INSERT INTO travel.raw_bookings
    (booking_id, user_id, route, transport_type, status, price, currency, created_at)
    FORMAT JSONEachRow
    """

    run_clickhouse_query(
        insert_query, 
        data=rows.encode("utf-8"), 
        timeout=10)
    
    print(f"Loaded {len(bookings)} bookings to ClickHouse")


with DAG(
    dag_id="booking_api_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "bookings"],
) as dag:

    load_bookings = PythonOperator(
        task_id="load_bookings_to_clickhouse",
        python_callable=load_bookings_to_clickhouse,
    )
