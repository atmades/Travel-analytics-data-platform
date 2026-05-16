from datetime import datetime
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


BOOKING_API_URL = "http://mock_booking_api:8000/bookings"
CLICKHOUSE_URL = "http://clickhouse:8123"

CLICKHOUSE_USER = "travel_user"
CLICKHOUSE_PASSWORD = "travel_password"



def normalize_booking(booking: dict) -> dict:
    raw_date = booking["created_at"]
    # Безопасно убираем таймзону и миллисекунды
    # Работает с форматами: "2026-05-16T03:34:28.772882+00:00", "...Z", "...+03:00"
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

    # Формируем JSONEachRow
    rows = "\n".join(
        json.dumps(normalize_booking(b.copy()), ensure_ascii=False) 
        for b in bookings
    )

    insert_query = """
    INSERT INTO travel.raw_bookings
    (booking_id, user_id, route, transport_type, status, price, currency, created_at)
    FORMAT JSONEachRow
    """

    clickhouse_response = requests.post(
        CLICKHOUSE_URL,
        params={"query": insert_query},
        data=rows.encode("utf-8"),
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10,
    )

    print("📦 Payload sent:")
    print(rows)  # Теперь тут будет "created_at": "2026-05-16 03:34:28"

    if not clickhouse_response.ok:
        print("❌ ClickHouse error:")
        print(clickhouse_response.text)
        raise Exception(clickhouse_response.text)

    print(f"✅ Loaded {len(bookings)} bookings to ClickHouse")


with DAG(
    dag_id="booking_api_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # запуск вручную
    catchup=False,
    tags=["travel", "bookings"],
) as dag:

    load_bookings = PythonOperator(
        task_id="load_bookings_to_clickhouse",
        python_callable=load_bookings_to_clickhouse,
    )