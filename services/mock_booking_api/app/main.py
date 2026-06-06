from fastapi import FastAPI
from datetime import datetime, timezone

app = FastAPI(title="Mock Booking API")


@app.get("/bookings")
def get_bookings():
    return [
        {
            "booking_id": 1,
            "user_id": 101,
            "route": "Bangkok -> Phuket",
            "transport_type": "bus",
            "status": "paid",
            "price": 35.50,
            "currency": "USD",
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        },
        {
            "booking_id": 2,
            "user_id": 102,
            "route": "Bangkok -> Chiang Mai",
            "transport_type": "train",
            "status": "created",
            "price": 22.00,
            "currency": "USD",
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }
    ]