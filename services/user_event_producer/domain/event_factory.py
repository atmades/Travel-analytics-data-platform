import json
import uuid
from datetime import datetime, timezone
from random import choice, randint


EVENT_TYPES = [
    "search_performed",
    "booking_started",
    "payment_completed",
]

ROUTES = [
    "Bangkok -> Phuket",
    "Bangkok -> Chiang Mai",
    "Hanoi -> Da Nang",
]

DEVICE_TYPES = [
    "ios",
    "android",
    "web",
]


def build_user_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": randint(100, 999),
        "event_type": choice(EVENT_TYPES),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "properties": json.dumps(
            {
                "route": choice(ROUTES),
            },
            ensure_ascii=False,
        ),
        "device_type": choice(DEVICE_TYPES),
    }