from datetime import datetime
from typing import Literal, Union

from pydantic import BaseModel, ConfigDict


class SearchPerformedProperties(BaseModel):
    route: str


class BookingStartedProperties(BaseModel):
    route: str


class PaymentCompletedProperties(BaseModel):
    route: str


UserEventType = Literal[
    "search_performed",
    "booking_started",
    "payment_completed",
]


UserEventProperties = Union[
    SearchPerformedProperties,
    BookingStartedProperties,
    PaymentCompletedProperties,
]


class UserEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    user_id: int
    event_type: UserEventType
    event_time: datetime
    properties: UserEventProperties