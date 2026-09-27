from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    DATA_USAGE = "DATA_USAGE"
    CALL_COMPLETED = "CALL_COMPLETED"
    SMS_SENT = "SMS_SENT"
    TOP_UP_ATTEMPT = "TOP_UP_ATTEMPT"
    LOGIN = "LOGIN"
    SIM_SWAP = "SIM_SWAP"
    DEVICE_CHANGE = "DEVICE_CHANGE"
    PAYMENT_ATTEMPT = "PAYMENT_ATTEMPT"


class TelecomEvent(BaseModel):
    schema_version: int = 1

    event_id: str

    scenario_id: str

    event_type: EventType

    event_time: datetime

    ingested_at: datetime

    customer_id: str

    country_code: str = Field(min_length=2, max_length=2)

    device_id: str

    payload: dict[str, Any]