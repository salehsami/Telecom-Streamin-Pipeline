from datetime import datetime, timedelta, timezone

from generator import TelecomEventGenerator
from models import EventType, TelecomEvent


class EdgeCaseGenerator:
    def __init__(self, seed: int = 42):
        self.generator = TelecomEventGenerator(seed=seed)

    def duplicate_event(self) -> list[TelecomEvent]:
        event = self.generator.data_usage()

        return [
            event,
            event.model_copy(deep=True),
        ]

    def out_of_order(self) -> list[TelecomEvent]:
        now = datetime.now(timezone.utc)

        customer_id = "C910001"
        device_id = "D910001"

        event_1 = TelecomEvent(
            event_id="ooo-001",
            scenario_id="out-of-order-001",
            event_type=EventType.DATA_USAGE,
            event_time=now,
            ingested_at=now,
            customer_id=customer_id,
            country_code="BE",
            device_id=device_id,
            payload={
                "megabytes": 100,
            },
        )

        event_2 = TelecomEvent(
            event_id="ooo-002",
            scenario_id="out-of-order-001",
            event_type=EventType.DATA_USAGE,
            event_time=now + timedelta(minutes=2),
            ingested_at=now,
            customer_id=customer_id,
            country_code="BE",
            device_id=device_id,
            payload={
                "megabytes": 200,
            },
        )

        event_3 = TelecomEvent(
            event_id="ooo-003",
            scenario_id="out-of-order-001",
            event_type=EventType.DATA_USAGE,
            event_time=now + timedelta(minutes=1),
            ingested_at=now,
            customer_id=customer_id,
            country_code="BE",
            device_id=device_id,
            payload={
                "megabytes": 300,
            },
        )

        return [
            event_1,
            event_2,
            event_3,
        ]

    def late_event(self) -> list[TelecomEvent]:
        now = datetime.now(timezone.utc)

        event = TelecomEvent(
            event_id="late-001",
            scenario_id="late-event-001",
            event_type=EventType.LOGIN,
            event_time=now - timedelta(minutes=15),
            ingested_at=now,
            customer_id="C910002",
            country_code="BE",
            device_id="D910002",
            payload={
                "success": True,
                "ip_address": "81.82.83.84",
            },
        )

        return [event]

    def malformed_event(self) -> list[dict]:
        return [
            {
                "schema_version": 1,

                # event_id deliberately missing

                "scenario_id": "malformed-001",

                "event_type": "NOT_A_REAL_EVENT",

                "event_time": "definitely-not-a-date",

                "customer_id": 12345,

                # country_code deliberately missing

                "payload": {
                    "something": "broken"
                },
            }
        ]