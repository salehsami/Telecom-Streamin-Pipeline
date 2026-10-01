from datetime import datetime, timedelta, timezone

from generator import TelecomEventGenerator
from models import EventType, TelecomEvent


class FraudScenarioGenerator:
    def __init__(self, seed: int = 42):
        self.generator = TelecomEventGenerator(seed=seed)

    def top_up_velocity(self) -> list[TelecomEvent]:
        customer_id = "C900001"
        device_id = "D900001"
        scenario_id = "top-up-velocity-001"

        start_time = datetime.now(timezone.utc)

        events = []

        for i in range(5):
            events.append(
                TelecomEvent(
                    event_id=f"topup-{i}",
                    scenario_id=scenario_id,
                    event_type=EventType.TOP_UP_ATTEMPT,
                    event_time=start_time + timedelta(seconds=i * 10),
                    ingested_at=start_time + timedelta(seconds=i * 10),
                    customer_id=customer_id,
                    country_code="BE",
                    device_id=device_id,
                    payload={
                        "amount": 50,
                        "currency": "EUR",
                        "status": "APPROVED",
                    },
                )
            )

        return events

    def sim_swap_risk(self) -> list[TelecomEvent]:
        customer_id = "C900002"
        scenario_id = "sim-swap-risk-001"

        start_time = datetime.now(timezone.utc)

        return [
            TelecomEvent(
                event_id="sim-swap-001",
                scenario_id=scenario_id,
                event_type=EventType.SIM_SWAP,
                event_time=start_time,
                ingested_at=start_time,
                customer_id=customer_id,
                country_code="BE",
                device_id="D900002",
                payload={
                    "old_sim_id": "SIM0001000",
                    "new_sim_id": "SIM0009000",
                },
            ),
            TelecomEvent(
                event_id="device-change-001",
                scenario_id=scenario_id,
                event_type=EventType.DEVICE_CHANGE,
                event_time=start_time + timedelta(minutes=2),
                ingested_at=start_time + timedelta(minutes=2),
                customer_id=customer_id,
                country_code="BE",
                device_id="D900003",
                payload={
                    "previous_device_id": "D900002",
                    "new_device_id": "D900003",
                },
            ),
            TelecomEvent(
                event_id="high-topup-001",
                scenario_id=scenario_id,
                event_type=EventType.TOP_UP_ATTEMPT,
                event_time=start_time + timedelta(minutes=5),
                ingested_at=start_time + timedelta(minutes=5),
                customer_id=customer_id,
                country_code="BE",
                device_id="D900003",
                payload={
                    "amount": 250,
                    "currency": "EUR",
                    "status": "APPROVED",
                },
            ),
        ]

    def impossible_travel(self) -> list[TelecomEvent]:
        customer_id = "C900003"
        scenario_id = "impossible-travel-001"

        start_time = datetime.now(timezone.utc)

        return [
            TelecomEvent(
                event_id="login-be-001",
                scenario_id=scenario_id,
                event_type=EventType.LOGIN,
                event_time=start_time,
                ingested_at=start_time,
                customer_id=customer_id,
                country_code="BE",
                device_id="D900004",
                payload={
                    "success": True,
                    "ip_address": "81.82.83.84",
                },
            ),
            TelecomEvent(
                event_id="login-us-001",
                scenario_id=scenario_id,
                event_type=EventType.LOGIN,
                event_time=start_time + timedelta(minutes=5),
                ingested_at=start_time + timedelta(minutes=5),
                customer_id=customer_id,
                country_code="US",
                device_id="D900004",
                payload={
                    "success": True,
                    "ip_address": "34.210.10.20",
                },
            ),
        ]

    def usage_spike(self) -> list[TelecomEvent]:
        customer_id = "C900004"
        scenario_id = "usage-spike-001"

        start_time = datetime.now(timezone.utc)

        events = []

        for i in range(10):
            events.append(
                TelecomEvent(
                    event_id=f"usage-spike-{i}",
                    scenario_id=scenario_id,
                    event_type=EventType.DATA_USAGE,
                    event_time=start_time + timedelta(seconds=i * 30),
                    ingested_at=start_time + timedelta(seconds=i * 30),
                    customer_id=customer_id,
                    country_code="BE",
                    device_id="D900005",
                    payload={
                        "megabytes": 800,
                    },
                )
            )

        return events