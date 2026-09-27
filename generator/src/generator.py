import random
import uuid
from datetime import datetime, timezone

from models import EventType, TelecomEvent


class TelecomEventGenerator:
    def __init__(self, seed: int = 42):
        self.random = random.Random(seed)

    def _customer_id(self) -> str:
        return f"C{self.random.randint(1, 1000):06d}"

    def _device_id(self) -> str:
        return f"D{self.random.randint(1, 1500):06d}"

    def _country(self) -> str:
        return self.random.choice(["BE", "NL", "DE", "FR"])

    def _base_event(
        self,
        event_type: EventType,
        payload: dict,
        scenario_id: str = "normal-traffic",
    ) -> TelecomEvent:

        now = datetime.now(timezone.utc)

        return TelecomEvent(
            event_id=str(uuid.uuid4()),
            scenario_id=scenario_id,
            event_type=event_type,
            event_time=now,
            ingested_at=now,
            customer_id=self._customer_id(),
            country_code=self._country(),
            device_id=self._device_id(),
            payload=payload,
        )

    def data_usage(self) -> TelecomEvent:
        return self._base_event(
            EventType.DATA_USAGE,
            {
                "megabytes": round(
                    self.random.uniform(1, 1000),
                    2,
                )
            },
        )

    def call_completed(self) -> TelecomEvent:
        return self._base_event(
            EventType.CALL_COMPLETED,
            {
                "duration_seconds": self.random.randint(10, 3600),
                "destination_country": self._country(),
            },
        )

    def sms_sent(self) -> TelecomEvent:
        return self._base_event(
            EventType.SMS_SENT,
            {
                "destination_country": self._country(),
                "message_count": self.random.randint(1, 3),
            },
        )

    def top_up_attempt(self) -> TelecomEvent:
        amount = self.random.choice(
            [5, 10, 15, 20, 25, 50, 100]
        )

        return self._base_event(
            EventType.TOP_UP_ATTEMPT,
            {
                "amount": amount,
                "currency": "EUR",
                "status": self.random.choices(
                    ["APPROVED", "DECLINED"],
                    weights=[90, 10],
                )[0],
            },
        )

    def login(self) -> TelecomEvent:
        return self._base_event(
            EventType.LOGIN,
            {
                "success": self.random.choices(
                    [True, False],
                    weights=[95, 5],
                )[0],
                "ip_address": (
                    f"192.168."
                    f"{self.random.randint(0, 255)}."
                    f"{self.random.randint(1, 254)}"
                ),
            },
        )

    def sim_swap(self) -> TelecomEvent:
        return self._base_event(
            EventType.SIM_SWAP,
            {
                "old_sim_id": (
                    f"SIM{self.random.randint(1, 5000):07d}"
                ),
                "new_sim_id": (
                    f"SIM{self.random.randint(5001, 9999):07d}"
                ),
            },
        )

    def device_change(self) -> TelecomEvent:
        return self._base_event(
            EventType.DEVICE_CHANGE,
            {
                "previous_device_id": self._device_id(),
                "new_device_id": self._device_id(),
            },
        )

    def payment_attempt(self) -> TelecomEvent:
        return self._base_event(
            EventType.PAYMENT_ATTEMPT,
            {
                "amount": round(
                    self.random.uniform(5, 300),
                    2,
                ),
                "currency": "EUR",
                "status": self.random.choices(
                    ["APPROVED", "DECLINED"],
                    weights=[92, 8],
                )[0],
            },
        )

    def normal_event(self) -> TelecomEvent:
        event_generators = [
            self.data_usage,
            self.call_completed,
            self.sms_sent,
            self.top_up_attempt,
            self.login,
            self.sim_swap,
            self.device_change,
            self.payment_attempt,
        ]

        weights = [
            40,  # DATA_USAGE
            20,  # CALL
            15,  # SMS
            7,   # TOP-UP
            8,   # LOGIN
            2,   # SIM SWAP
            2,   # DEVICE CHANGE
            6,   # PAYMENT
        ]

        selected_generator = self.random.choices(
            event_generators,
            weights=weights,
            k=1,
        )[0]

        return selected_generator()