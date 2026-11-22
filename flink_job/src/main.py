import json
import math
from datetime import datetime, timezone

from pyflink.common import (
    SimpleStringSchema,
    WatermarkStrategy,
)
from pyflink.common.time import Duration

from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import (
    ListStateDescriptor,
    ValueStateDescriptor,
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"

RAW_TOPIC = "telecom.raw.events.v1"
CONSUMER_GROUP = "telecom-flink-raw-reader-v1"

VALID_TOPIC = "telecom.valid.events.v1"
DLQ_TOPIC = "telecom.dlq.events.v1"

USAGE_AGGREGATES_TOPIC = "telecom.usage.country.minute.v1"
LATE_USAGE_TOPIC = "telecom.usage.late-events.v1"

FRAUD_ALERTS_TOPIC = "telecom.fraud.alerts.v1"


# Fraud detection configuration

TOPUP_VELOCITY_COUNT = 5
TOPUP_VELOCITY_WINDOW_MS = 60_000

SIM_SWAP_WINDOW_MS = 600_000
SIM_SWAP_HIGH_VALUE_EUR = 50.0

LOGIN_COMPARISON_WINDOW_MS = 86_400_000
IMPOSSIBLE_TRAVEL_MIN_DISTANCE_KM = 500.0
IMPOSSIBLE_TRAVEL_MAX_SPEED_KMH = 900.0

USAGE_SPIKE_WINDOW_MS = 600_000
USAGE_SPIKE_THRESHOLD_MB = 1500.0


# Usage aggregation configuration

USAGE_WINDOW_MINUTES = 1
ALLOWED_LATENESS_SECONDS = 30
WATERMARK_OUT_OF_ORDER_SECONDS = 10


# ============================================================
# 2. OUTPUT TAGS
# ============================================================

INVALID_EVENTS_TAG = OutputTag(
    "invalid-events",
    Types.STRING(),
)

# ============================================================
# 3. VALIDATION HELPERS
# ============================================================

REQUIRED_FIELDS = {
    "schema_version": int,
    "event_id": str,
    "scenario_id": str,
    "event_type": str,
    "event_time": str,
    "ingested_at": str,
    "customer_id": str,
    "country_code": str,
    "device_id": str,
    "payload": dict,
}


def is_valid_timestamp(value: str) -> bool:
    """
    Check whether a timestamp is valid ISO-8601 and timezone-aware.

    We require a timezone because Flink needs an unambiguous
    point in time to process event-time windows.
    """
    if not isinstance(value, str):
        return False

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        return (
            parsed.tzinfo is not None
            and parsed.utcoffset() is not None
        )

    except (ValueError, TypeError):
        return False


def timestamp_to_epoch_ms(value: str) -> int:
    """
    Convert an ISO-8601 timestamp into Unix epoch milliseconds.
    """
    parsed = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    return int(parsed.timestamp() * 1000)


def epoch_ms_to_iso(timestamp_ms: int) -> str:
    """
    Convert Unix epoch milliseconds into UTC ISO-8601 format.
    """
    return (
        datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=timezone.utc,
        )
        .isoformat()
        .replace("+00:00", "Z")
    )


def build_fraud_alert(
    rule_name: str,
    severity: str,
    event: dict,
    window_start_ms: int,
    window_end_ms: int,
    contributing_event_ids: list[str],
    evidence: str,
    additional_fields: dict | None = None,
) -> str:
    """
    Build a consistent JSON alert for all fraud detectors.
    """

    alert = {
        "alert_id": (
            f"{rule_name}-"
            f"{event['customer_id']}-"
            f"{event['event_id']}"
        ),
        "rule_name": rule_name,
        "rule_version": 1,
        "severity": severity,
        "customer_id": event["customer_id"],
        "country_code": event["country_code"],
        "detected_at": epoch_ms_to_iso(window_end_ms),
        "window_start": epoch_ms_to_iso(window_start_ms),
        "window_end": epoch_ms_to_iso(window_end_ms),
        "contributing_event_ids": contributing_event_ids,
        "evidence": evidence,
    }

    if additional_fields:
        alert.update(additional_fields)

    return json.dumps(
        alert,
        separators=(",", ":"),
        ensure_ascii=False,
    )


# ============================================================
# 4. KAFKA EVENT PARSING AND VALIDATION
# ============================================================


class ParseAndValidateTelecomEvent(ProcessFunction):
    """
    Parse raw Kafka JSON and separate valid from invalid events.

    Both record types travel through the main stream using an envelope.
    They are split into separate streams downstream.
    """

    def process_element(self, value, ctx):

        try:
            event = json.loads(value)

        except (json.JSONDecodeError, TypeError) as exc:
            yield json.dumps(
                {
                    "record_type": "invalid",
                    "record": {
                        "raw_event": value,
                        "errors": [f"Invalid JSON: {exc}"],
                    },
                },
                separators=(",", ":"),
            )
            return

        errors = []

        if not isinstance(event, dict):
            errors.append("Event must be a JSON object")

        else:
            for field, expected_type in REQUIRED_FIELDS.items():

                if field not in event:
                    errors.append(
                        f"Missing required field: {field}"
                    )
                    continue

                field_value = event[field]

                # bool is a subclass of int in Python.
                # Explicitly reject it for schema_version.
                if field == "schema_version":
                    if (
                        not isinstance(field_value, int)
                        or isinstance(field_value, bool)
                    ):
                        errors.append(
                            "schema_version must be an integer"
                        )

                elif not isinstance(field_value, expected_type):
                    errors.append(
                        f"{field} must be "
                        f"{expected_type.__name__}"
                    )

            if event.get("schema_version") != 1:
                errors.append(
                    "Unsupported schema_version; expected 1"
                )

            if not is_valid_timestamp(event.get("event_time")):
                errors.append(
                    "event_time must be a valid "
                    "timezone-aware ISO-8601 timestamp"
                )

            if not is_valid_timestamp(event.get("ingested_at")):
                errors.append(
                    "ingested_at must be a valid "
                    "timezone-aware ISO-8601 timestamp"
                )

            country_code = event.get("country_code")

            if (
                isinstance(country_code, str)
                and len(country_code) != 2
            ):
                errors.append(
                    "country_code must contain exactly 2 characters"
                )

            if (
                isinstance(event.get("event_id"), str)
                and not event["event_id"].strip()
            ):
                errors.append("event_id cannot be empty")

            if (
                isinstance(event.get("customer_id"), str)
                and not event["customer_id"].strip()
            ):
                errors.append("customer_id cannot be empty")

        if errors:
            yield json.dumps(
                {
                    "record_type": "invalid",
                    "record": {
                        "raw_event": value,
                        "errors": errors,
                    },
                },
                separators=(",", ":"),
            )
            return

        yield json.dumps(
            {
                "record_type": "valid",
                "record": event,
            },
            separators=(",", ":"),
        )



# ============================================================
# 5. EVENT-TIME ASSIGNMENT
# ============================================================

class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):

        event = json.loads(value)

        return timestamp_to_epoch_ms(
            event["event_time"]
        )


# ============================================================
# 6. USAGE AGGREGATION
# ============================================================

class UsageCountryMinuteAggregator(KeyedProcessFunction):
    """
    Aggregate DATA_USAGE events by country and one-minute event-time bucket.

    This uses keyed state and event-time timers instead of Flink's window
    aggregate/process combination. That keeps the Python operator simple and
    avoids serializing a ProcessWindowFunction for this pipeline.

    A bucket is emitted after its allowed-lateness period has elapsed. Events
    arriving after that point are sent to the late-usage side output.
    """

    WINDOW_SIZE_MS = USAGE_WINDOW_MINUTES * 60_000
    ALLOWED_LATENESS_MS = ALLOWED_LATENESS_SECONDS * 1_000

    def open(self, runtime_context):
        self.windows_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "country-minute-usage-windows",
                Types.TUPLE(
                    [
                        Types.LONG(),   # window_start_ms
                        Types.DOUBLE(), # total_megabytes
                        Types.LONG(),   # event_count
                        Types.STRING(), # JSON array of event IDs
                    ]
                ),
            )
        )

    def process_element(self, value, ctx):
        event = json.loads(value)
        event_timestamp = ctx.timestamp()

        if event_timestamp is None:
            return

        payload = event.get("payload", {})
        megabytes = payload.get("megabytes")

        if (
            not isinstance(megabytes, (int, float))
            or isinstance(megabytes, bool)
            or not math.isfinite(float(megabytes))
            or megabytes <= 0
        ):
            return

        window_start = (event_timestamp // self.WINDOW_SIZE_MS) * self.WINDOW_SIZE_MS
        window_end = window_start + self.WINDOW_SIZE_MS
        cleanup_timestamp = (
            window_end - 1 + self.ALLOWED_LATENESS_MS
        )

        current_watermark = ctx.timer_service().current_watermark()
        if current_watermark > cleanup_timestamp:
            yield json.dumps(
                {
                    "record_type": "late_usage",
                    "record": {
                        "country_code": event["country_code"],
                        "event": event,
                        "reason": "Received after the allowed event-time lateness period",
                    },
                },
                separators=(",", ":"),
            )
            return        

        buckets = list(self.windows_state.get())
        event_id = event["event_id"]
        updated = False
        new_buckets = []

        for start_ms, total_mb, count, event_ids_json in buckets:
            if start_ms != window_start:
                new_buckets.append((start_ms, total_mb, count, event_ids_json))
                continue

            event_ids = json.loads(event_ids_json)
            if event_id not in event_ids:
                event_ids.append(event_id)
                total_mb += float(megabytes)
                count += 1

            new_buckets.append(
                (start_ms, total_mb, count, json.dumps(event_ids))
            )
            updated = True

        if not updated:
            new_buckets.append(
                (
                    window_start,
                    float(megabytes),
                    1,
                    json.dumps([event_id]),
                )
            )

        self.windows_state.update(new_buckets)
        ctx.timer_service().register_event_time_timer(cleanup_timestamp)

    def on_timer(self, timestamp, ctx):
        remaining_buckets = []

        for window_start, total_mb, event_count, event_ids_json in self.windows_state.get():
            window_end = window_start + self.WINDOW_SIZE_MS
            cleanup_timestamp = window_end - 1 + self.ALLOWED_LATENESS_MS

            if cleanup_timestamp <= timestamp:
                result = {
                    "country_code": ctx.get_current_key(),
                    "window_start": epoch_ms_to_iso(window_start),
                    "window_end": epoch_ms_to_iso(window_end),
                    "total_megabytes": round(total_mb, 2),
                    "event_count": event_count,
                    "contributing_event_ids": json.loads(event_ids_json),
                }
                yield json.dumps(
                    {
                        "record_type": "aggregate",
                        "record": result,
                    },
                    separators=(",", ":"),
                )

            else:
                remaining_buckets.append(
                    (window_start, total_mb, event_count, event_ids_json)
                )

        self.windows_state.update(remaining_buckets)


# ============================================================
# 7. FRAUD DETECTOR 1: TOP-UP VELOCITY
# ============================================================

class TopUpVelocityDetector(KeyedProcessFunction):
    """
    Detect five or more top-up attempts by one customer
    within a rolling 60-second event-time window.
    """

    def open(self, runtime_context):

        self.attempts_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "topup-attempts",
                Types.TUPLE(
                    [
                        Types.LONG(),
                        Types.STRING(),
                    ]
                ),
            )
        )

        self.alerted_state = runtime_context.get_state(
            ValueStateDescriptor(
                "topup-velocity-alerted",
                Types.BOOLEAN(),
            )
        )

    def process_element(self, value, ctx):

        event = json.loads(value)

        if event["event_type"] != "TOP_UP_ATTEMPT":
            return

        event_timestamp = ctx.timestamp()

        if event_timestamp is None:
            return

        cutoff = (
            event_timestamp
            - TOPUP_VELOCITY_WINDOW_MS
        )

        attempts = [
            (timestamp, event_id)
            for timestamp, event_id
            in self.attempts_state.get()
            if timestamp > cutoff
        ]

        known_event_ids = {
            event_id
            for _, event_id in attempts
        }

        # Do not count the same event ID twice
        # inside this customer's active window.
        if (
            event_timestamp > cutoff
            and event["event_id"] not in known_event_ids
        ):
            attempts.append(
                (
                    event_timestamp,
                    event["event_id"],
                )
            )

        attempts.sort(key=lambda item: item[0])

        self.attempts_state.update(attempts)

        ctx.timer_service().register_event_time_timer(
            event_timestamp
            + TOPUP_VELOCITY_WINDOW_MS
            + 1
        )

        if (
            len(attempts) >= TOPUP_VELOCITY_COUNT
            and not self.alerted_state.value()
        ):

            alert = build_fraud_alert(
                rule_name="TOP_UP_VELOCITY",
                severity="HIGH",
                event=event,
                window_start_ms=attempts[0][0],
                window_end_ms=event_timestamp,
                contributing_event_ids=[
                    event_id
                    for _, event_id in attempts
                ],
                evidence=(
                    f"{len(attempts)} top-up attempts "
                    f"within {TOPUP_VELOCITY_WINDOW_MS // 1000} seconds"
                ),
                additional_fields={
                    "attempt_count": len(attempts),
                },
            )

            self.alerted_state.update(True)

            yield alert

    def on_timer(self, timestamp, ctx):

        cutoff = (
            timestamp
            - TOPUP_VELOCITY_WINDOW_MS
        )

        attempts = [
            (event_timestamp, event_id)
            for event_timestamp, event_id
            in self.attempts_state.get()
            if event_timestamp > cutoff
        ]

        self.attempts_state.update(attempts)

        if len(attempts) < TOPUP_VELOCITY_COUNT:
            self.alerted_state.clear()

        if attempts:
            next_expiry = (
                min(timestamp for timestamp, _ in attempts)
                + TOPUP_VELOCITY_WINDOW_MS
                + 1
            )

            ctx.timer_service().register_event_time_timer(
                next_expiry
            )
        return []


# ============================================================
# 8. FRAUD DETECTOR 2: SIM-SWAP RISK
# ============================================================

class SimSwapRiskDetector(KeyedProcessFunction):
    """
    Detect this sequence for one customer:

    1. SIM_SWAP
    2. DEVICE_CHANGE
    3. High-value TOP_UP_ATTEMPT from the new device

    All three events must occur within ten minutes.
    """

    def open(self, runtime_context):

        self.sim_swap_timestamp = runtime_context.get_state(
            ValueStateDescriptor(
                "sim-swap-timestamp",
                Types.LONG(),
            )
        )

        self.sim_swap_event_id = runtime_context.get_state(
            ValueStateDescriptor(
                "sim-swap-event-id",
                Types.STRING(),
            )
        )

        self.new_device_id_state = runtime_context.get_state(
            ValueStateDescriptor(
                "sim-swap-new-device-id",
                Types.STRING(),
            )
        )

        self.device_change_timestamp = runtime_context.get_state(
            ValueStateDescriptor(
                "device-change-timestamp",
                Types.LONG(),
            )
        )

        self.device_change_event_id = runtime_context.get_state(
            ValueStateDescriptor(
                "device-change-event-id",
                Types.STRING(),
            )
        )

        self.alerted_state = runtime_context.get_state(
            ValueStateDescriptor(
                "sim-swap-alerted",
                Types.BOOLEAN(),
            )
        )

    def process_element(self, value, ctx):

        event = json.loads(value)

        event_type = event["event_type"]
        event_timestamp = ctx.timestamp()

        if event_timestamp is None:
            return

        # Step 1: Record a SIM swap.
        if event_type == "SIM_SWAP":

            self.sim_swap_timestamp.update(
                event_timestamp
            )

            self.sim_swap_event_id.update(
                event["event_id"]
            )

            self.device_change_timestamp.clear()
            self.device_change_event_id.clear()
            self.new_device_id_state.clear()
            self.alerted_state.clear()

            ctx.timer_service().register_event_time_timer(
                event_timestamp
                + SIM_SWAP_WINDOW_MS
                + 1
            )

            return

        sim_timestamp = self.sim_swap_timestamp.value()

        if sim_timestamp is None:
            return

        if not (
            sim_timestamp
            <= event_timestamp
            <= sim_timestamp + SIM_SWAP_WINDOW_MS
        ):
            return

        # Step 2: Record the device change.
        if event_type == "DEVICE_CHANGE":

            payload = event.get("payload", {})

            new_device_id = payload.get(
                "new_device_id",
                event.get("device_id"),
            )

            if not new_device_id:
                return

            self.device_change_timestamp.update(
                event_timestamp
            )

            self.device_change_event_id.update(
                event["event_id"]
            )

            self.new_device_id_state.update(
                new_device_id
            )

            return

        # Step 3: Check for a high-value top-up
        # from the new device.
        if event_type == "TOP_UP_ATTEMPT":

            device_timestamp = (
                self.device_change_timestamp.value()
            )

            expected_device = (
                self.new_device_id_state.value()
            )

            if (
                device_timestamp is None
                or expected_device is None
            ):
                return

            if event_timestamp < device_timestamp:
                return

            if event["device_id"] != expected_device:
                return

            payload = event.get("payload", {})
            amount = payload.get("amount")

            if (
                not isinstance(amount, (int, float))
                or isinstance(amount, bool)
                or amount < SIM_SWAP_HIGH_VALUE_EUR
            ):
                return

            if self.alerted_state.value():
                return

            alert = build_fraud_alert(
                rule_name="SIM_SWAP_RISK",
                severity="HIGH",
                event=event,
                window_start_ms=sim_timestamp,
                window_end_ms=event_timestamp,
                contributing_event_ids=[
                    self.sim_swap_event_id.value(),
                    self.device_change_event_id.value(),
                    event["event_id"],
                ],
                evidence=(
                    "SIM swap followed by device change "
                    "and a high-value top-up of "
                    f"EUR {amount:.2f}"
                ),
                additional_fields={
                    "top_up_amount": amount,
                    "currency": payload.get(
                        "currency",
                        "EUR",
                    ),
                    "new_device_id": expected_device,
                },
            )

            self.alerted_state.update(True)

            yield alert

    def on_timer(self, timestamp, ctx):

        sim_timestamp = self.sim_swap_timestamp.value()

        if (
            sim_timestamp is not None
            and timestamp
            >= sim_timestamp + SIM_SWAP_WINDOW_MS + 1
        ):

            self.sim_swap_timestamp.clear()
            self.sim_swap_event_id.clear()
            self.device_change_timestamp.clear()
            self.device_change_event_id.clear()
            self.new_device_id_state.clear()
            self.alerted_state.clear()
        return []


# ============================================================
# 9. FRAUD DETECTOR 3: IMPOSSIBLE TRAVEL
# ============================================================

# Approximate country-centre coordinates.
# These are demo coordinates, not precise device locations.

COUNTRY_COORDINATES = {
    "BE": (50.5039, 4.4699),
    "NL": (52.1326, 5.2913),
    "DE": (51.1657, 10.4515),
    "FR": (46.2276, 2.2137),
    "US": (39.8283, -98.5795),
    "PK": (30.3753, 69.3451),
}


def calculate_distance_km(country_a, country_b):
    """
    Calculate approximate distance between country centres
    using the Haversine formula.
    """

    if (
        country_a not in COUNTRY_COORDINATES
        or country_b not in COUNTRY_COORDINATES
    ):
        return None

    lat1, lon1 = COUNTRY_COORDINATES[country_a]
    lat2, lon2 = COUNTRY_COORDINATES[country_b]

    earth_radius_km = 6371.0

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(delta_lon / 2) ** 2
    )

    c = 2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a),
    )

    return earth_radius_km * c


class ImpossibleTravelDetector(KeyedProcessFunction):
    """
    Compare consecutive successful logins for one customer.

    Generate an alert when the implied travel speed exceeds
    the configured maximum.
    """

    def open(self, runtime_context):

        self.last_login_timestamp = runtime_context.get_state(
            ValueStateDescriptor(
                "last-login-timestamp",
                Types.LONG(),
            )
        )

        self.last_login_country = runtime_context.get_state(
            ValueStateDescriptor(
                "last-login-country",
                Types.STRING(),
            )
        )

        self.last_login_event_id = runtime_context.get_state(
            ValueStateDescriptor(
                "last-login-event-id",
                Types.STRING(),
            )
        )

    def process_element(self, value, ctx):

        event = json.loads(value)

        if event["event_type"] != "LOGIN":
            return

        payload = event.get("payload", {})

        if payload.get("success") is not True:
            return

        event_timestamp = ctx.timestamp()

        if event_timestamp is None:
            return

        current_country = event["country_code"]

        previous_timestamp = (
            self.last_login_timestamp.value()
        )

        previous_country = (
            self.last_login_country.value()
        )

        previous_event_id = (
            self.last_login_event_id.value()
        )

        if (
            previous_timestamp is not None
            and previous_country is not None
            and previous_event_id is not None
        ):

            elapsed_ms = (
                event_timestamp - previous_timestamp
            )

            distance = calculate_distance_km(
                previous_country,
                current_country,
            )

            if (
                distance is not None
                and 0 < elapsed_ms <= LOGIN_COMPARISON_WINDOW_MS
            ):

                elapsed_hours = elapsed_ms / 3_600_000

                speed_kmh = (
                    distance / elapsed_hours
                )

                if (
                    distance
                    >= IMPOSSIBLE_TRAVEL_MIN_DISTANCE_KM
                    and speed_kmh
                    > IMPOSSIBLE_TRAVEL_MAX_SPEED_KMH
                ):

                    alert = build_fraud_alert(
                        rule_name="IMPOSSIBLE_TRAVEL",
                        severity="CRITICAL",
                        event=event,
                        window_start_ms=previous_timestamp,
                        window_end_ms=event_timestamp,
                        contributing_event_ids=[
                            previous_event_id,
                            event["event_id"],
                        ],
                        evidence=(
                            f"Successful logins from "
                            f"{previous_country} and "
                            f"{current_country} imply "
                            f"travel at approximately "
                            f"{speed_kmh:.0f} km/h"
                        ),
                        additional_fields={
                            "previous_country": previous_country,
                            "current_country": current_country,
                            "distance_km": round(
                                distance,
                                2,
                            ),
                            "elapsed_minutes": round(
                                elapsed_ms / 60_000,
                                2,
                            ),
                            "implied_speed_kmh": round(
                                speed_kmh,
                                2,
                            ),
                        },
                    )

                    yield alert

        # Keep the current successful login as the
        # comparison point for the next successful login.
        self.last_login_timestamp.update(
            event_timestamp
        )

        self.last_login_country.update(
            current_country
        )

        self.last_login_event_id.update(
            event["event_id"]
        )


# ============================================================
# 10. FRAUD DETECTOR 4: USAGE SPIKE
# ============================================================

class UsageSpikeDetector(KeyedProcessFunction):
    """
    Detect a customer's data usage exceeding the configured
    threshold within a rolling ten-minute event-time window.
    """

    def open(self, runtime_context):

        self.usage_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "usage-spike-events",
                Types.TUPLE(
                    [
                        Types.LONG(),
                        Types.STRING(),
                        Types.DOUBLE(),
                        Types.STRING(),
                    ]
                ),
            )
        )

        self.alerted_state = runtime_context.get_state(
            ValueStateDescriptor(
                "usage-spike-alerted",
                Types.BOOLEAN(),
            )
        )

    def process_element(self, value, ctx):

        event = json.loads(value)

        if event["event_type"] != "DATA_USAGE":
            return

        event_timestamp = ctx.timestamp()

        if event_timestamp is None:
            return

        payload = event.get("payload", {})
        megabytes = payload.get("megabytes")

        if (
            not isinstance(megabytes, (int, float))
            or isinstance(megabytes, bool)
            or megabytes <= 0
        ):
            return

        cutoff = (
            event_timestamp
            - USAGE_SPIKE_WINDOW_MS
        )

        usage_events = [
            (
                timestamp,
                event_id,
                amount,
                country,
            )
            for timestamp, event_id, amount, country
            in self.usage_state.get()
            if timestamp > cutoff
        ]

        known_event_ids = {
            event_id
            for _, event_id, _, _ in usage_events
        }

        if event["event_id"] not in known_event_ids:

            usage_events.append(
                (
                    event_timestamp,
                    event["event_id"],
                    float(megabytes),
                    event["country_code"],
                )
            )

        usage_events.sort(
            key=lambda item: item[0]
        )

        self.usage_state.update(usage_events)

        ctx.timer_service().register_event_time_timer(
            event_timestamp
            + USAGE_SPIKE_WINDOW_MS
            + 1
        )

        total_mb = sum(
            amount
            for _, _, amount, _ in usage_events
        )

        if (
            total_mb >= USAGE_SPIKE_THRESHOLD_MB
            and not self.alerted_state.value()
        ):

            alert = build_fraud_alert(
                rule_name="USAGE_SPIKE",
                severity="MEDIUM",
                event=event,
                window_start_ms=usage_events[0][0],
                window_end_ms=event_timestamp,
                contributing_event_ids=[
                    event_id
                    for _, event_id, _, _ in usage_events
                ],
                evidence=(
                    f"Customer used {total_mb:.2f} MB "
                    f"within "
                    f"{USAGE_SPIKE_WINDOW_MS // 60_000} minutes"
                ),
                additional_fields={
                    "total_megabytes": round(
                        total_mb,
                        2,
                    ),
                    "threshold_megabytes": (
                        USAGE_SPIKE_THRESHOLD_MB
                    ),
                    "usage_event_count": len(
                        usage_events
                    ),
                },
            )

            self.alerted_state.update(True)

            yield alert

    def on_timer(self, timestamp, ctx):

        cutoff = (
            timestamp
            - USAGE_SPIKE_WINDOW_MS
        )

        usage_events = [
            (
                event_timestamp,
                event_id,
                amount,
                country,
            )
            for (
                event_timestamp,
                event_id,
                amount,
                country,
            ) in self.usage_state.get()
            if event_timestamp > cutoff
        ]

        self.usage_state.update(usage_events)

        total_mb = sum(
            amount
            for _, _, amount, _ in usage_events
        )

        if total_mb < USAGE_SPIKE_THRESHOLD_MB:
            self.alerted_state.clear()

        if usage_events:

            next_expiry = (
                min(
                    event_timestamp
                    for (
                        event_timestamp,
                        _,
                        _,
                        _,
                    ) in usage_events
                )
                + USAGE_SPIKE_WINDOW_MS
                + 1
            )

            ctx.timer_service().register_event_time_timer(
                next_expiry
            )
        return []


# ============================================================
# 11. KAFKA SINK HELPER
# ============================================================

def create_kafka_sink(topic_name: str):

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP_SERVERS
        )
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(
                SimpleStringSchema()
            )
            .build()
        )
        .set_delivery_guarantee(
            DeliveryGuarantee.AT_LEAST_ONCE
        )
        .build()
    )


# ============================================================
# 12. MAIN FLINK PIPELINE
# ============================================================

def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    # --------------------------------------------------------
    # A. Read raw events from Kafka
    # --------------------------------------------------------

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP_SERVERS
        )
        .set_topics(RAW_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka raw event source",
    )

    # --------------------------------------------------------
    # B. Parse and validate events
    # --------------------------------------------------------

    parsed_stream = raw_stream.process(
        ParseAndValidateTelecomEvent(),
        output_type=Types.STRING(),
    )

    # Extract valid events and restore their original JSON format.
    valid_stream = (
        parsed_stream
        .filter(
            lambda record: json.loads(record)["record_type"] == "valid"
        )
        .map(
            lambda record: json.dumps(
                json.loads(record)["record"],
                separators=(",", ":"),
            ),
            output_type=Types.STRING(),
        )
    )

    # Extract invalid events for the dead-letter queue.
    invalid_stream = (
        parsed_stream
        .filter(
            lambda record: json.loads(record)["record_type"] == "invalid"
        )
        .map(
            lambda record: json.dumps(
                json.loads(record)["record"],
                separators=(",", ":"),
            ),
            output_type=Types.STRING(),
        )
    )

    # --------------------------------------------------------
    # C. Assign event-time timestamps and watermarks
    # --------------------------------------------------------

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(
                WATERMARK_OUT_OF_ORDER_SECONDS
            )
        )
        .with_timestamp_assigner(
            EventTimestampAssigner()
        )
    )

    timed_valid_stream = (
        valid_stream.assign_timestamps_and_watermarks(
            watermark_strategy
        )
    )

    # --------------------------------------------------------
    # D. Publish valid events
    # --------------------------------------------------------

    timed_valid_stream.sink_to(
        create_kafka_sink(VALID_TOPIC)
    )

    # --------------------------------------------------------
    # E. Top-up velocity fraud detection
    # --------------------------------------------------------

    topup_stream = (
        timed_valid_stream
        .key_by(
            lambda value: json.loads(value)["customer_id"]
        )
        .process(
            TopUpVelocityDetector(),
            output_type=Types.STRING(),
        )
    )

    # --------------------------------------------------------
    # F. SIM-swap risk fraud detection
    # --------------------------------------------------------

    sim_swap_stream = (
        timed_valid_stream
        .key_by(
            lambda value: json.loads(value)["customer_id"]
        )
        .process(
            SimSwapRiskDetector(),
            output_type=Types.STRING(),
        )
    )

    # --------------------------------------------------------
    # G. Impossible travel fraud detection
    # --------------------------------------------------------

    impossible_travel_stream = (
        timed_valid_stream
        .key_by(
            lambda value: json.loads(value)["customer_id"]
        )
        .process(
            ImpossibleTravelDetector(),
            output_type=Types.STRING(),
        )
    )

    # --------------------------------------------------------
    # H. Usage spike fraud detection
    # --------------------------------------------------------

    usage_spike_stream = (
        timed_valid_stream
        .key_by(
            lambda value: json.loads(value)["customer_id"]
        )
        .process(
            UsageSpikeDetector(),
            output_type=Types.STRING(),
        )
    )

    # --------------------------------------------------------
    # I. Country-level one-minute usage aggregation
    #
    # The keyed process function groups by country, stores minute buckets,
    # and emits each bucket after the allowed-lateness period.

    usage_output_stream = (
        timed_valid_stream
        .filter(
            lambda value: json.loads(value)["event_type"] == "DATA_USAGE"
        )
        .key_by(
            lambda value: json.loads(value)["country_code"]
        )
        .process(
            UsageCountryMinuteAggregator(),
            output_type=Types.STRING(),
        )
    )

    # Separate normal aggregates from late events.
    usage_aggregates_stream = (
        usage_output_stream
        .filter(
            lambda value: json.loads(value)["record_type"] == "aggregate"
        )
        .map(
            lambda value: json.dumps(
                json.loads(value)["record"],
                separators=(",", ":"),
            ),
            output_type=Types.STRING(),
        )
    )

    late_usage_stream = (
        usage_output_stream
        .filter(
            lambda value: json.loads(value)["record_type"] == "late_usage"
        )
        .map(
            lambda value: json.dumps(
                json.loads(value)["record"],
                separators=(",", ":"),
            ),
            output_type=Types.STRING(),
        )
    )

    # J. Publish usage aggregates and late events
    # --------------------------------------------------------

    usage_aggregates_stream.sink_to(
        create_kafka_sink(USAGE_AGGREGATES_TOPIC)
    )

    late_usage_stream.sink_to(
        create_kafka_sink(LATE_USAGE_TOPIC)
    ) 

    # --------------------------------------------------------
    # K. Publish all four fraud alert streams
    # --------------------------------------------------------

    topup_stream.sink_to(
        create_kafka_sink(
            FRAUD_ALERTS_TOPIC
        )
    )

    sim_swap_stream.sink_to(
        create_kafka_sink(
            FRAUD_ALERTS_TOPIC
        )
    )

    impossible_travel_stream.sink_to(
        create_kafka_sink(
            FRAUD_ALERTS_TOPIC
        )
    )

    usage_spike_stream.sink_to(
        create_kafka_sink(
            FRAUD_ALERTS_TOPIC
        )
    )

    # --------------------------------------------------------
    # L. Publish invalid events to the dead-letter topic
    # --------------------------------------------------------

    invalid_stream.sink_to(
        create_kafka_sink(DLQ_TOPIC)
    )

    # --------------------------------------------------------
    # M. Start the Flink job
    # --------------------------------------------------------

    env.execute(
        "Telecom Event Validation Usage and Fraud Detection"
    )


if __name__ == "__main__":
    main()
