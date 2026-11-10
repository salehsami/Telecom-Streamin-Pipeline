
import json
from datetime import datetime

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaSource,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
RAW_TOPIC = "telecom.raw.events.v1"
CONSUMER_GROUP = "telecom-flink-raw-reader-v1"
VALID_TOPIC = "telecom.valid.events.v1"
DLQ_TOPIC = "telecom.dlq.events.v1"

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
    """Check that a timestamp is ISO-formatted and includes a timezone."""
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed.tzinfo is not None
    except ValueError:
        return False


class ParseAndValidateTelecomEvent(ProcessFunction):

    def process_element(self, value, ctx):
        # Step 1: Parse the incoming JSON message.
        try:
            event = json.loads(value)
        except json.JSONDecodeError as exc:
            yield json.dumps(
                {
                    "status": "INVALID",
                    "raw_event": value,
                    "errors": [f"Invalid JSON: {exc.msg}"],
                },
                separators=(",", ":"),
                ensure_ascii=False,
            )
            return

        errors = []

        # Step 2: Check that the JSON represents an object.
        if not isinstance(event, dict):
            errors.append("Top-level JSON value must be an object")

        else:
            # Step 3: Check required fields and their data types.
            for field, expected_type in REQUIRED_FIELDS.items():
                if field not in event:
                    errors.append(f"Missing required field: {field}")
                    continue

                actual_value = event[field]

                if expected_type is int:
                    # Prevent True/False from being accepted as integers.
                    correct_type = type(actual_value) is int
                else:
                    correct_type = isinstance(
                        actual_value, expected_type
                    )

                if not correct_type:
                    errors.append(
                        f"{field} must be {expected_type.__name__}"
                    )
                    continue

                if (
                    isinstance(actual_value, str)
                    and not actual_value.strip()
                ):
                    errors.append(f"{field} must not be empty")

            # Step 4: Check the supported schema version.
            schema_version = event.get("schema_version")
            if type(schema_version) is int and schema_version != 1:
                errors.append(
                    f"Unsupported schema_version: {schema_version}"
                )

            # Step 5: Validate timestamps and timezone information.
            for field in ("event_time", "ingested_at"):
                timestamp = event.get(field)

                if isinstance(timestamp, str) and timestamp.strip():
                    if not is_valid_timestamp(timestamp):
                        errors.append(
                            f"{field} must be an ISO timestamp "
                            "with a timezone"
                        )

        # Step 6: Emit a classified invalid record.
        if errors:
            yield json.dumps(
                {
                    "status": "INVALID",
                    "raw_event": value,
                    "errors": errors,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            )
            return

        # Step 7: Emit a classified valid record.
        yield json.dumps(
            {
                "status": "VALID",
                "event": event,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )


def build_kafka_source():
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(RAW_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_sink(topic: str):
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = build_kafka_source()

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Raw Telecom Source",
    )

    # Classify every incoming message as VALID or INVALID.
    classified_stream = raw_stream.process(
        ParseAndValidateTelecomEvent(),
        output_type=Types.STRING(),
    )

    # Branch 1: Keep valid events and remove the classification wrapper.
    valid_stream = (
        classified_stream
        .filter(lambda record: json.loads(record)["status"] == "VALID")
        .map(
            lambda record: json.dumps(
                json.loads(record)["event"],
                separators=(",", ":"),
                ensure_ascii=False,
            ),
            output_type=Types.STRING(),
        )
    )

    # Branch 2: Keep invalid records with their error details.
    invalid_stream = classified_stream.filter(
        lambda record: json.loads(record)["status"] == "INVALID"
    )

    valid_stream.sink_to(
        build_kafka_sink(VALID_TOPIC)
    ).name("Write Valid Events to Kafka")

    invalid_stream.sink_to(
        build_kafka_sink(DLQ_TOPIC)
    ).name("Write Invalid Events to Dead Letter Topic")

    env.execute("Telecom Event Parsing and Validation")


if __name__ == "__main__":
    main()
