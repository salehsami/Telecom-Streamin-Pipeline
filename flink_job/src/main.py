
import json
from datetime import datetime, timezone
from pyflink.common import Time
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream.output_tag import OutputTag
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
RAW_TOPIC = "telecom.raw.events.v1"
CONSUMER_GROUP = "telecom-flink-raw-reader-v1"
VALID_TOPIC = "telecom.valid.events.v1"
DLQ_TOPIC = "telecom.dlq.events.v1"
USAGE_AGGREGATES_TOPIC = "telecom.usage.country.minute.v1"
LATE_USAGE_TOPIC = "telecom.usage.late-events.v1"

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

class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)

        event_time = event["event_time"]

        parsed_time = datetime.fromisoformat(
            event_time.replace("Z", "+00:00")
        )

        # Flink expects timestamps in milliseconds.
        return int(parsed_time.timestamp() * 1000)
    
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

class UsageAggregate(AggregateFunction):

    def create_accumulator(self):
        # (total_megabytes, event_count)
        return (0.0, 0)

    def add(self, value, accumulator):
        # value = (country_code, megabytes)
        total = accumulator[0] + value[1]
        count = accumulator[1] + 1

        return (total, count)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return (
            a[0] + b[0],
            a[1] + b[1],
        )

class UsageWindowResult(ProcessWindowFunction):

    def process(self, key, context, elements):

        total_megabytes, event_count = next(iter(elements))

        window = context.window()

        window_start = datetime.fromtimestamp(
            window.start / 1000,
            tz=timezone.utc,
        ).isoformat().replace("+00:00", "Z")

        window_end = datetime.fromtimestamp(
            window.end / 1000,
            tz=timezone.utc,
        ).isoformat().replace("+00:00", "Z")

        result = {
            "country_code": key,
            "window_start": window_start,
            "window_end": window_end,
            "total_megabytes": round(total_megabytes, 2),
            "event_count": event_count,
        }

        yield json.dumps(
            result,
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
    
    
    # Generate event-time watermarks.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    # Assign event timestamps and watermarks to valid events.
    event_time_stream = valid_stream.assign_timestamps_and_watermarks(
        watermark_strategy
    ).name("Assign Event Time and Watermarks")
    
    data_usage_stream = (
    event_time_stream
    .filter(
        lambda record: json.loads(record).get("event_type") == "DATA_USAGE"
    )
    .map(
        lambda record: (
            json.loads(record)["country_code"],
            float(json.loads(record)["payload"]["megabytes"]),
            record,
        ),
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.DOUBLE(),
            Types.STRING(),
        ]),
    )
)

    late_usage_tag = OutputTag(
        "late-usage-events",
        Types.TUPLE([
            Types.STRING(),
            Types.DOUBLE(),
            Types.STRING(),
        ]),
    )

    windowed_usage_stream = (
        data_usage_stream
        .key_by(lambda usage: usage[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(30_000)
        .side_output_late_data(late_usage_tag)
    )

    usage_aggregation_stream = windowed_usage_stream.aggregate(
        UsageAggregate(),
        UsageWindowResult(),
        accumulator_type=Types.TUPLE([
            Types.DOUBLE(),
            Types.LONG(),
        ]),
        output_type=Types.STRING(),
    )

    late_usage_stream = (
        usage_aggregation_stream
        .get_side_output(late_usage_tag)
        .map(
            lambda late_event: late_event[2],
            output_type=Types.STRING(),
        )
    )


    # Branch 2: Keep invalid records with their error details.
    invalid_stream = classified_stream.filter(
        lambda record: json.loads(record)["status"] == "INVALID"
    )
    
    # Write the valid events to a Kafka topic.
    event_time_stream.sink_to(
        build_kafka_sink(VALID_TOPIC)
    ).name("Write Valid Events to Kafka")
    
    # Write the usage aggregates to a Kafka topic.
    usage_aggregation_stream.sink_to(
    build_kafka_sink(USAGE_AGGREGATES_TOPIC)
    ).name("Write Country Minute Usage Aggregates to Kafka")    

    late_usage_stream.sink_to(
    build_kafka_sink(LATE_USAGE_TOPIC)
    ).name("Write Late Usage Events to Kafka")  
    


    # Write the invalid events to a Kafka topic.
    invalid_stream.sink_to(
        build_kafka_sink(DLQ_TOPIC)
    ).name("Write Invalid Events to Dead Letter Topic")

    env.execute("Telecom Event Parsing and Validation")


if __name__ == "__main__":
    main()
