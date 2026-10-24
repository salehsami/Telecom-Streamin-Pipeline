from pyflink.common import SimpleStringSchema
from pyflink.common import WatermarkStrategy

from pyflink.datastream import (
    StreamExecutionEnvironment,
)

from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"

RAW_TOPIC = "telecom.raw.events.v1"

CONSUMER_GROUP = "telecom-flink-raw-reader-v1"


def build_kafka_source():

    return (
        KafkaSource.builder()

        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP_SERVERS
        )

        .set_topics(
            RAW_TOPIC
        )

        .set_group_id(
            CONSUMER_GROUP
        )

        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )

        .set_value_only_deserializer(
            SimpleStringSchema()
        )

        .build()
    )


def main():

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    env.set_parallelism(1)

    kafka_source = build_kafka_source()

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Raw Telecom Source",
    )

    raw_stream.print()

    env.execute(
        "Telecom Raw Event Reader"
    )


if __name__ == "__main__":
    main()