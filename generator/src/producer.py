import argparse
import json
import time

from confluent_kafka import Producer

from generator import TelecomEventGenerator
from models import TelecomEvent


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.raw.events.v1"


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "telecom-event-generator",
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
        return

    print(
        f"Delivered event to {msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def produce_event(event: TelecomEvent):
    producer.produce(
        topic=TOPIC,
        key=event.customer_id,
        value=event.model_dump_json(),
        callback=delivery_report,
    )

    producer.poll(0)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Synthetic telecom event producer"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random generator seed",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=1,
        help="Events generated per second",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of events to generate. 0 means unlimited.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    generator = TelecomEventGenerator(seed=args.seed)

    sleep_interval = 1 / args.rate

    produced = 0

    print("Starting telecom traffic generator")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC}")
    print(f"Seed: {args.seed}")
    print(f"Rate: {args.rate} events/sec")

    try:
        while args.count == 0 or produced < args.count:

            event = generator.normal_event()

            print(
                json.dumps(
                    event.model_dump(mode="json"),
                    indent=2,
                )
            )

            produce_event(event)

            produced += 1

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        print("Flushing outstanding Kafka messages...")
        producer.flush()

        print(
            f"Producer stopped. "
            f"Generated {produced} events."
        )


if __name__ == "__main__":
    main()