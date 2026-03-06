import argparse
import json
import time
import pytz
import random
import itertools
from datetime import datetime
from confluent_kafka import Producer, admin

random.seed(42)

LABEL_CHOICES = {
    "hostname": ["host1", "host2", "host3"],
    "location": ["us-east", "us-west", "eu-central"],
    "application_name": ["app1", "app2", "app3"],
}
METRIC_NAME = "cpu_usage"


def create_topic_if_not_exists(producer, topic):
    admin_client = admin.AdminClient(
        {"bootstrap.servers": producer.list_topics().brokers}
    )
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic not in topic_metadata.topics:
        new_topic = admin.NewTopic(topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic}' created.")


def generate_data(labels):
    label_names = list(LABEL_CHOICES.keys())
    labels = {label_names[i]: labels[i] for i in range(len(label_names))}

    metric_name = METRIC_NAME

    local_datetime = datetime.now()
    utc_datetime = local_datetime.astimezone(pytz.utc)

    data = {
        "labels": labels,
        "value": random.uniform(0, 100),
        "name": metric_name,
        "timestamp": utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{utc_datetime.microsecond // 1000:03d}Z",
    }
    return json.dumps(data).encode("utf-8")


def main(args):
    label_names = list(LABEL_CHOICES.keys())
    total_combinations = 1
    for label_name in label_names:
        total_combinations *= len(LABEL_CHOICES[label_name])

    if args.data_points > total_combinations:
        raise ValueError(
            "data_points cannot be greater than the number of possible combinations for labels"
        )

    producer = Producer({"bootstrap.servers": args.kafka_broker})
    create_topic_if_not_exists(producer, args.kafka_topic)

    # Generate all possible label combinations dynamically
    label_values = [LABEL_CHOICES[label_name] for label_name in label_names]
    all_labels = list(itertools.product(*label_values))

    num_labels = args.data_points

    while True:
        for idx in range(num_labels):
            data = generate_data(labels=all_labels[idx])
            producer.produce(args.kafka_topic, value=data)
            producer.flush()

            if args.debug_print:
                print(data)

        if args.debug_print:
            print("-" * 50)
        time.sleep(args.frequency)

        if args.vary_labels:
            num_labels += random.randint(-1, 1)
            if num_labels < 1:
                num_labels = 1
            if num_labels > total_combinations:
                num_labels = total_combinations


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka_broker", type=str, default="localhost:9092")
    parser.add_argument(
        "--debug_print",
        action="store_true",
        help="Print data to console instead of sending to Kafka",
    )
    parser.add_argument("--kafka_topic", type=str, required=True)
    parser.add_argument(
        "--frequency",
        type=int,
        default=1,
        help="Frequency in seconds to dump data to Kafka",
    )
    parser.add_argument(
        "--data_points",
        type=int,
        required=True,
        help="Number of data points to dump at each frequency interval",
    )
    parser.add_argument(
        "--vary_labels",
        action="store_true",
        help="Vary the number of labels to dump data for",
    )
    args = parser.parse_args()
    main(args)
