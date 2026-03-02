import json
import gzip
import msgpack
import argparse
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError

kafka_config = {"auto.offset.reset": "beginning", "group.id": "flink"}


def recurse_and_print_data(data):
    if isinstance(data, dict):
        print("Dict")
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                print(f"{key}:")
                recurse_and_print_data(value)
            else:
                print(f"{key}: {value}")
    elif isinstance(data, list):
        print("List")
        for item in data:
            if isinstance(item, (list, dict)):
                recurse_and_print_data(item)
            else:
                print(f"- {item}")
    else:
        raise TypeError("Unsupported data type for printing: {}".format(type(data)))


def deserialize_message(message):
    message = json.loads(message)
    # convert "2025-05-26T20:42:30" to datetime object
    message["start_timestamp"] = datetime.strptime(
        message["window"]["start"] + "Z", "%Y-%m-%dT%H:%M:%S%z"
    ).timestamp()
    message["end_timestamp"] = datetime.strptime(
        message["window"]["end"] + "Z", "%Y-%m-%dT%H:%M:%S%z"
    ).timestamp()

    del message["window"]
    message["precompute"] = bytes.fromhex(message["precompute"])
    message["precompute"] = gzip.decompress(message["precompute"])
    message["aggregation"] = msgpack.unpackb(
        message["aggregation"], raw=False, strict_map_key=False
    )
    return message


def main(args):
    kafka_config["bootstrap.servers"] = args.kafka_broker

    consumer = None

    try:
        consumer = Consumer(kafka_config)
        consumer.subscribe([args.kafka_topic])

        with open(args.output_file, "w") as f:
            while True:
                try:
                    messages = consumer.consume(num_messages=1000, timeout=1.0)

                    if not messages:  # No messages received
                        continue

                    for msg in messages:
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            else:
                                print(f"Consumer error: {msg.error()}")
                                continue

                        decoded_message = msg.value().decode("utf-8")
                        f.write(decoded_message + "\n")
                        try:
                            if args.print_messages:
                                print(decoded_message)
                                deserialized_message = deserialize_message(
                                    decoded_message
                                )
                                recurse_and_print_data(deserialized_message)
                        except Exception as e:
                            print(f"Error deserializing message: {e}")
                            continue

                    f.flush()

                except KafkaException as e:
                    print(f"Kafka error: {e}")
                    break

                except Exception as e:
                    print(f"Error processing messages: {e}")
                    break

    except Exception as e:
        print(f"Fatal error: {e}")

    finally:
        print("Shutting down consumer...")
        try:
            if consumer:
                consumer.close()
            print("Consumer closed.")
        except Exception as e:
            print(f"Error closing consumer: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dumb Kafka Consumer")

    parser.add_argument(
        "--kafka_broker", type=str, required=False, default="localhost:9092"
    )
    parser.add_argument(
        "--kafka_topic", type=str, required=True, help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--output_file", type=str, required=True, help="File to store consumed messages"
    )
    parser.add_argument(
        "--print_messages",
        action="store_true",
        default=False,
        help="Print messages to stdout",
    )

    args = parser.parse_args()
    main(args)
