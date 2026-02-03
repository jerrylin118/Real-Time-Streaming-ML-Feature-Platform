"""Create Kafka topics on Redpanda. Run once after Redpanda is up."""

import os
import sys
import time

try:
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("confluent-kafka not installed")
    sys.exit(1)


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    client = AdminClient({"bootstrap.servers": bootstrap})

    topics = [
        NewTopic("events.raw", num_partitions=4, replication_factor=1),
        NewTopic("features.online", num_partitions=4, replication_factor=1),
    ]

    for attempt in range(10):
        try:
            fs = client.create_topics(topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Created topic {topic}")
                except Exception as e:
                    if "already exists" in str(e).lower() or "topic_already_exists" in str(e).lower():
                        print(f"Topic {topic} already exists")
                    else:
                        raise
            return 0
        except Exception as e:
            print(f"Attempt {attempt + 1}/10 failed: {e}")
            time.sleep(3)

    print("Failed to create topics")
    return 1


if __name__ == "__main__":
    sys.exit(main())
