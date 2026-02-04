"""Producer service: generates realistic clickstream events."""

import json
import os
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer


# Non-uniform distributions for meaningful features
USER_IDS = [f"u{i}" for i in range(100)]
ITEM_IDS = [f"i{i}" for i in range(50)]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "UK", "DE", "FR", "JP", "IN", "BR", "CA"]

# Weighted: some users/items more active (improves feature signal)
USER_WEIGHTS = [10 if i < 20 else 5 if i < 50 else 1 for i in range(100)]
ITEM_WEIGHTS = [15 if i < 10 else 5 if i < 25 else 1 for i in range(50)]
# Click probability after impression (EVENTS_PER_SEC controls impression rate)
CLICK_PROBABILITY = 0.08


def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def create_event(event_type: str, ts: int, user_id: str, item_id: str,
                 session_id: str, device: str, country: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "ts": ts,
        "user_id": user_id,
        "item_id": item_id,
        "session_id": session_id,
        "device": device,
        "country": country,
    }


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    elif msg:
        pass  # Success


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("EVENTS_RAW_TOPIC", "events.raw")
    events_per_sec = float(os.environ.get("EVENTS_PER_SEC", "5"))

    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": "producer",
    }
    producer = Producer(conf)

    interval = 1.0 / events_per_sec if events_per_sec > 0 else 1.0
    session_counter = 0

    while True:
        ts = int(datetime.utcnow().timestamp() * 1000)
        user_id = weighted_choice(USER_IDS, USER_WEIGHTS)
        item_id = weighted_choice(ITEM_IDS, ITEM_WEIGHTS)
        session_id = f"s{session_counter % 500}"
        device = random.choice(DEVICES)
        country = weighted_choice(COUNTRIES, [5, 4, 3, 3, 2, 2, 1, 1])

        # Impression first
        impression = create_event("impression", ts, user_id, item_id, session_id, device, country)
        producer.produce(
            topic,
            key=user_id.encode("utf-8"),
            value=json.dumps(impression).encode("utf-8"),
            callback=delivery_callback,
        )

        # Probabilistically emit click shortly after (CTR ~5-10%)
        if random.random() < CLICK_PROBABILITY:
            time.sleep(random.uniform(0.1, 1.0))
            click_ts = int(datetime.utcnow().timestamp() * 1000)
            click = create_event("click", click_ts, user_id, item_id, session_id, device, country)
            producer.produce(
                topic,
                key=user_id.encode("utf-8"),
                value=json.dumps(click).encode("utf-8"),
                callback=delivery_callback,
            )

        producer.flush()
        session_counter += 1
        time.sleep(interval)


if __name__ == "__main__":
    main()
