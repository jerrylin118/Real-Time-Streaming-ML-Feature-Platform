"""Stream processor: consume events, compute features, store in Redis/Kafka/Postgres."""

import json
import os
import time
from datetime import datetime

from confluent_kafka import Consumer, Producer
import redis
import psycopg
from prometheus_client import Counter, Histogram, start_http_server

from schemas.event import Event
from feature_defs import compute_features

# Prometheus metrics
PROCESSED_EVENTS = Counter("processed_events_total", "Total events processed")
DEDUP_SKIPS = Counter("dedup_skips_total", "Events skipped due to deduplication")
FEATURE_COMPUTE_LATENCY = Histogram("feature_compute_latency_seconds", "Feature computation latency")


DEDUP_TTL = 3600
FEATURE_TTL = 3600
WINDOWS = {
    "1m": 60,
    "5m": 300,
    "10m": 600,
    "1h": 3600,
}


def get_redis():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url)


def get_postgres_conn():
    return psycopg.connect(
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('POSTGRES_USER', 'mlplatform')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'mlplatform')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'mlplatform')}"
    )


def ensure_postgres_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR(64) PRIMARY KEY,
                event_type VARCHAR(32) NOT NULL,
                ts BIGINT NOT NULL,
                user_id VARCHAR(128) NOT NULL,
                item_id VARCHAR(128) NOT NULL,
                session_id VARCHAR(128) NOT NULL,
                device VARCHAR(64) NOT NULL,
                country VARCHAR(16) NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()


def persist_event(conn, event: Event):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (event_id, event_type, ts, user_id, item_id, session_id, device, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event.event_id,
                event.event_type,
                event.ts,
                event.user_id,
                event.item_id,
                event.session_id,
                event.device,
                event.country,
            ),
        )
    conn.commit()


def deduplicate(r: redis.Redis, event_id: str) -> bool:
    """Return True if event is new (not duplicate)."""
    key = f"dedup:{event_id}"
    if r.set(key, "1", nx=True, ex=DEDUP_TTL):
        return True
    return False


def prune_sorted_set(r: redis.Redis, key: str, window_sec: int, now_ts: int):
    cutoff = now_ts - window_sec * 1000
    r.zremrangebyscore(key, "-inf", cutoff)


def count_in_window(r: redis.Redis, key: str, window_sec: int, now_ts: int) -> int:
    prune_sorted_set(r, key, window_sec, now_ts)
    cutoff = now_ts - window_sec * 1000
    return r.zcard(key)  # After prune, all remaining are in window


def add_to_sorted_set(r: redis.Redis, key: str, member: str, score: float, window_sec: int, now_ts: int):
    r.zadd(key, {member: score})
    prune_sorted_set(r, key, window_sec, now_ts)


def get_time_since_last_click(r: redis.Redis, user_id: str, now_ts: int) -> float:
    key = f"user:{user_id}:last_click"
    val = r.get(key)
    if val is None:
        return 86400.0  # 24 hours default
    last_ts = int(val)
    return (now_ts - last_ts) / 1000.0


def set_last_click(r: redis.Redis, user_id: str, ts: int):
    key = f"user:{user_id}:last_click"
    r.set(key, str(ts), ex=86400 * 2)  # 2 days


def fetch_counts_and_update(r: redis.Redis, event: Event) -> dict:
    now_ts = event.ts
    user_id = event.user_id
    item_id = event.item_id
    session_id = event.session_id

    # User impressions/clicks - separate keys per window
    user_imp_1m_key = f"user:{user_id}:impressions:1m"
    user_clk_1m_key = f"user:{user_id}:clicks:1m"
    user_imp_10m_key = f"user:{user_id}:impressions:10m"
    user_clk_10m_key = f"user:{user_id}:clicks:10m"

    member = f"{event.event_id}:{now_ts}"
    if event.event_type == "impression":
        add_to_sorted_set(r, user_imp_1m_key, member, now_ts, WINDOWS["1m"], now_ts)
        add_to_sorted_set(r, user_imp_10m_key, member, now_ts, WINDOWS["10m"], now_ts)
    else:
        add_to_sorted_set(r, user_clk_1m_key, member, now_ts, WINDOWS["1m"], now_ts)
        add_to_sorted_set(r, user_clk_10m_key, member, now_ts, WINDOWS["10m"], now_ts)
        set_last_click(r, user_id, now_ts)

    user_impressions_1m = count_in_window(r, user_imp_1m_key, WINDOWS["1m"], now_ts)
    user_clicks_1m = count_in_window(r, user_clk_1m_key, WINDOWS["1m"], now_ts)
    user_impressions_10m = count_in_window(r, user_imp_10m_key, WINDOWS["10m"], now_ts)
    user_clicks_10m = count_in_window(r, user_clk_10m_key, WINDOWS["10m"], now_ts)

    # Item impressions/clicks - separate keys per window
    item_imp_10m_key = f"item:{item_id}:impressions:10m"
    item_clk_10m_key = f"item:{item_id}:clicks:10m"
    item_imp_1h_key = f"item:{item_id}:impressions:1h"
    item_clk_1h_key = f"item:{item_id}:clicks:1h"

    if event.event_type == "impression":
        add_to_sorted_set(r, item_imp_10m_key, member, now_ts, WINDOWS["10m"], now_ts)
        add_to_sorted_set(r, item_imp_1h_key, member, now_ts, WINDOWS["1h"], now_ts)
    else:
        add_to_sorted_set(r, item_clk_10m_key, member, now_ts, WINDOWS["10m"], now_ts)
        add_to_sorted_set(r, item_clk_1h_key, member, now_ts, WINDOWS["1h"], now_ts)

    item_impressions_10m = count_in_window(r, item_imp_10m_key, WINDOWS["10m"], now_ts)
    item_clicks_10m = count_in_window(r, item_clk_10m_key, WINDOWS["10m"], now_ts)
    item_impressions_1h = count_in_window(r, item_imp_1h_key, WINDOWS["1h"], now_ts)
    item_clicks_1h = count_in_window(r, item_clk_1h_key, WINDOWS["1h"], now_ts)

    # Session events (5m)
    session_key = f"session:{session_id}:events"
    add_to_sorted_set(r, session_key, member, now_ts, WINDOWS["5m"], now_ts)
    session_events_5m = count_in_window(r, session_key, WINDOWS["5m"], now_ts)

    time_since_last_click_user = get_time_since_last_click(r, user_id, now_ts)

    return {
        "user_impressions_1m": user_impressions_1m,
        "user_clicks_1m": user_clicks_1m,
        "user_impressions_10m": user_impressions_10m,
        "user_clicks_10m": user_clicks_10m,
        "item_impressions_10m": item_impressions_10m,
        "item_clicks_10m": item_clicks_10m,
        "item_impressions_1h": item_impressions_1h,
        "item_clicks_1h": item_clicks_1h,
        "session_events_5m": session_events_5m,
        "time_since_last_click_user_sec": time_since_last_click_user,
    }


def process_event(r: redis.Redis, pg_conn, producer: Producer, event: Event):
    start = time.perf_counter()
    counts = fetch_counts_and_update(r, event)
    feats = compute_features(
        event.ts,
        counts["user_impressions_1m"],
        counts["user_clicks_1m"],
        counts["user_impressions_10m"],
        counts["user_clicks_10m"],
        counts["item_impressions_10m"],
        counts["item_clicks_10m"],
        counts["item_impressions_1h"],
        counts["item_clicks_1h"],
        counts["time_since_last_click_user_sec"],
        counts["session_events_5m"],
    )
    FEATURE_COMPUTE_LATENCY.observe(time.perf_counter() - start)

    feature_record = {
        "timestamp": event.ts,
        "user_id": event.user_id,
        "item_id": event.item_id,
        "session_id": event.session_id,
        "features": feats,
    }

    # Store in Redis
    redis_key = f"features:{event.user_id}:{event.item_id}"
    mapping = {k: str(v) for k, v in feats.items()}
    r.hset(redis_key, mapping=mapping)
    r.expire(redis_key, FEATURE_TTL)

    # Publish to Kafka
    features_topic = os.environ.get("FEATURES_ONLINE_TOPIC", "features.online")
    producer.produce(
        features_topic,
        key=event.user_id.encode("utf-8"),
        value=json.dumps(feature_record).encode("utf-8"),
    )
    producer.flush()


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    events_topic = os.environ.get("EVENTS_RAW_TOPIC", "events.raw")
    consumer_group = os.environ.get("CONSUMER_GROUP", "processor")

    start_http_server(9091)

    for _ in range(30):
        try:
            r = get_redis()
            r.ping()
            break
        except Exception as e:
            print(f"Redis not ready: {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Redis unavailable")

    for _ in range(30):
        try:
            pg_conn = get_postgres_conn()
            ensure_postgres_schema(pg_conn)
            break
        except Exception as e:
            print(f"Postgres not ready: {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Postgres unavailable")

    for _ in range(60):
        try:
            consumer = Consumer({
                "bootstrap.servers": bootstrap,
                "group.id": consumer_group,
                "auto.offset.reset": "earliest",
            })
            consumer.subscribe([events_topic])
            break
        except Exception as e:
            print(f"Kafka not ready: {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Kafka unavailable")

    producer = Producer({"bootstrap.servers": bootstrap})

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
            event = Event.from_dict(data)
        except Exception as e:
            print(f"Parse error: {e}")
            continue

        if not deduplicate(r, event.event_id):
            DEDUP_SKIPS.inc()
            continue

        persist_event(pg_conn, event)
        process_event(r, pg_conn, producer, event)
        PROCESSED_EVENTS.inc()


if __name__ == "__main__":
    main()
