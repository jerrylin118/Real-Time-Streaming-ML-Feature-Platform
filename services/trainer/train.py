"""Trainer service: offline training from Postgres."""

import json
import os
import sys
from collections import defaultdict
from typing import List, Tuple

import psycopg
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
import numpy as np

# Import feature definitions (PYTHONPATH=/app)
from feature_defs import compute_features

FEATURE_ORDER = [
    "user_impressions_1m",
    "user_clicks_1m",
    "user_ctr_10m",
    "item_impressions_10m",
    "item_clicks_10m",
    "item_ctr_1h",
    "time_since_last_click_user",
    "session_events_5m",
    "hour_of_day",
    "day_of_week",
]

# Window definitions in seconds
WINDOW_1M = 60
WINDOW_5M = 300
WINDOW_10M = 600
WINDOW_1H = 3600


def get_postgres_conn():
    return psycopg.connect(
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('POSTGRES_USER', 'mlplatform')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'mlplatform')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'mlplatform')}"
    )


def load_events(conn, start_ts: int, end_ts: int) -> List[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id, event_type, ts, user_id, item_id, session_id, device, country
            FROM events
            WHERE ts >= %s AND ts <= %s
            ORDER BY ts ASC
            """,
            (start_ts, end_ts),
        )
        rows = cur.fetchall()
    return [
        {
            "event_id": r[0],
            "event_type": r[1],
            "ts": r[2],
            "user_id": r[3],
            "item_id": r[4],
            "session_id": r[5],
            "device": r[6],
            "country": r[7],
        }
        for r in rows
    ]


def build_training_dataset(events: List[dict]) -> Tuple[np.ndarray, np.ndarray]:
    """
    For each impression, create label 1 if click within 60s (same user, item, session).
    Compute features using deterministic approximation from historical events.
    """
    # Group clicks by (user_id, item_id, session_id) -> list of timestamps
    clicks_by_key = defaultdict(list)
    for e in events:
        if e["event_type"] == "click":
            k = (e["user_id"], e["item_id"], e["session_id"])
            clicks_by_key[k].append(e["ts"])

    # Build rolling counts per entity (approximation)
    user_imps_1m = defaultdict(lambda: defaultdict(int))
    user_clks_1m = defaultdict(lambda: defaultdict(int))
    user_imps_10m = defaultdict(lambda: defaultdict(int))
    user_clks_10m = defaultdict(lambda: defaultdict(int))
    item_imps_10m = defaultdict(lambda: defaultdict(int))
    item_clks_10m = defaultdict(lambda: defaultdict(int))
    item_imps_1h = defaultdict(lambda: defaultdict(int))
    item_clks_1h = defaultdict(lambda: defaultdict(int))
    session_events_5m = defaultdict(lambda: defaultdict(int))
    user_last_click = defaultdict(int)

    X_list = []
    y_list = []

    # Process events in order to build counts
    for event in events:
        ts = event["ts"]
        ts_sec = ts / 1000
        user_id = event["user_id"]
        item_id = event["item_id"]
        session_id = event["session_id"]
        et = event["event_type"]

        # Prune old entries (simplified: use cutoff based on current ts)
        cutoff_1m = ts_sec - WINDOW_1M
        cutoff_5m = ts_sec - WINDOW_5M
        cutoff_10m = ts_sec - WINDOW_10M
        cutoff_1h = ts_sec - WINDOW_1H

        # Update counts
        if et == "impression":
            user_imps_1m[user_id][ts_sec] += 1
            user_imps_10m[user_id][ts_sec] += 1
            item_imps_10m[item_id][ts_sec] += 1
            item_imps_1h[item_id][ts_sec] += 1
            session_events_5m[session_id][ts_sec] += 1
        else:
            user_clks_1m[user_id][ts_sec] += 1
            user_clks_10m[user_id][ts_sec] += 1
            item_clks_10m[item_id][ts_sec] += 1
            item_clks_1h[item_id][ts_sec] += 1
            session_events_5m[session_id][ts_sec] += 1
            user_last_click[user_id] = ts

        # Only build training rows for impressions
        if et != "impression":
            continue

        # Label: 1 if any click with same (user, item, session) within 60s after impression
        label = 0
        for click_ts in clicks_by_key.get((user_id, item_id, session_id), []):
            if 0 <= (click_ts - ts) <= 60_000:
                label = 1
                break

        # Compute rolling counts at this timestamp
        def count_in_window(d, entity_id, window_sec):
            total = 0
            for t, c in d[entity_id].items():
                if t >= ts_sec - window_sec and t <= ts_sec:
                    total += c
            return total

        ui1 = count_in_window(user_imps_1m, user_id, WINDOW_1M)
        uc1 = count_in_window(user_clks_1m, user_id, WINDOW_1M)
        ui10 = count_in_window(user_imps_10m, user_id, WINDOW_10M)
        uc10 = count_in_window(user_clks_10m, user_id, WINDOW_10M)
        ii10 = count_in_window(item_imps_10m, item_id, WINDOW_10M)
        ic10 = count_in_window(item_clks_10m, item_id, WINDOW_10M)
        ii1h = count_in_window(item_imps_1h, item_id, WINDOW_1H)
        ic1h = count_in_window(item_clks_1h, item_id, WINDOW_1H)
        se5 = count_in_window(session_events_5m, session_id, WINDOW_5M)

        last_click = user_last_click.get(user_id, 0)
        time_since = (ts - last_click) / 1000.0 if last_click else 86400.0

        feats = compute_features(
            ts,
            ui1, uc1, ui10, uc10,
            ii10, ic10, ii1h, ic1h,
            time_since,
            se5,
        )
        vec = [feats[f] for f in FEATURE_ORDER]
        X_list.append(vec)
        y_list.append(label)

    if not X_list:
        return np.array([]).reshape(0, len(FEATURE_ORDER)), np.array([])

    return np.array(X_list), np.array(y_list)


def main():
    conn = get_postgres_conn()

    # Default: last 24 hours
    import time as t
    end_ts = int(t.time() * 1000)
    start_ts = end_ts - 24 * 3600 * 1000

    if os.environ.get("TRAIN_START_TS"):
        start_ts = int(os.environ["TRAIN_START_TS"])
    if os.environ.get("TRAIN_END_TS"):
        end_ts = int(os.environ["TRAIN_END_TS"])

    events = load_events(conn, start_ts, end_ts)
    conn.close()

    if len(events) < 100:
        print("Insufficient events for training. Need at least 100.")
        sys.exit(1)

    X, y = build_training_dataset(events)
    if len(X) == 0 or len(np.unique(y)) < 2:
        print("Insufficient positive/negative labels for training.")
        sys.exit(1)

    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X, y)

    proba = model.predict_proba(X)[:, 1]
    auc = roc_auc_score(y, proba) if len(np.unique(y)) == 2 else 0.0

    model_path = os.environ.get("MODEL_OUTPUT_PATH", "/models/model.pkl")
    metrics_path = os.environ.get("METRICS_OUTPUT_PATH", "/models/metrics.json")

    os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)
    with open(model_path, "wb") as f:
        import pickle
        pickle.dump(model, f)

    metrics = {"auc": auc, "n_samples": len(X), "n_positive": int(y.sum())}
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"Model saved to {model_path}")
    print(f"Metrics: AUC={auc:.4f}, n_samples={len(X)}")


if __name__ == "__main__":
    main()
