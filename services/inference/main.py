"""Inference service: FastAPI for CTR predictions."""

import os
import pickle
import time
from typing import Optional

import redis
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from sklearn.linear_model import LogisticRegression
import numpy as np

app = FastAPI(title="CTR Inference Service")

# Prometheus metrics
REQUEST_COUNT = Counter("inference_requests_total", "Total inference requests", ["cold_start"])
REQUEST_LATENCY = Histogram("inference_request_latency_seconds", "Inference request latency")

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

DEFAULT_PROBABILITY = 0.05
MODEL_VERSION = "1.0.0"


def get_redis():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url)


def load_model():
    path = os.environ.get("MODEL_PATH", "/models/model.pkl")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f), False
    # Fallback baseline: use user_ctr_10m as proxy for click probability
    return None, True


_model, is_baseline = load_model()


def predict_probability(model_or_none, features: dict) -> float:
    if model_or_none is not None:
        vec = np.array([[features.get(f, 0.0) for f in FEATURE_ORDER]], dtype=np.float64)
        return float(model_or_none.predict_proba(vec)[0, 1])
    # Baseline: use user_ctr_10m if available, else DEFAULT_PROBABILITY
    ctr = features.get("user_ctr_10m", DEFAULT_PROBABILITY)
    return min(1.0, max(0.0, float(ctr)))


class PredictRequest(BaseModel):
    user_id: str
    item_id: str


class PredictResponse(BaseModel):
    probability: float
    model_version: str
    cold_start: bool


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    start = time.perf_counter()
    r = get_redis()

    redis_key = f"features:{req.user_id}:{req.item_id}"
    raw = r.hgetall(redis_key)

    if not raw:
        REQUEST_COUNT.labels(cold_start="true").inc()
        REQUEST_LATENCY.observe(time.perf_counter() - start)
        return PredictResponse(
            probability=DEFAULT_PROBABILITY,
            model_version=MODEL_VERSION,
            cold_start=True,
        )

    features = {}
    for k, v in raw.items():
        key = k.decode("utf-8") if isinstance(k, bytes) else k
        val = v.decode("utf-8") if isinstance(v, bytes) else v
        try:
            features[key] = float(val)
        except ValueError:
            features[key] = 0.0

    prob = predict_probability(_model, features)

    REQUEST_COUNT.labels(cold_start="false").inc()
    REQUEST_LATENCY.observe(time.perf_counter() - start)

    return PredictResponse(
        probability=min(1.0, max(0.0, prob)),
        model_version=MODEL_VERSION,
        cold_start=False,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
