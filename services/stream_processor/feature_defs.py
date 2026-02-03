"""
Shared feature definitions for online streaming and offline training.
Bayesian CTR smoothing: (clicks + 1) / (impressions + 11)
"""

import math
from typing import Dict, Any


def bayesian_ctr(impressions: int, clicks: int) -> float:
    """Bayesian smoothing: (clicks + 1) / (impressions + 11)."""
    return (clicks + 1.0) / (impressions + 11.0)


def compute_features(
    ts_ms: int,
    user_impressions_1m: int,
    user_clicks_1m: int,
    user_impressions_10m: int,
    user_clicks_10m: int,
    item_impressions_10m: int,
    item_clicks_10m: int,
    item_impressions_1h: int,
    item_clicks_1h: int,
    time_since_last_click_user_sec: float,
    session_events_5m: int,
) -> Dict[str, float]:
    """
    Compute all streaming features from raw counts and metadata.
    Used by both stream processor and trainer (approximation).
    """
    user_ctr_10m = bayesian_ctr(user_impressions_10m, user_clicks_10m)
    item_ctr_1h = bayesian_ctr(item_impressions_1h, item_clicks_1h)

    # hour_of_day (0-23)
    hour = (ts_ms // 1000) % 86400 // 3600
    # day_of_week (0=Monday, 6=Sunday)
    day_of_week = ((ts_ms // 1000) // 86400 + 3) % 7

    return {
        "user_impressions_1m": float(user_impressions_1m),
        "user_clicks_1m": float(user_clicks_1m),
        "user_ctr_10m": user_ctr_10m,
        "item_impressions_10m": float(item_impressions_10m),
        "item_clicks_10m": float(item_clicks_10m),
        "item_ctr_1h": item_ctr_1h,
        "time_since_last_click_user": time_since_last_click_user_sec,
        "session_events_5m": float(session_events_5m),
        "hour_of_day": float(hour),
        "day_of_week": float(day_of_week),
    }
