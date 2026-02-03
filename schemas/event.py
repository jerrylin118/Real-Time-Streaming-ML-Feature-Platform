"""Strict data contract for events and feature records."""

from dataclasses import dataclass, field
from typing import Literal, Optional
import json
import uuid


EVENT_TYPES = ("impression", "click")


@dataclass
class Event:
    """Event schema: clickstream impression or click."""

    event_id: str
    event_type: Literal["impression", "click"]
    ts: int
    user_id: str
    item_id: str
    session_id: str
    device: str
    country: str

    def to_json(self) -> str:
        return json.dumps({
            "event_id": self.event_id,
            "event_type": self.event_type,
            "ts": self.ts,
            "user_id": self.user_id,
            "item_id": self.item_id,
            "session_id": self.session_id,
            "device": self.device,
            "country": self.country,
        })

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        return cls(
            event_id=str(d["event_id"]),
            event_type=d["event_type"],
            ts=int(d["ts"]),
            user_id=str(d["user_id"]),
            item_id=str(d["item_id"]),
            session_id=str(d["session_id"]),
            device=str(d["device"]),
            country=str(d["country"]),
        )

    @classmethod
    def from_json(cls, s: str) -> "Event":
        return cls.from_dict(json.loads(s))


@dataclass
class FeatureRecord:
    """Feature record written to Kafka and Redis."""

    timestamp: int
    user_id: str
    item_id: str
    session_id: str
    features: dict

    def to_json(self) -> str:
        return json.dumps({
            "timestamp": self.timestamp,
            "user_id": self.user_id,
            "item_id": self.item_id,
            "session_id": self.session_id,
            "features": self.features,
        })

    @classmethod
    def from_dict(cls, d: dict) -> "FeatureRecord":
        return cls(
            timestamp=int(d["timestamp"]),
            user_id=str(d["user_id"]),
            item_id=str(d["item_id"]),
            session_id=str(d["session_id"]),
            features=dict(d["features"]),
        )

    @classmethod
    def from_json(cls, s: str) -> "FeatureRecord":
        return cls.from_dict(json.loads(s))
