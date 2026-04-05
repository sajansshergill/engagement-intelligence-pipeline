from __future__ import annotations

from ingestion.kafka_consumer import validate_event


def test_validate_event_accepts_well_formed():
    raw = {
        "user_token": "t1",
        "event_type": "like",
        "timestamp": "2024-01-01T12:00:00",
        "app": "fb",
        "session_id": "s1",
    }
    r = validate_event(raw)
    assert r.is_valid
    assert r.event.app == "fb"


def test_validate_event_rejects_bad_app():
    raw = {
        "user_token": "t1",
        "event_type": "like",
        "timestamp": "2024-01-01T12:00:00",
        "app": "snapchat",
        "session_id": "s1",
    }
    r = validate_event(raw)
    assert not r.is_valid
