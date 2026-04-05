from __future__ import annotations

from ingestion.dead_letter_handler import attempt_recovery


def test_recover_missing_session_id():
    event = {
        "user_token": "u1",
        "event_type": "like",
        "timestamp": "2024-01-01T12:00:00",
        "app": "fb",
    }
    fixed = attempt_recovery(event, "Missing fields: {'session_id'}")
    assert fixed is not None
    assert "session_id" in fixed
    assert fixed.get("_recovered") is True
