"""
Synthetic event generator — simulates 3B+ scale engagement events
across all four Meta apps for local pipeline testing.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

APPS = ["fb", "ig", "threads", "wa"]
EVENT_TYPES = {
    "fb":      ["like", "share", "comment", "react", "click", "view"],
    "ig":      ["story_view", "reel_watch", "like", "save", "dm", "follow"],
    "threads": ["post_view", "reply", "repost", "like", "follow"],
    "wa":      ["message_sent", "message_read", "media_shared", "voice_note"],
}
TOPIC_MAP = {
    "fb": "fb_engagement_raw",
    "ig": "ig_engagement_raw",
    "threads": "threads_engagement_raw",
    "wa": "wa_engagement_raw",
}

# Inject ~3% invalid events to test validation
INVALID_RATE = 0.03


def make_valid_event(app: str) -> dict:
    base_time = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 60))
    return {
        "user_token": f"tok_{uuid.uuid4().hex[:16]}",
        "event_type": random.choice(EVENT_TYPES[app]),
        "timestamp": base_time.isoformat(),
        "app": app,
        "session_id": f"sess_{uuid.uuid4().hex[:12]}",
        "metadata": {
            "content_id": f"cid_{random.randint(1000, 9999999)}",
            "region": random.choice(["US", "EU", "APAC", "LATAM"]),
            "platform": random.choice(["ios", "android", "web"]),
        },
    }


def make_invalid_event(app: str) -> dict:
    event = make_valid_event(app)
    fault = random.choice(["missing_session", "bad_timestamp", "unknown_app", "empty_token"])
    if fault == "missing_session":
        del event["session_id"]
    elif fault == "bad_timestamp":
        event["timestamp"] = "not-a-real-date"
    elif fault == "unknown_app":
        event["app"] = "snapchat"
    elif fault == "empty_token":
        event["user_token"] = ""
    return event


def simulate(
    n: int = 100_000,
    bootstrap_servers: str = "localhost:9092",
    apps: list[str] | None = None,
):
    app_pool = apps if apps else APPS
    if not app_pool:
        raise ValueError("apps must be a non-empty list")

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    invalid_count = 0

    for i in range(n):
        app = random.choice(app_pool)
        is_invalid = random.random() < INVALID_RATE

        event = make_invalid_event(app) if is_invalid else make_valid_event(app)
        if is_invalid:
            invalid_count += 1

        producer.send(TOPIC_MAP[app], value=event)
        sent += 1

        if sent % 10_000 == 0:
            print(f"Sent {sent:,} events ({invalid_count} invalid so far)...")

    producer.flush()
    producer.close()
    print(f"\nDone. Total sent: {sent:,} | Invalid injected: {invalid_count} ({invalid_count/sent*100:.1f}%)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--n",
        type=int,
        default=100_000,
        help="Number of events to generate",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=None,
        help="Alias for --n (README / batch job compatibility)",
    )
    parser.add_argument("--brokers", type=str, default="localhost:9092")
    parser.add_argument(
        "--apps",
        type=str,
        default="fb,ig,threads,wa",
        help="Comma-separated apps to include (subset of fb,ig,threads,wa)",
    )
    args = parser.parse_args()
    n = args.scale if args.scale is not None else args.n
    apps = [a.strip() for a in args.apps.split(",") if a.strip()]
    invalid = [a for a in apps if a not in APPS]
    if invalid:
        raise SystemExit(f"Unknown app(s): {invalid}. Valid: {APPS}")
    simulate(n=n, bootstrap_servers=args.brokers, apps=apps)