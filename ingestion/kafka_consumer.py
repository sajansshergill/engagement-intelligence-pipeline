import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPPORTED_APPS = ["fb", "ig", "threads", "wa"]

TOPIC_MAP = {
    "fb":      "fb_engagement_raw",
    "ig":      "ig_engagement_raw",
    "threads": "threads_engagement_raw",
    "wa":      "wa_engagement_raw",
}

REQUIRED_FIELDS = {"user_token", "event_type", "timestamp", "app", "session_id"}


@dataclass
class EngagementEvent:
    user_token: str
    event_type: str
    timestamp: str
    app: str
    session_id: str
    metadata: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)

    @staticmethod
    def from_dict(data: dict) -> "EngagementEvent":
        return EngagementEvent(
            user_token=data["user_token"],
            event_type=data["event_type"],
            timestamp=data["timestamp"],
            app=data["app"],
            session_id=data["session_id"],
            metadata=data.get("metadata", {}),
            raw=data,
        )


@dataclass
class ValidationResult:
    is_valid: bool
    event: Optional[EngagementEvent] = None
    reason: Optional[str] = None


def validate_event(raw: dict) -> ValidationResult:
    missing = REQUIRED_FIELDS - raw.keys()
    if missing:
        return ValidationResult(is_valid=False, reason=f"Missing fields: {missing}")

    if raw["app"] not in SUPPORTED_APPS:
        return ValidationResult(is_valid=False, reason=f"Unknown app: {raw['app']}")

    try:
        datetime.fromisoformat(raw["timestamp"])
    except ValueError:
        return ValidationResult(is_valid=False, reason=f"Bad timestamp: {raw['timestamp']}")

    if not raw["user_token"] or not raw["session_id"]:
        return ValidationResult(is_valid=False, reason="Empty user_token or session_id")

    return ValidationResult(is_valid=True, event=EngagementEvent.from_dict(raw))


class MetaKafkaConsumer:
    """
    Consumes raw engagement events from Kafka topics for all Meta apps.
    Valid events are forwarded to a handler; invalid events go to dead-letter.
    """

    def __init__(
        self,
        bootstrap_servers: list[str],
        apps: list[str] = None,
        group_id: str = "meta-engagement-consumer",
        on_valid: Callable[[EngagementEvent], None] = None,
        on_invalid: Callable[[dict, str], None] = None,
    ):
        self.apps = apps or SUPPORTED_APPS
        self.topics = [TOPIC_MAP[app] for app in self.apps]
        self.on_valid = on_valid or self._default_valid_handler
        self.on_invalid = on_invalid or self._default_invalid_handler

        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=500,
        )

        self.stats = {"consumed": 0, "valid": 0, "invalid": 0}

    def consume(self, max_messages: int = None):
        logger.info(f"Consuming from topics: {self.topics}")
        consumed = 0

        try:
            for message in self.consumer:
                raw = message.value
                result = validate_event(raw)
                self.stats["consumed"] += 1

                if result.is_valid:
                    self.stats["valid"] += 1
                    self.on_valid(result.event)
                else:
                    self.stats["invalid"] += 1
                    self.on_invalid(raw, result.reason)

                consumed += 1
                if max_messages and consumed >= max_messages:
                    break

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        finally:
            self.consumer.close()
            self._log_stats()

    def _log_stats(self):
        total = self.stats["consumed"]
        valid_pct = (self.stats["valid"] / total * 100) if total else 0
        logger.info(
            f"Stats — consumed: {total} | valid: {self.stats['valid']} "
            f"({valid_pct:.1f}%) | invalid: {self.stats['invalid']}"
        )

    @staticmethod
    def _default_valid_handler(event: EngagementEvent):
        logger.debug(f"Valid event: {event.app} | {event.event_type} | {event.user_token}")

    @staticmethod
    def _default_invalid_handler(raw: dict, reason: str):
        logger.warning(f"Invalid event: {reason} | raw: {raw}")


if __name__ == "__main__":
    def handle_valid(event: EngagementEvent):
        logger.info(f"[VALID] {event.app} | {event.event_type} | session={event.session_id}")

    def handle_invalid(raw: dict, reason: str):
        logger.warning(f"[DEAD-LETTER] {reason} | {raw}")

    consumer = MetaKafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        apps=["fb", "ig", "threads", "wa"],
        on_valid=handle_valid,
        on_invalid=handle_invalid,
    )
    consumer.consume(max_messages=10000)