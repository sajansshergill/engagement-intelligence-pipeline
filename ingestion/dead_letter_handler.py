"""
Dead-letter handler — consumes malformed events, logs them with structured
metadata, attempts auto-recovery for common fixable issues, and writes
unrecoverable events to a persistent audit table (DuckDB for local dev).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import duckdb
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = ["localhost:9092"]
DEAD_LETTER_TOPIC = "dead_letter_queue"
RECOVERED_TOPIC = "validated_engagement"
AUDIT_DB_PATH = "data/dead_letter_audit.duckdb"

RECOVERABLE_REASONS = {
    "missing_session_id": "Generate a fallback session_id from user_token + timestamp",
    "bad_timestamp_format": "Attempt ISO parse with multiple format fallbacks",
}


def attempt_recovery(event: dict, reason: str) -> Optional[dict]:
    """
    Tries to fix common, recoverable validation failures.
    Returns the fixed event dict if successful, None otherwise.
    """
    if "Missing fields: {'session_id'}" in reason:
        ts = event.get("timestamp", datetime.now(timezone.utc).isoformat())
        event["session_id"] = f"recovered_{event.get('user_token', 'unknown')}_{ts}"
        event["_recovered"] = True
        event["_recovery_reason"] = "Generated fallback session_id"
        logger.info(f"Recovered event — generated session_id for user {event.get('user_token')}")
        return event

    if "Invalid timestamp format" in reason:
        raw_ts = event.get("timestamp", "")
        for fmt in ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y"]:
            try:
                parsed = datetime.strptime(raw_ts, fmt)
                event["timestamp"] = parsed.isoformat()
                event["_recovered"] = True
                event["_recovery_reason"] = f"Reformatted timestamp from format {fmt}"
                logger.info(f"Recovered event — fixed timestamp for user {event.get('user_token')}")
                return event
            except ValueError:
                continue

    return None


class DeadLetterHandler:
    """
    Consumes from the dead-letter queue.
    - Attempts recovery on fixable events → re-publishes to validated topic
    - Writes unrecoverable events to DuckDB audit table
    """

    def __init__(self):
        self.consumer = KafkaConsumer(
            DEAD_LETTER_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id="dead-letter-handler",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.db = duckdb.connect(AUDIT_DB_PATH)
        self._init_audit_table()

        self.stats = {"consumed": 0, "recovered": 0, "unrecoverable": 0}

    def _init_audit_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter_audit (
                id          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                received_at TIMESTAMP,
                app         VARCHAR,
                user_token  VARCHAR,
                event_type  VARCHAR,
                reason      VARCHAR,
                recovered   BOOLEAN,
                raw_event   JSON
            )
        """)

    def _write_audit(self, event: dict, reason: str, recovered: bool):
        self.db.execute("""
            INSERT INTO dead_letter_audit
                (received_at, app, user_token, event_type, reason, recovered, raw_event)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            datetime.now(timezone.utc),
            event.get("app"),
            event.get("user_token"),
            event.get("event_type"),
            reason,
            recovered,
            json.dumps(event),
        ])

    def run(self, max_messages: int = None):
        logger.info("Dead-letter handler started.")
        consumed = 0

        for message in self.consumer:
            event = message.value
            reason = event.get("_reason", "unknown")
            self.stats["consumed"] += 1

            recovered = attempt_recovery(event, reason)

            if recovered:
                self.producer.send(RECOVERED_TOPIC, value=recovered)
                self._write_audit(recovered, reason, recovered=True)
                self.stats["recovered"] += 1
            else:
                self._write_audit(event, reason, recovered=False)
                self.stats["unrecoverable"] += 1
                logger.warning(f"Unrecoverable event: {reason} | app={event.get('app')}")

            consumed += 1
            if max_messages and consumed >= max_messages:
                break

        self._log_stats()
        self.consumer.close()
        self.producer.flush()
        self.db.close()

    def _log_stats(self):
        total = self.stats["consumed"]
        rec_pct = (self.stats["recovered"] / total * 100) if total else 0
        logger.info(
            f"Dead-letter stats — total: {total} | "
            f"recovered: {self.stats['recovered']} ({rec_pct:.1f}%) | "
            f"unrecoverable: {self.stats['unrecoverable']}"
        )


if __name__ == "__main__":
    handler = DeadLetterHandler()
    handler.run()