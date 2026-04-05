"""
Flink stream processor — validates events, routes valid events to Hive staging
and invalid events to the dead-letter Kafka topic.

Note: Runs locally with pyflink. In production this submits to a Flink cluster.
"""

import json
import logging
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetResetStrategy,
    KafkaSink,
    KafkaRecordSerializationSchema,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = "localhost:9092"
INPUT_TOPICS = ["fb_engagement_raw", "ig_engagement_raw", "threads_engagement_raw", "wa_engagement_raw"]
DEAD_LETTER_TOPIC = "dead_letter_queue"
VALID_STAGING_TOPIC = "validated_engagement"
REQUIRED_FIELDS = {"user_token", "event_type", "timestamp", "app", "session_id"}
SUPPORTED_APPS = {"fb", "ig", "threads", "wa"}


def validate_and_tag(raw_json: str) -> str:
    """
    Parses and validates a raw event JSON string.
    Returns a JSON string with an added `_valid` boolean and `_reason` field.
    """
    try:
        event = json.loads(raw_json)
    except json.JSONDecodeError as e:
        return json.dumps({"_valid": False, "_reason": f"JSON parse error: {e}", "_raw": raw_json})

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        event["_valid"] = False
        event["_reason"] = f"Missing fields: {missing}"
        return json.dumps(event)

    if event.get("app") not in SUPPORTED_APPS:
        event["_valid"] = False
        event["_reason"] = f"Unsupported app: {event.get('app')}"
        return json.dumps(event)

    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        event["_valid"] = False
        event["_reason"] = f"Invalid timestamp format: {event['timestamp']}"
        return json.dumps(event)

    event["_valid"] = True
    event["_reason"] = None
    return json.dumps(event)


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    # Source — consume from all four app topics
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(*INPUT_TOPICS)
        .set_group_id("flink-engagement-validator")
        .set_starting_offsets(KafkaOffsetResetStrategy.EARLIEST)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "KafkaEngagementSource",
    )

    # Validate and tag each event
    tagged = stream.map(validate_and_tag, output_type=Types.STRING())

    # Split: valid → staging topic, invalid → dead-letter topic
    valid_stream = tagged.filter(
        lambda e: json.loads(e).get("_valid") is True
    )
    invalid_stream = tagged.filter(
        lambda e: json.loads(e).get("_valid") is False
    )

    # Sink — valid events
    valid_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(VALID_STAGING_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Sink — dead-letter events
    dead_letter_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DEAD_LETTER_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    valid_stream.sink_to(valid_sink)
    invalid_stream.sink_to(dead_letter_sink)

    logger.info("Submitting Flink engagement validation job...")
    env.execute("MetaEngagementValidationPipeline")


if __name__ == "__main__":
    build_pipeline()