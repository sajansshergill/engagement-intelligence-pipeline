from __future__ import annotations

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from etl import deduplicator as dd


def test_dedup_keeps_first_fingerprint(spark):
    schema = StructType(
        [
            StructField("user_token", StringType()),
            StructField("event_type", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("app", StringType()),
        ]
    )
    t = datetime(2024, 2, 1, 8, 0, 0)
    rows = [
        Row("u1", "like", t, "fb"),
        Row("u1", "like", t.replace(second=30), "fb"),
    ]
    df = spark.createDataFrame(rows, schema)
    fp = dd.make_event_fingerprint(df)
    out = dd.dedup_with_window(fp)
    assert out.count() == 1


def test_bloom_filter_dedup_list():
    events = [
        {"user_token": "u", "event_type": "x", "app": "fb", "timestamp": "2024-01-01T08:00:00"},
        {"user_token": "u", "event_type": "x", "app": "fb", "timestamp": "2024-01-01T08:00:15"},
    ]
    out = dd.dedup_small_batch_bloom(events)
    assert len(out) == 1
