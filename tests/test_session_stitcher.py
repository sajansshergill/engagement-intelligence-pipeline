from __future__ import annotations

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from etl import session_stitcher as ss


def _events_schema():
    meta = StructType(
        [
            StructField("region", StringType()),
            StructField("platform", StringType()),
            StructField("content_id", StringType()),
        ]
    )
    return StructType(
        [
            StructField("user_token", StringType()),
            StructField("event_type", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("app", StringType()),
            StructField("session_id", StringType()),
            StructField("metadata", meta),
        ]
    )


def test_assign_global_sessions_splits_on_gap(spark):
    schema = _events_schema()
    rows = [
        Row("u1", "like", datetime(2024, 1, 1, 10, 0, 0), "fb", "s1", Row("US", "ios", "c1")),
        Row("u1", "view", datetime(2024, 1, 1, 10, 10, 0), "ig", "s2", Row("US", "ios", "c2")),
        Row("u1", "share", datetime(2024, 1, 1, 11, 0, 0), "fb", "s3", Row("US", "ios", "c3")),
    ]
    base = spark.createDataFrame(rows, schema)
    df = base.select(
        "user_token",
        "event_type",
        "timestamp",
        "app",
        "session_id",
        F.col("metadata.region").alias("region"),
        F.col("metadata.platform").alias("platform"),
        F.col("metadata.content_id").alias("content_id"),
    )
    out = ss.assign_global_sessions(df)
    assert "global_session_id" in out.columns
    ids = [r.global_session_id for r in out.select("global_session_id").distinct().collect()]
    assert len(ids) == 2


def test_build_session_summary_cross_app_flag(spark):
    schema = _events_schema()
    rows = [
        Row("u1", "like", datetime(2024, 1, 1, 9, 0, 0), "fb", "s1", Row("US", "ios", "c1")),
        Row("u1", "like", datetime(2024, 1, 1, 9, 5, 0), "ig", "s2", Row("US", "ios", "c2")),
    ]
    base = spark.createDataFrame(rows, schema)
    df = base.select(
        "user_token",
        "event_type",
        "timestamp",
        "app",
        "session_id",
        F.col("metadata.region").alias("region"),
        F.col("metadata.platform").alias("platform"),
        F.col("metadata.content_id").alias("content_id"),
    )
    stitched = ss.assign_global_sessions(df)
    summary = ss.build_session_summary(stitched)
    row = summary.collect()[0]
    assert row.is_cross_app is True
    assert row.app_count == 2
