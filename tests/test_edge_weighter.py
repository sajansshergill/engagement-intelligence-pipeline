from __future__ import annotations

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from etl import edge_weighter as ew


def _schema():
    meta = StructType([StructField("target_user_token", StringType())])
    return StructType(
        [
            StructField("user_token", StringType()),
            StructField("metadata", meta),
            StructField("event_type", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("app", StringType()),
        ]
    )


def test_reciprocity_boosts_mutual_edges(spark):
    ts = datetime(2024, 1, 15, 12, 0, 0)
    schema = _schema()
    rows = [
        Row("a", Row("b"), "message_sent", ts, "wa"),
        Row("b", Row("a"), "message_read", ts, "wa"),
    ]
    df = (
        spark.createDataFrame(rows, schema)
        .select(
            "user_token",
            F.col("metadata.target_user_token").alias("target_user_token"),
            "event_type",
            "timestamp",
            "app",
        )
    )
    w = ew.apply_interaction_weights(df)
    d = ew.apply_recency_decay(w, reference_time=datetime(2024, 1, 20, 12, 0, 0))
    agg = ew.aggregate_edge_weights(d)
    out = ew.apply_reciprocity_bonus(agg)
    r = out.filter(out.user_token == "a").collect()[0]
    assert r.reciprocity_multiplier == 1.5
