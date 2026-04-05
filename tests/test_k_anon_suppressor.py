from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

from privacy import k_anon_suppressor as ka


def test_suppresses_small_cohorts(spark):
    schema = StructType(
        [
            StructField("user_token", StringType()),
            StructField("app", StringType()),
            StructField("event_type", StringType()),
            StructField("region", StringType()),
        ]
    )
    rows = []
    for i in range(50):
        rows.append(Row(f"u{i}", "fb", "like", "US"))
    for i in range(120):
        rows.append(Row(f"v{i}", "fb", "share", "EU"))
    df = spark.createDataFrame(rows, schema)
    safe, suppressed = ka.suppress_low_k_cohorts(
        df, group_cols=["app", "event_type", "region"], user_col="user_token", k=100
    )
    types = {r.event_type for r in safe.select("event_type").distinct().collect()}
    assert types == {"share"}
