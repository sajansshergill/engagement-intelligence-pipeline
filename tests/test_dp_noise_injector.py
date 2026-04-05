from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from privacy import dp_noise_injector as dp


def test_inject_adds_dp_columns(spark):
    schema = StructType(
        [
            StructField("app", StringType()),
            StructField("cnt", DoubleType()),
        ]
    )
    df = spark.createDataFrame([Row("fb", 100.0), Row("ig", 200.0)], schema)
    out = dp.inject_laplace_noise(df, metric_cols=["cnt"], budget_key="standard")
    cols = set(out.columns)
    assert "cnt_dp" in cols
    assert "dp_epsilon" in cols


def test_privacy_loss_report():
    r = dp.compute_privacy_loss_report([dp.BUDGETS["standard"], dp.BUDGETS["sensitive"]])
    assert r["num_queries"] == 2
    assert r["total_epsilon"] > 0
