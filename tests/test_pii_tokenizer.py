from __future__ import annotations

import os

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

from privacy import pii_tokenizer as pt


def test_pseudonymize_stable_within_epoch():
    os.environ["PII_HMAC_SECRET"] = "test-secret"
    a = pt.pseudonymize("user-1", epoch_id=7)
    b = pt.pseudonymize("user-1", epoch_id=7)
    c = pt.pseudonymize("user-1", epoch_id=8)
    assert a == b
    assert a != c


def test_tokenize_dataframe(spark):
    os.environ["PII_HMAC_SECRET"] = "test-secret"
    schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("app", StringType()),
        ]
    )
    df = spark.createDataFrame([Row("raw-1", "fb")], schema)
    out = pt.tokenize_dataframe(df, id_columns=["user_id"])
    row = out.collect()[0]
    assert row.user_id_token.startswith("tok_")
    assert hasattr(row, "token_epoch_id")
