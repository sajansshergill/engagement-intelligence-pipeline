"""
PII tokenizer — replaces raw user identifiers with rotating pseudonymous
tokens at ingest time. No raw IDs ever reach analytical tables.
"""

import hashlib
import hmac
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Token rotation period — tokens rotate every 30 days
TOKEN_ROTATION_DAYS = 30

# Secret key — in production pulled from AWS Secrets Manager / Vault
HMAC_SECRET = os.environ.get("PII_HMAC_SECRET", "dev-secret-replace-in-prod")


@dataclass
class TokenEpoch:
    epoch_id: int
    start_date: datetime
    end_date: datetime

    @staticmethod
    def current() -> "TokenEpoch":
        now = datetime.now(timezone.utc)
        epoch_id = int(now.timestamp() // (TOKEN_ROTATION_DAYS * 86400))
        start = datetime.fromtimestamp(
            epoch_id * TOKEN_ROTATION_DAYS * 86400, tz=timezone.utc
        )
        end = start + timedelta(days=TOKEN_ROTATION_DAYS)
        return TokenEpoch(epoch_id=epoch_id, start_date=start, end_date=end)


def pseudonymize(raw_id: str, epoch_id: int, secret: str = HMAC_SECRET) -> str:
    """
    HMAC-SHA256 pseudonymization.
    Same raw_id + same epoch → same token (deterministic within epoch).
    Different epoch → completely different token (rotation).
    Not reversible without the secret key.
    """
    message = f"{raw_id}:{epoch_id}".encode("utf-8")
    token = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"tok_{token[:32]}"


def make_tokenizer_udf(epoch_id: int, secret: str = HMAC_SECRET):
    """Returns a Spark UDF that tokenizes a raw user ID for the given epoch."""
    def _tokenize(raw_id: Optional[str]) -> Optional[str]:
        if not raw_id:
            return None
        return pseudonymize(raw_id, epoch_id, secret)

    return F.udf(_tokenize, StringType())


def tokenize_dataframe(df: DataFrame, id_columns: list[str]) -> DataFrame:
    """
    Tokenizes all specified PII columns in a Spark DataFrame.
    Original columns are dropped after tokenization.
    """
    epoch = TokenEpoch.current()
    logger.info(
        f"Tokenizing with epoch {epoch.epoch_id} "
        f"(valid {epoch.start_date.date()} → {epoch.end_date.date()})"
    )

    tokenizer = make_tokenizer_udf(epoch.epoch_id)

    for col_name in id_columns:
        if col_name not in df.columns:
            logger.warning(f"Column {col_name} not found — skipping tokenization")
            continue

        token_col = f"{col_name}_token"
        df = df.withColumn(token_col, tokenizer(F.col(col_name)))
        df = df.drop(col_name)
        logger.info(f"Tokenized: {col_name} → {token_col}")

    df = df.withColumn("token_epoch_id", F.lit(epoch.epoch_id))
    df = df.withColumn("tokenized_at", F.current_timestamp())

    return df


def run(input_path: str, output_path: str, pii_columns: list[str]):
    spark = SparkSession.builder.appName("MetaPIITokenizer").getOrCreate()

    logger.info(f"Loading events from {input_path}")
    df = spark.read.parquet(input_path)

    total = df.count()
    logger.info(f"Tokenizing {total:,} records | PII columns: {pii_columns}")

    tokenized = tokenize_dataframe(df, pii_columns)

    logger.info(f"Writing tokenized data to {output_path}")
    (
        tokenized
        .repartition(100)
        .write
        .mode("overwrite")
        .partitionBy("app")
        .parquet(output_path)
    )

    logger.info(f"Done. {total:,} records tokenized.")
    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--pii-columns",
        nargs="+",
        default=["user_id", "target_user_id", "device_id", "ip_address"],
        help="Column names containing PII to tokenize"
    )
    args = parser.parse_args()
    run(args.input, args.output, args.pii_columns)