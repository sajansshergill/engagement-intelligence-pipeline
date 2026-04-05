"""
Deduplicator — removes duplicate events produced by client retries
using a Bloom filter for memory-efficient exactly-once semantics,
with a fallback deterministic hash for guaranteed correctness.
"""

import hashlib
import logging
import math

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bloom filter config
EXPECTED_ELEMENTS = 500_000_000   # 500M events per day
FALSE_POSITIVE_RATE = 0.001       # 0.1% false positive rate


class BloomFilter:
    """
    Pure-Python Bloom filter for driver-side dedup logic.
    For distributed dedup, use Spark's built-in approx_count_distinct
    or the PySpark bloomfilter join (Spark 3.3+).
    """

    def __init__(self, n: int, p: float):
        self.m = self._optimal_m(n, p)   # bit array size
        self.k = self._optimal_k(n, self.m)  # number of hash functions
        self.bit_array = bytearray(self.m // 8 + 1)
        self.count = 0

    @staticmethod
    def _optimal_m(n: int, p: float) -> int:
        return int(-n * math.log(p) / (math.log(2) ** 2))

    @staticmethod
    def _optimal_k(n: int, m: int) -> int:
        return max(1, int((m / n) * math.log(2)))

    def _hash_positions(self, item: str) -> list[int]:
        positions = []
        for i in range(self.k):
            digest = hashlib.md5(f"{item}{i}".encode()).hexdigest()
            pos = int(digest, 16) % self.m
            positions.append(pos)
        return positions

    def add(self, item: str):
        for pos in self._hash_positions(item):
            self.bit_array[pos // 8] |= (1 << (pos % 8))
        self.count += 1

    def __contains__(self, item: str) -> bool:
        return all(
            self.bit_array[pos // 8] & (1 << (pos % 8))
            for pos in self._hash_positions(item)
        )


def make_event_fingerprint(df: DataFrame) -> DataFrame:
    """
    Build a deterministic fingerprint per event.
    Two events are duplicates if they share user_token + event_type +
    timestamp (truncated to minute) + app.
    """
    return df.withColumn(
        "event_fingerprint",
        F.md5(
            F.concat_ws(
                "|",
                F.col("user_token"),
                F.col("event_type"),
                F.col("app"),
                F.date_trunc("minute", F.col("timestamp")).cast(StringType()),
            )
        )
    )


def dedup_with_window(df: DataFrame) -> DataFrame:
    """
    Distributed dedup using Spark window functions.
    Keeps the first occurrence of each fingerprint per partition date.
    This is the production-scale approach — no driver-side state needed.
    """
    from pyspark.sql import Window

    df = df.withColumn("event_date", F.to_date("timestamp"))

    dedup_window = Window.partitionBy("event_fingerprint", "event_date").orderBy("timestamp")

    df = df.withColumn("row_num", F.row_number().over(dedup_window))

    deduped = df.filter(F.col("row_num") == 1).drop("row_num")

    return deduped


def dedup_small_batch_bloom(events: list[dict]) -> list[dict]:
    """
    Driver-side Bloom filter dedup for small batches (e.g. streaming micro-batches).
    Not for full Spark jobs — use dedup_with_window for distributed scale.
    """
    bf = BloomFilter(n=len(events) * 2, p=FALSE_POSITIVE_RATE)
    seen = []

    for event in events:
        key = "|".join([
            event.get("user_token", ""),
            event.get("event_type", ""),
            event.get("app", ""),
            event.get("timestamp", "")[:16],  # truncate to minute
        ])
        fingerprint = hashlib.md5(key.encode()).hexdigest()

        if fingerprint not in bf:
            bf.add(fingerprint)
            seen.append(event)

    return seen


def run(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("MetaDeduplicator").getOrCreate()

    logger.info(f"Loading events from {input_path}")
    df = (
        spark.read.parquet(input_path)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )

    total_before = df.count()
    logger.info(f"Events before dedup: {total_before:,}")

    logger.info("Building event fingerprints...")
    df = make_event_fingerprint(df)

    logger.info("Running distributed window dedup...")
    deduped = dedup_with_window(df)

    total_after = deduped.count()
    removed = total_before - total_after
    logger.info(
        f"Events after dedup: {total_after:,} | "
        f"Duplicates removed: {removed:,} ({removed/total_before*100:.2f}%)"
    )

    logger.info(f"Writing deduped events to {output_path}")
    (
        deduped
        .drop("event_fingerprint", "event_date")
        .repartition(100)
        .write
        .mode("overwrite")
        .partitionBy("app")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(args.input, args.output)