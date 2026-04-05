"""
Edge weighter — scores social graph edges (user → user interactions)
using interaction type weight, recency decay, and reciprocity bonus.
Output feeds downstream ranking and recommendation models.
"""

import logging
import math
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Interaction type base weights — higher = stronger social signal
INTERACTION_WEIGHTS = {
    "message_sent":   1.0,
    "share":          0.9,
    "comment":        0.8,
    "reply":          0.8,
    "react":          0.5,
    "like":           0.4,
    "follow":         0.7,
    "story_view":     0.2,
    "reel_watch":     0.25,
    "post_view":      0.15,
    "save":           0.6,
    "dm":             1.0,
    "repost":         0.85,
    "voice_note":     1.0,
    "media_shared":   0.9,
    "message_read":   0.3,
    "click":          0.3,
    "view":           0.15,
}

# Half-life for recency decay — interactions older than this decay to ~50% weight
DECAY_HALF_LIFE_DAYS = 14


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MetaEdgeWeighter")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_events(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.parquet(path)
        .select(
            "user_token",
            F.col("metadata.target_user_token").alias("target_user_token"),
            "event_type",
            "timestamp",
            "app",
        )
        .filter(F.col("target_user_token").isNotNull())
        .filter(F.col("user_token") != F.col("target_user_token"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )


def apply_interaction_weights(df: DataFrame) -> DataFrame:
    """Map each event type to its base interaction weight."""
    weight_map = F.create_map(
        *[x for kv in INTERACTION_WEIGHTS.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )
    return df.withColumn(
        "base_weight",
        F.coalesce(weight_map[F.col("event_type")], F.lit(0.1))
    )


def apply_recency_decay(df: DataFrame, reference_time: datetime = None) -> DataFrame:
    """
    Exponential decay based on event age.
    weight = base_weight * exp(-lambda * days_old)
    where lambda = ln(2) / half_life
    """
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)

    ref_ts = reference_time.timestamp()
    decay_lambda = math.log(2) / DECAY_HALF_LIFE_DAYS

    df = df.withColumn(
        "days_old",
        (F.lit(ref_ts) - F.unix_timestamp("timestamp")) / 86400
    )

    df = df.withColumn(
        "decayed_weight",
        F.col("base_weight") * F.exp(-F.lit(decay_lambda) * F.col("days_old"))
    )

    return df


def aggregate_edge_weights(df: DataFrame) -> DataFrame:
    """
    Sum decayed weights per directed edge (source → target, per app).
    Also count interaction frequency as a separate signal.
    """
    return df.groupBy("user_token", "target_user_token", "app").agg(
        F.sum("decayed_weight").alias("raw_edge_weight"),
        F.count("*").alias("interaction_count"),
        F.max("timestamp").alias("last_interaction_at"),
        F.collect_set("event_type").alias("interaction_types"),
    )


def apply_reciprocity_bonus(df: DataFrame) -> DataFrame:
    """
    Boost edge weight when interaction is mutual (A→B and B→A both exist).
    Reciprocal edges get a 1.5x multiplier — strong signal of real relationship.
    """
    # Self-join to find reverse edges
    reverse = df.select(
        F.col("user_token").alias("target_user_token"),
        F.col("target_user_token").alias("user_token"),
        F.col("app"),
        F.lit(True).alias("has_reverse"),
    )

    df = df.join(
        reverse,
        on=["user_token", "target_user_token", "app"],
        how="left"
    )

    df = df.withColumn(
        "reciprocity_multiplier",
        F.when(F.col("has_reverse") == True, F.lit(1.5)).otherwise(F.lit(1.0))
    )

    df = df.withColumn(
        "final_edge_weight",
        F.round(F.col("raw_edge_weight") * F.col("reciprocity_multiplier"), 6)
    )

    return df.drop("has_reverse")


def normalize_weights(df: DataFrame) -> DataFrame:
    """
    Min-max normalize final_edge_weight per app so weights are
    comparable across apps with different interaction volumes.
    """
    app_window = Window.partitionBy("app")

    df = df.withColumn("app_min", F.min("final_edge_weight").over(app_window))
    df = df.withColumn("app_max", F.max("final_edge_weight").over(app_window))

    df = df.withColumn(
        "normalized_weight",
        F.when(
            F.col("app_max") == F.col("app_min"),
            F.lit(0.5)
        ).otherwise(
            F.round(
                (F.col("final_edge_weight") - F.col("app_min")) /
                (F.col("app_max") - F.col("app_min")),
                6
            )
        )
    )

    return df.drop("app_min", "app_max")


def run(input_path: str, output_path: str):
    spark = get_spark()
    logger.info(f"Loading events from {input_path}")

    events = load_events(spark, input_path)

    logger.info("Applying interaction weights...")
    weighted = apply_interaction_weights(events)

    logger.info("Applying recency decay...")
    decayed = apply_recency_decay(weighted)

    logger.info("Aggregating edge weights...")
    edges = aggregate_edge_weights(decayed)

    logger.info("Applying reciprocity bonus...")
    edges = apply_reciprocity_bonus(edges)

    logger.info("Normalizing weights per app...")
    edges = normalize_weights(edges)

    edges = edges.withColumn("computed_at", F.current_timestamp())

    logger.info(f"Writing edge weights to {output_path}")
    (
        edges
        .repartition(100, "app")
        .write
        .mode("overwrite")
        .partitionBy("app")
        .parquet(output_path)
    )

    total_edges = edges.count()
    reciprocal = edges.filter(F.col("reciprocity_multiplier") > 1.0).count()
    logger.info(
        f"Done. Total edges: {total_edges:,} | "
        f"Reciprocal: {reciprocal:,} ({reciprocal/total_edges*100:.1f}%)"
    )
    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(args.input, args.output)