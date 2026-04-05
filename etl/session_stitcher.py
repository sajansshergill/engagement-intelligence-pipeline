"""
Session stitcher — joins cross-app engagement events on a shared pseudonymous
user token to build unified session timelines across FB, IG, Threads, and WA.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gap threshold — if >30 min between events, start a new session
SESSION_GAP_MINUTES = 30
APPS = ["fb", "ig", "threads", "wa"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MetaSessionStitcher")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_validated_events(spark: SparkSession, path: str) -> DataFrame:
    """Load validated events from Hive staging or parquet path."""
    return (
        spark.read
        .option("mergeSchema", "true")
        .parquet(path)
        .select(
            "user_token", "event_type", "timestamp",
            "app", "session_id",
            F.col("metadata.region").alias("region"),
            F.col("metadata.platform").alias("platform"),
            F.col("metadata.content_id").alias("content_id"),
        )
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .filter(F.col("user_token").isNotNull())
        .filter(F.col("timestamp").isNotNull())
    )


def assign_global_sessions(df: DataFrame) -> DataFrame:
    """
    Assigns a global_session_id across all apps per user.
    A new session starts when the gap between consecutive events
    exceeds SESSION_GAP_MINUTES — regardless of which app they came from.
    """
    user_time_window = Window.partitionBy("user_token").orderBy("timestamp")

    df = df.withColumn(
        "prev_timestamp",
        F.lag("timestamp").over(user_time_window)
    )

    df = df.withColumn(
        "gap_minutes",
        (
            F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")
        ) / 60
    )

    # Flag start of new session: first event or gap > threshold
    df = df.withColumn(
        "is_new_session",
        F.when(
            F.col("prev_timestamp").isNull() |
            (F.col("gap_minutes") > SESSION_GAP_MINUTES),
            1
        ).otherwise(0)
    )

    # Cumulative sum of session starts → unique session index per user
    df = df.withColumn(
        "session_index",
        F.sum("is_new_session").over(
            user_time_window.rowsBetween(Window.unboundedPreceding, 0)
        )
    )

    # Build global session ID
    df = df.withColumn(
        "global_session_id",
        F.concat_ws("_", "user_token", "session_index")
    )

    return df.drop("prev_timestamp", "gap_minutes", "is_new_session", "session_index")


def build_session_summary(df: DataFrame) -> DataFrame:
    """
    Aggregates event-level data into one row per global session.
    Captures: apps visited, event sequence, duration, engagement score.
    """
    session_window = Window.partitionBy("global_session_id").orderBy("timestamp")

    df = df.withColumn("event_rank", F.rank().over(session_window))

    summary = df.groupBy("global_session_id", "user_token").agg(
        F.min("timestamp").alias("session_start"),
        F.max("timestamp").alias("session_end"),
        F.count("*").alias("total_events"),
        F.collect_set("app").alias("apps_visited"),
        F.collect_list(
            F.struct("event_rank", "app", "event_type", "timestamp", "content_id")
        ).alias("event_sequence"),
        F.first("region").alias("region"),
        F.first("platform").alias("platform"),
        F.countDistinct("app").alias("app_count"),
    )

    # Session duration in seconds
    summary = summary.withColumn(
        "duration_seconds",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
    )

    # Cross-app flag — session touched more than one app
    summary = summary.withColumn(
        "is_cross_app",
        F.col("app_count") > 1
    )

    # Simple engagement score: log(events) * app_count * duration weight
    summary = summary.withColumn(
        "engagement_score",
        F.round(
            F.log(F.col("total_events") + 1) *
            F.col("app_count") *
            F.log(F.col("duration_seconds") + 1),
            4
        )
    )

    return summary.withColumn("stitched_at", F.current_timestamp())


def run(input_path: str, output_path: str):
    spark = get_spark()
    logger.info(f"Loading validated events from {input_path}")

    events = load_validated_events(spark, input_path)
    logger.info(f"Loaded {events.count():,} events")

    logger.info("Assigning global sessions across apps...")
    with_sessions = assign_global_sessions(events)

    logger.info("Building session summary...")
    session_summary = build_session_summary(with_sessions)

    logger.info(f"Writing session summary to {output_path}")
    (
        session_summary
        .repartition(50)
        .write
        .mode("overwrite")
        .partitionBy("region")
        .parquet(output_path)
    )

    total_sessions = session_summary.count()
    cross_app = session_summary.filter(F.col("is_cross_app")).count()
    logger.info(
        f"Done. Total sessions: {total_sessions:,} | "
        f"Cross-app sessions: {cross_app:,} ({cross_app/total_sessions*100:.1f}%)"
    )

    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to validated events parquet")
    parser.add_argument("--output", required=True, help="Output path for session summary")
    args = parser.parse_args()
    run(args.input, args.output)