"""
k-Anonymity suppressor — ensures no analytical output is backed by
fewer than k=100 distinct users. Suppresses sub-threshold cohorts
at write time before any downstream consumption.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global k threshold — no cohort smaller than this reaches consumers
K_THRESHOLD = 100


def suppress_low_k_cohorts(
    df: DataFrame,
    group_cols: list[str],
    user_col: str = "user_token",
    k: int = K_THRESHOLD,
) -> tuple[DataFrame, DataFrame]:
    """
    Groups the DataFrame by group_cols, counts distinct users per cohort.
    Cohorts with fewer than k distinct users are suppressed entirely.

    Returns:
        (safe_df, suppressed_df) — callers can audit suppressed cohorts.
    """
    # Count distinct users per cohort
    cohort_counts = df.groupBy(group_cols).agg(
        F.countDistinct(user_col).alias("distinct_users")
    )

    safe_cohorts = cohort_counts.filter(F.col("distinct_users") >= k)
    suppressed_cohorts = cohort_counts.filter(F.col("distinct_users") < k)

    suppressed_count = suppressed_cohorts.count()
    total_cohorts = cohort_counts.count()

    logger.info(
        f"k-Anonymity (k={k}): {total_cohorts:,} cohorts evaluated | "
        f"{suppressed_count:,} suppressed ({suppressed_count/max(total_cohorts,1)*100:.1f}%)"
    )

    # Join back to keep only safe cohorts
    safe_df = df.join(safe_cohorts.select(group_cols), on=group_cols, how="inner")
    suppressed_df = df.join(suppressed_cohorts.select(group_cols), on=group_cols, how="inner")

    return safe_df, suppressed_df


def suppress_aggregation_output(
    df: DataFrame,
    group_cols: list[str],
    agg_col: str,
    user_col: str = "user_token",
    k: int = K_THRESHOLD,
) -> DataFrame:
    """
    For pre-aggregated DataFrames (e.g. metric tables without raw rows),
    suppresses rows where a user_count column is below k.
    Replaces suppressed values with null rather than dropping the row,
    preserving cohort structure for auditing.
    """
    if "user_count" not in df.columns:
        if "distinct_users" in df.columns:
            df = df.withColumnRenamed("distinct_users", "user_count")
        else:
            raise ValueError(
                "suppress_aggregation_output requires a numeric 'user_count' or "
                "'distinct_users' column (pre-aggregated cohort sizes). "
                f"Got columns {list(df.columns)} (user_col={user_col!r} is not used as a substitute)."
            )

    df = df.withColumn(
        agg_col,
        F.when(F.col("user_count") >= k, F.col(agg_col)).otherwise(F.lit(None))
    )

    df = df.withColumn(
        "suppressed",
        F.col("user_count") < k
    )

    suppressed_rows = df.filter(F.col("suppressed")).count()
    logger.info(f"Suppressed {suppressed_rows:,} rows in aggregated output (k={k})")

    return df


def add_suppression_metadata(df: DataFrame, k: int = K_THRESHOLD) -> DataFrame:
    """Tags each row with suppression policy metadata for audit trail."""
    return (
        df
        .withColumn("k_threshold", F.lit(k))
        .withColumn("suppression_policy", F.lit(f"k_anonymity_k{k}"))
        .withColumn("suppression_applied_at", F.current_timestamp())
    )


def run(input_path: str, output_path: str, suppressed_path: str, group_cols: list[str]):
    spark = SparkSession.builder.appName("MetaKAnonSuppressor").getOrCreate()

    logger.info(f"Loading data from {input_path}")
    df = spark.read.parquet(input_path)

    total = df.count()
    logger.info(f"Loaded {total:,} records | Grouping by: {group_cols}")

    safe_df, suppressed_df = suppress_low_k_cohorts(
        df, group_cols=group_cols, user_col="user_token", k=K_THRESHOLD
    )

    safe_df = add_suppression_metadata(safe_df)
    suppressed_df = add_suppression_metadata(suppressed_df)

    safe_count = safe_df.count()
    supp_count = suppressed_df.count()
    logger.info(
        f"Safe records: {safe_count:,} | "
        f"Suppressed records: {supp_count:,} ({supp_count/total*100:.1f}%)"
    )

    logger.info(f"Writing safe records to {output_path}")
    (
        safe_df.repartition(50)
        .write.mode("overwrite")
        .partitionBy("app")
        .parquet(output_path)
    )

    logger.info(f"Writing suppressed records to audit path {suppressed_path}")
    (
        suppressed_df.repartition(10)
        .write.mode("overwrite")
        .parquet(suppressed_path)
    )

    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--suppressed-path", required=True)
    parser.add_argument(
        "--group-cols", nargs="+",
        default=["app", "event_type", "region"],
        help="Columns defining cohort boundaries for k-anonymity"
    )
    args = parser.parse_args()
    run(args.input, args.output, args.suppressed_path, args.group_cols)