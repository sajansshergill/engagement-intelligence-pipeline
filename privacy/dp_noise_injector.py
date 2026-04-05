"""
Differential privacy noise injector — adds Laplace noise to aggregate
metrics before serving. Prevents inference of individual user behavior
from published statistics.

ε = 0.1 for sensitive demographic cohorts (age, location)
ε = 1.0 for standard engagement metrics
"""

import logging
import math
import numpy as np
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sensitivity — max change one user's data can cause in the aggregate
# For counts: sensitivity = 1 (one user contributes at most 1 to any count)
# For sums: sensitivity = max possible per-user value
DEFAULT_SENSITIVITY = 1.0

# Privacy budgets
EPSILON_SENSITIVE = 0.1   # tight — demographic cuts, location cohorts
EPSILON_STANDARD  = 1.0   # standard — engagement counts, event rates


@dataclass
class PrivacyBudget:
    epsilon: float
    sensitivity: float
    description: str

    @property
    def laplace_scale(self) -> float:
        """b = sensitivity / epsilon"""
        return self.sensitivity / self.epsilon


# Pre-defined budgets
BUDGETS = {
    "sensitive":  PrivacyBudget(EPSILON_SENSITIVE, DEFAULT_SENSITIVITY, "Demographic / location cohorts"),
    "standard":   PrivacyBudget(EPSILON_STANDARD,  DEFAULT_SENSITIVITY, "Standard engagement metrics"),
    "high_util":  PrivacyBudget(2.0, DEFAULT_SENSITIVITY, "Aggregated product metrics (less privacy)"),
}


def laplace_noise(scale: float, size: int = 1) -> np.ndarray:
    """Draw samples from Laplace distribution with given scale."""
    return np.random.laplace(loc=0.0, scale=scale, size=size)


def make_noise_udf(scale: float):
    """
    Spark UDF that adds Laplace noise to a numeric column.
    Each row gets an independent noise draw.
    """
    def _add_noise(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        noise = float(np.random.laplace(0.0, scale))
        # Counts can't go negative — floor at 0
        return max(0.0, round(value + noise, 4))

    return F.udf(_add_noise, DoubleType())


def inject_laplace_noise(
    df: DataFrame,
    metric_cols: list[str],
    budget_key: str = "standard",
    custom_epsilon: float = None,
    custom_sensitivity: float = None,
) -> DataFrame:
    """
    Adds calibrated Laplace noise to specified metric columns.

    Args:
        df:                 Input aggregated DataFrame
        metric_cols:        Numeric columns to privatize
        budget_key:         One of 'sensitive', 'standard', 'high_util'
        custom_epsilon:     Override epsilon if budget_key not sufficient
        custom_sensitivity: Override sensitivity for non-count metrics
    """
    if custom_epsilon is not None:
        budget = PrivacyBudget(
            epsilon=custom_epsilon,
            sensitivity=custom_sensitivity or DEFAULT_SENSITIVITY,
            description="Custom budget"
        )
    else:
        budget = BUDGETS[budget_key]

    scale = budget.laplace_scale
    logger.info(
        f"Injecting Laplace noise | ε={budget.epsilon} | "
        f"sensitivity={budget.sensitivity} | scale={scale:.4f} | "
        f"policy: {budget.description}"
    )

    noise_udf = make_noise_udf(scale)

    for col_name in metric_cols:
        if col_name not in df.columns:
            logger.warning(f"Column {col_name} not found — skipping noise injection")
            continue

        noisy_col = f"{col_name}_dp"
        df = df.withColumn(noisy_col, noise_udf(F.col(col_name).cast(DoubleType())))
        logger.info(f"Noised: {col_name} → {noisy_col} (scale={scale:.4f})")

    df = df.withColumn("dp_epsilon", F.lit(budget.epsilon))
    df = df.withColumn("dp_sensitivity", F.lit(budget.sensitivity))
    df = df.withColumn("dp_mechanism", F.lit("laplace"))
    df = df.withColumn("dp_applied_at", F.current_timestamp())

    return df


def compute_privacy_loss_report(budgets_used: list[PrivacyBudget]) -> dict:
    """
    Basic privacy accounting — sequential composition theorem.
    Total epsilon = sum of individual epsilons (worst case).
    """
    total_epsilon = sum(b.epsilon for b in budgets_used)
    return {
        "total_epsilon": total_epsilon,
        "num_queries": len(budgets_used),
        "mechanism": "laplace_sequential_composition",
        "warning": "High risk" if total_epsilon > 5.0 else "Within acceptable range",
    }


def run(
    input_path: str,
    output_path: str,
    metric_cols: list[str],
    budget_key: str = "standard",
):
    spark = SparkSession.builder.appName("MetaDPNoiseInjector").getOrCreate()

    logger.info(f"Loading aggregated data from {input_path}")
    df = spark.read.parquet(input_path)

    logger.info(f"Injecting DP noise into columns: {metric_cols}")
    noised_df = inject_laplace_noise(df, metric_cols, budget_key=budget_key)

    logger.info(f"Writing privatized output to {output_path}")
    (
        noised_df
        .repartition(50)
        .write
        .mode("overwrite")
        .partitionBy("app")
        .parquet(output_path)
    )

    budget = BUDGETS[budget_key]
    report = compute_privacy_loss_report([budget])
    logger.info(f"Privacy loss report: {report}")

    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metric-cols", nargs="+", required=True)
    parser.add_argument(
        "--budget", default="standard",
        choices=["sensitive", "standard", "high_util"],
        help="Privacy budget to apply"
    )
    args = parser.parse_args()
    run(args.input, args.output, args.metric_cols, args.budget)