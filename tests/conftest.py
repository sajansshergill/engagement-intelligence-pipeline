from __future__ import annotations

import pytest

try:
    from pyspark.errors.exceptions.base import PySparkRuntimeError
except ImportError:
    PySparkRuntimeError = RuntimeError  # type: ignore[misc, assignment]


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("engagement-pipeline-tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except (PySparkRuntimeError, OSError, RuntimeError) as e:
        pytest.skip(f"PySpark requires a working Java runtime: {e}")
    yield session
    session.stop()
