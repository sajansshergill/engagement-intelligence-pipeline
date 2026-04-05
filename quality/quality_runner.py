"""
Batch data quality runner — completeness, consistency, and timeliness checks
over pipeline outputs (Parquet samples). Uses Great Expectations when the
legacy PandasDataset API is available; otherwise falls back to equivalent
pandas validations so CI stays green across GE versions.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_ge_pandas_dataset():
    try:
        from great_expectations.dataset import PandasDataset

        return PandasDataset
    except Exception:
        return None


PandasDataset = _load_ge_pandas_dataset()


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: Any = None


@dataclass
class DatasetQualityReport:
    dataset: str
    path: str
    row_count: int
    score_pct: float
    checks: list[CheckResult]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["checks"] = [asdict(c) for c in self.checks]
        return d


def _read_sample_parquet(path: Path, max_rows: int = 50_000) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset path: {path}")
    return pd.read_parquet(path, engine="pyarrow").head(max_rows)


def _run_ge_checks(df: pd.DataFrame, required_cols: list[str]) -> list[CheckResult]:
    results: list[CheckResult] = []
    if PandasDataset is None:
        return results
    pds = PandasDataset(df)
    for col in required_cols:
        if col in df.columns:
            exp = pds.expect_column_values_to_not_be_null(col)
            results.append(
                CheckResult(
                    name=f"ge_not_null:{col}",
                    passed=bool(exp.success),
                    detail=exp.result,
                )
            )
    return results


def _run_pandas_checks(
    df: pd.DataFrame,
    required_cols: list[str],
    ts_col: str | None,
    max_age_hours: float,
) -> list[CheckResult]:
    checks: list[CheckResult] = []
    for col in required_cols:
        if col not in df.columns:
            checks.append(CheckResult(name=f"column_present:{col}", passed=False, detail="missing"))
            continue
        nulls = int(df[col].isna().sum())
        checks.append(
            CheckResult(
                name=f"not_null:{col}",
                passed=nulls == 0,
                detail={"null_count": nulls},
            )
        )

    if ts_col and ts_col in df.columns:
        now = datetime.now(timezone.utc)
        ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        latest = ts.max()
        if pd.isna(latest):
            checks.append(CheckResult(name="timeliness:latest_ts", passed=False, detail="no valid ts"))
        else:
            delta_h = (now - latest.to_pydatetime()).total_seconds() / 3600
            checks.append(
                CheckResult(
                    name="timeliness:freshness_hours",
                    passed=delta_h <= max_age_hours,
                    detail={"hours_behind": round(delta_h, 3), "max_allowed": max_age_hours},
                )
            )

    return checks


DATASET_SPECS: dict[str, dict[str, Any]] = {
    "fb_engagement_edges": {
        "path_key": "FB_EDGES_PATH",
        "default_path": "data/sample/fb_engagement_edges",
        "required_cols": ["user_token", "target_user_token", "app"],
        "ts_col": "last_interaction_at",
        "max_age_hours": 48,
    },
    "xapp_session_stitched": {
        "path_key": "XAPP_SESSION_PATH",
        "default_path": "data/sample/xapp_session_stitched",
        "required_cols": ["global_session_id", "user_token", "session_start"],
        "ts_col": "session_end",
        "max_age_hours": 168,
    },
    "generic_events": {
        "path_key": "EVENTS_PATH",
        "default_path": "data/sample/validated_events",
        "required_cols": ["user_token", "event_type", "timestamp", "app"],
        "ts_col": "timestamp",
        "max_age_hours": 48,
    },
}


def evaluate_dataset(name: str, path: Path) -> DatasetQualityReport:
    spec = DATASET_SPECS[name]
    df = _read_sample_parquet(path)

    required: list[str] = spec["required_cols"]
    ts_col: str | None = spec.get("ts_col")
    max_age: float = float(spec.get("max_age_hours", 72))

    checks: list[CheckResult] = []
    checks.extend(_run_ge_checks(df, required))
    checks.extend(_run_pandas_checks(df, required, ts_col=ts_col, max_age_hours=max_age))

    passed = sum(1 for c in checks if c.passed)
    score = (passed / len(checks) * 100) if checks else 100.0

    return DatasetQualityReport(
        dataset=name,
        path=str(path),
        row_count=len(df),
        score_pct=round(score, 2),
        checks=checks,
    )


def resolve_path(name: str) -> Path:
    spec = DATASET_SPECS[name]
    env = spec["path_key"]
    override = os.environ.get(env)
    if override:
        return Path(override)
    return REPO_ROOT / spec["default_path"]


def run(datasets: list[str], json_out: Path | None = None) -> list[DatasetQualityReport]:
    reports: list[DatasetQualityReport] = []
    for name in datasets:
        path = resolve_path(name)
        logger.info("Evaluating %s @ %s", name, path)
        if not path.exists():
            logger.warning("Skipping %s — path not found: %s", name, path)
            reports.append(
                DatasetQualityReport(
                    dataset=name,
                    path=str(path),
                    row_count=0,
                    score_pct=0.0,
                    checks=[
                        CheckResult(
                            name="dataset_path_exists",
                            passed=False,
                            detail="file or directory not found",
                        )
                    ],
                )
            )
            continue
        reports.append(evaluate_dataset(name, path))

    for r in reports:
        status = "PASS" if r.score_pct >= 97 else "WARN"
        logger.info("%s %s — score=%s%% rows=%s", status, r.dataset, r.score_pct, r.row_count)

    if json_out:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps([rep.to_dict() for rep in reports], indent=2))
        logger.info("Wrote report %s", json_out)

    return reports


def main():
    parser = argparse.ArgumentParser(description="Run batch quality checks on Parquet datasets.")
    parser.add_argument(
        "--dataset",
        nargs="+",
        default=["generic_events"],
        help=f"One or more of: {', '.join(DATASET_SPECS)} or 'all'",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Optional path to write JSON report",
    )
    args = parser.parse_args()

    names = list(args.dataset)
    if names == ["all"]:
        names = list(DATASET_SPECS.keys())

    unknown = [n for n in names if n not in DATASET_SPECS]
    if unknown:
        raise SystemExit(f"Unknown dataset(s): {unknown}")

    run(names, json_out=args.json_out)


if __name__ == "__main__":
    main()
