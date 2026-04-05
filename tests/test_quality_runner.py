from __future__ import annotations

from pathlib import Path

import pandas as pd

from quality import quality_runner as qr


def test_evaluate_generic_events(tmp_path: Path):
    df = pd.DataFrame(
        {
            "user_token": ["a", "b"],
            "event_type": ["like", "view"],
            "timestamp": pd.to_datetime(["2026-04-05T10:00:00Z", "2026-04-05T10:05:00Z"], utc=True),
            "app": ["fb", "ig"],
        }
    )
    p = tmp_path / "events.parquet"
    df.to_parquet(p, index=False)
    rep = qr.evaluate_dataset("generic_events", p)
    assert rep.row_count == 2
    assert rep.score_pct > 0
