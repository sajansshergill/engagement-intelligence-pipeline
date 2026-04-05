from __future__ import annotations

from orchestration.sla_monitor import SLABreach, build_breach_from_metrics, format_slack_message


def test_format_slack_message():
    from datetime import datetime, timezone

    b = SLABreach(
        dataset="fb_engagement_edges",
        breach_type="freshness",
        expected="2h",
        observed="3h",
        delta="1h",
        occurred_at=datetime(2026, 4, 5, tzinfo=timezone.utc),
    )
    payload = format_slack_message(b, explanation="Kafka lag elevated.")
    assert "fb_engagement_edges" in payload["text"]
    assert "Kafka lag" in payload["text"]


def test_build_breach_with_stub_explainer():
    def explainer(**kwargs):
        return "stub"

    payload = build_breach_from_metrics(
        dataset="wa_msg_graph",
        breach_type="completeness",
        expected="99%",
        observed="96%",
        delta="3pp",
        explainer=explainer,
    )
    assert "stub" in payload["text"]
