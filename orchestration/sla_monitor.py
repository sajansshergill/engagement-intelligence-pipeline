"""
SLA breach alerting — formats Slack-ready payloads and optionally attaches
a Claude-generated explanation via ai_assist.anomaly_explainer.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SLABreach:
    dataset: str
    breach_type: str
    expected: str
    observed: str
    delta: str
    occurred_at: datetime


SLA_TARGETS: dict[str, dict[str, str]] = {
    "fb_engagement_edges": {"freshness": "2h", "completeness": "99%"},
    "ig_story_interactions": {"freshness": "2h", "completeness": "99%"},
    "threads_post_signals": {"freshness": "4h", "completeness": "97%"},
    "wa_msg_graph": {"freshness": "2h", "completeness": "99%"},
    "xapp_session_stitched": {"freshness": "6h", "completeness": "98%"},
}


def format_slack_message(breach: SLABreach, explanation: str | None = None) -> dict[str, Any]:
    text_lines = [
        f"*SLA breach* — `{breach.dataset}`",
        f"Type: *{breach.breach_type}*",
        f"Expected: {breach.expected} | Observed: {breach.observed} | Delta: {breach.delta}",
        f"Time (UTC): {breach.occurred_at.isoformat()}",
    ]
    if explanation:
        text_lines.append(f"_Analyst note:_\n{explanation}")
    return {"text": "\n".join(text_lines)}


def slack_sla_miss_callback(
    dag,
    task_list,
    blocking_task_list,
    slas,
    blocking_tis,
    *args,
    **kwargs,
):
    """
    Airflow sla_miss_callback signature.
    Logs a structured breach record; wire to Slack webhook in production.
    """
    ts = datetime.now(timezone.utc)
    for sla in slas or []:
        breach = SLABreach(
            dataset=getattr(sla, "dag_id", "unknown"),
            breach_type="freshness",
            expected=str(getattr(sla, "sla", "")),
            observed="missed",
            delta="n/a",
            occurred_at=ts,
        )
        payload = format_slack_message(breach)
        logger.warning("SLA miss: %s", json.dumps(payload))
        webhook = os.environ.get("SLACK_WEBHOOK_URL")
        if webhook:
            try:
                import urllib.request

                req = urllib.request.Request(
                    webhook,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                logger.error("Failed to post Slack webhook: %s", e)


def build_breach_from_metrics(
    dataset: str,
    breach_type: str,
    expected: str,
    observed: str,
    delta: str,
    explainer: Callable[..., str] | None = None,
    logs: str = "",
) -> dict[str, Any]:
    breach = SLABreach(
        dataset=dataset,
        breach_type=breach_type,
        expected=expected,
        observed=observed,
        delta=delta,
        occurred_at=datetime.now(timezone.utc),
    )
    explanation = None
    if explainer is not None:
        try:
            explanation = explainer(
                dataset=dataset,
                metric=breach_type,
                observed=observed,
                expected=expected,
                logs=logs,
            )
        except Exception as e:
            explanation = f"(explainer failed: {e})"
    return format_slack_message(breach, explanation=explanation)
