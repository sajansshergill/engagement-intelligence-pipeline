"""
Anomaly explainer — sends SLA / quality context to Anthropic Claude when
ANTHROPIC_API_KEY is set; otherwise returns a deterministic local template.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")


def explain_anomaly(
    dataset: str,
    metric: str,
    observed: str,
    expected: str,
    logs: str = "",
) -> str:
    """
    Produce a short plain-English explanation for Slack / dashboards.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return (
            f"No API key configured. Dataset `{dataset}` breached `{metric}` "
            f"(observed {observed} vs expected {expected}). "
            f"Check upstream task duration, Kafka lag, and Spark executor metrics. "
            f"Log excerpt: {logs[:500]!r}"
        )

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        prompt = f"""Dataset: {dataset}
Quality / SLA metric: {metric}
Expected: {expected}
Observed: {observed}
Recent pipeline logs:
{logs[:8000]}

In 2-3 sentences, state the most likely root cause and one concrete next step for the on-call engineer."""

        response = client.messages.create(
            model=DEFAULT_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.exception("Claude explain_anomaly failed")
        return f"(LLM call failed: {e}) Fallback: investigate {dataset} / {metric}."
