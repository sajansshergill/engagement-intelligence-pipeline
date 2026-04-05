from __future__ import annotations

import os

from ai_assist import anomaly_explainer as ae
from ai_assist import schema_doc_generator as sg


def test_explain_anomaly_without_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    text = ae.explain_anomaly("ds", "freshness", "3h", "2h", logs="lag")
    assert "ds" in text
    assert "freshness" in text


def test_generate_schema_docs_baseline(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    models = tmp_path / "models"
    models.mkdir()
    (models / "demo.sql").write_text("CREATE TABLE demo (id INT);\n", encoding="utf-8")
    md = sg.generate_schema_docs(models_dir=models, use_llm=True)
    assert "demo.sql" in md
