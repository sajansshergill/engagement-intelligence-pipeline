"""
Schema documentation generator — reads Hive-style DDL under models/ and
produces Markdown documentation, optionally enriched via Anthropic Claude.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = REPO_ROOT / "models"
DEFAULT_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")


def load_ddl_files(models_dir: Path | None = None) -> dict[str, str]:
    root = models_dir or MODELS_DIR
    if not root.exists():
        return {}
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(root.glob("*.sql"))}


def _baseline_markdown(ddl_by_file: dict[str, str]) -> str:
    parts = ["# Engagement intelligence — table schemas\n"]
    for name, ddl in ddl_by_file.items():
        parts.append(f"## `{name}`\n\n```sql\n{ddl.strip()}\n```\n")
    return "\n".join(parts)


def generate_schema_docs(models_dir: Path | None = None, use_llm: bool = True) -> str:
    ddl_by_file = load_ddl_files(models_dir)
    if not ddl_by_file:
        return "# Schemas\n\nNo `.sql` files found in models/.\n"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not use_llm or not api_key:
        return _baseline_markdown(ddl_by_file)

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        bundle = "\n\n".join(f"--- {n} ---\n{b}" for n, b in ddl_by_file.items())
        prompt = f"""You are a principal data engineer. Given these Hive DDL files, write concise documentation:
- One short paragraph per table explaining purpose and grain
- Bullet list of key columns with business meaning
- Note partition keys and retention implications

DDL bundle:
{bundle[:120000]}
"""
        response = client.messages.create(
            model=DEFAULT_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.exception("Claude schema_doc_generator failed")
        return _baseline_markdown(ddl_by_file) + f"\n\n<!-- LLM enrichment failed: {e} -->\n"


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "data/docs/schema_generated.md")
    parser.add_argument("--no-llm", action="store_true")
    args = parser.parse_args()
    md = generate_schema_docs(use_llm=not args.no_llm)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
