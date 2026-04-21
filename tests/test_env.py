from __future__ import annotations

import os
from pathlib import Path

from pipeline.env import load_dotenv_file


def test_load_dotenv_file_populates_missing_values(tmp_path: Path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        'PIPELINE_ENABLE_LLM_ENRICHMENT="1"\nOPENAI_API_KEY="openai-test-key"\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("PIPELINE_ENABLE_LLM_ENRICHMENT", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    loaded = load_dotenv_file(dotenv_path)

    assert loaded is True
    assert os.environ["PIPELINE_ENABLE_LLM_ENRICHMENT"] == "1"
    assert os.environ["OPENAI_API_KEY"] == "openai-test-key"


def test_load_dotenv_file_does_not_override_existing_env(tmp_path: Path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text('PIPELINE_ALERT_SUPPRESSION_MINUTES="30"\n', encoding="utf-8")
    monkeypatch.setenv("PIPELINE_ALERT_SUPPRESSION_MINUTES", "5")

    load_dotenv_file(dotenv_path)

    assert os.environ["PIPELINE_ALERT_SUPPRESSION_MINUTES"] == "5"
