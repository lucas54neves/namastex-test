from __future__ import annotations

from pathlib import Path

from pipeline.runtime.langfuse_bootstrap import (
    ensure_langfuse_prompt,
    run_langfuse_prompt_bootstrap,
    runtime_langfuse_prompt_template,
)


def test_runtime_langfuse_prompt_template_contains_required_variables() -> None:
    template = runtime_langfuse_prompt_template()

    assert "{{allowed_values_json}}" in template
    assert "{{payload_json}}" in template
    assert "Return valid JSON only" in template
    assert "privacy-safe" in template


def test_ensure_langfuse_prompt_is_noop_when_prompt_already_matches(monkeypatch) -> None:
    prompt_template = runtime_langfuse_prompt_template()

    monkeypatch.setattr(
        "pipeline.runtime.langfuse_bootstrap.fetch_prompt_by_label",
        lambda **kwargs: {
            "type": "text",
            "version": 7,
            "prompt": prompt_template,
        },
    )
    monkeypatch.setattr(
        "pipeline.runtime.langfuse_bootstrap.create_prompt_version",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("create should not be called")),
    )

    result = ensure_langfuse_prompt(
        base_url="http://langfuse-web:3000",
        public_key="pk",
        secret_key="sk",
        prompt_name="conversation-enrichment-v1",
        prompt_label="production",
        prompt_template=prompt_template,
    )

    assert result["status"] == "unchanged"
    assert result["prompt_version"] == 7


def test_ensure_langfuse_prompt_creates_new_version_when_prompt_differs(monkeypatch) -> None:
    prompt_template = runtime_langfuse_prompt_template()

    monkeypatch.setattr(
        "pipeline.runtime.langfuse_bootstrap.fetch_prompt_by_label",
        lambda **kwargs: {
            "type": "text",
            "version": 2,
            "prompt": "older prompt",
        },
    )
    monkeypatch.setattr(
        "pipeline.runtime.langfuse_bootstrap.create_prompt_version",
        lambda **kwargs: {
            "type": "text",
            "version": 3,
            "prompt": prompt_template,
        },
    )

    result = ensure_langfuse_prompt(
        base_url="http://langfuse-web:3000",
        public_key="pk",
        secret_key="sk",
        prompt_name="conversation-enrichment-v1",
        prompt_label="production",
        prompt_template=prompt_template,
    )

    assert result["status"] == "updated"
    assert result["prompt_version"] == 3


def test_run_langfuse_prompt_bootstrap_skips_when_required_env_is_missing(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_LANGFUSE_BOOTSTRAP_ENABLED", "1")
    monkeypatch.delenv("LANGFUSE_BASE_URL", raising=False)
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
    monkeypatch.delenv("PIPELINE_LANGFUSE_PROMPT_NAME", raising=False)

    result = run_langfuse_prompt_bootstrap()

    assert result["status"] == "skipped"
    assert "missing_required_env" in str(result["detail"])


def test_compose_assets_define_required_langfuse_stack_roles() -> None:
    compose_text = Path("docker-compose.yml").read_text(encoding="utf-8")

    for marker in (
        "langfuse-web:",
        "langfuse-worker:",
        "langfuse-bootstrap:",
        "pipeline:",
        "postgres:",
        "clickhouse:",
        "redis:",
        "minio:",
        "LANGFUSE_INIT_PROJECT_PUBLIC_KEY",
        "LANGFUSE_INIT_PROJECT_SECRET_KEY",
        "PIPELINE_LANGFUSE_PROMPT_NAME",
        "PIPELINE_LANGFUSE_PROMPT_LABEL",
        "PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK",
        "http://langfuse-web:3000",
    ):
        assert marker in compose_text


def test_env_example_documents_compose_contract() -> None:
    env_example = Path(".env.example").read_text(encoding="utf-8")

    for marker in (
        "PIPELINE_ENABLE_LANGFUSE=",
        "PIPELINE_LANGFUSE_PROMPT_NAME=",
        "PIPELINE_LANGFUSE_PROMPT_LABEL=",
        "LANGFUSE_INIT_PROJECT_PUBLIC_KEY=",
        "LANGFUSE_INIT_PROJECT_SECRET_KEY=",
        "LANGFUSE_INIT_USER_EMAIL=",
        "PIPELINE_LANGFUSE_BOOTSTRAP_ENABLED=",
    ):
        assert marker in env_example
