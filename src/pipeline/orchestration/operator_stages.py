from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import read_parquet, write_parquet
from pipeline.quality.publication import sanitize_for_publication
from pipeline.quality.quality import (
    ValidationResult,
    summarize_validation_results,
    validate_bronze,
    validate_cross_layer_consistency,
    validate_gold,
    validate_gold_macro,
    validate_silver,
    validate_silver_conversations_llm,
    validate_silver_messages,
)
from pipeline.quality.quarantine import quarantine_bronze_records
from pipeline.runtime.terminal_logging import log_event
from pipeline.transforms.bronze import build_bronze
from pipeline.transforms.conversation_enrichment import build_conversation_enrichment
from pipeline.transforms.gold import build_gold
from pipeline.transforms.gold_macro import build_gold_macro
from pipeline.transforms.silver import build_silver, build_silver_leads

__all__ = [
    "_get_llm_call",
    "_determine_retry_stage",
    "_run_validation_suite",
    "_run_bronze_stage",
    "_run_silver_stage",
    "_run_gold_stage",
    "_run_validation_stage",
]


def _get_llm_call() -> Any:
    from pipeline.runtime.env import env_flag

    if not env_flag("PIPELINE_ENABLE_LLM_AGENT", True):
        return None
    try:
        from pipeline.runtime.llm_runtime import call_llm

        return call_llm
    except Exception:
        return None


def _determine_retry_stage(
    failed_checks: list[dict[str, Any]],
    stage_failure_counts: dict[str, int],
) -> str | None:
    stage_order = ["bronze", "silver", "gold"]
    for stage in stage_order:
        for check in failed_checks:
            layer = str(check.get("layer", ""))
            if layer.startswith(stage) or layer == stage:
                count = stage_failure_counts.get(stage, 0)
                if count < 2:
                    return stage
    return None


def _run_bronze_stage(
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    result = build_bronze(str(paths.raw_bronze_source), compiled_plan=compiled_plan)
    bronze_df = result.df
    log_event(
        logging.INFO,
        "bronze_loaded",
        rows=int(len(bronze_df)),
        schema_ok=result.validation_report.schema_ok,
    )

    quarantine = quarantine_bronze_records(bronze_df, paths.quarantine, compiled_plan)
    bronze_df = quarantine["clean_df"]
    quarantine_report = quarantine["report"]
    log_event(
        logging.INFO,
        "quarantine_completed",
        quarantined_rows=quarantine_report.get("quarantined_rows", 0),
        clean_rows=int(len(bronze_df)),
    )

    bronze_path = paths.bronze / "conversations.parquet"
    write_parquet(bronze_df, bronze_path)

    return {
        "bronze_df": bronze_df,
        "quarantine_report": quarantine_report,
        "bronze_validation": result.validation_report,
    }


def _run_silver_stage(
    bronze_df: pd.DataFrame,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    silver_conversations_llm_path: Path,
) -> dict[str, Any]:
    silver_messages_runtime_df = build_silver(bronze_df, compiled_plan=compiled_plan)
    silver_runtime_df = build_silver_leads(silver_messages_runtime_df)
    silver_df = sanitize_for_publication(silver_runtime_df, "silver")
    silver_messages_df = sanitize_for_publication(silver_messages_runtime_df, "silver_messages")
    log_event(
        logging.INFO,
        "silver_completed",
        silver_rows=int(len(silver_df)),
        silver_messages_rows=int(len(silver_messages_df)),
    )

    existing_enrichment = (
        read_parquet(silver_conversations_llm_path)
        if silver_conversations_llm_path.exists()
        else None
    )
    silver_conversations_llm_runtime_df = build_conversation_enrichment(
        silver_messages_runtime_df,
        compiled_plan=compiled_plan,
        existing_enrichment=existing_enrichment,
    )
    silver_conversations_llm_df = sanitize_for_publication(
        silver_conversations_llm_runtime_df, "silver_conversations_llm"
    )
    log_event(
        logging.INFO,
        "enrichment_completed",
        silver_conversations_llm_rows=int(len(silver_conversations_llm_df)),
    )

    silver_path = paths.silver / "silver_leads.parquet"
    silver_messages_path = paths.silver / "silver_messages.parquet"
    write_parquet(silver_df, silver_path)
    write_parquet(silver_messages_df, silver_messages_path)
    write_parquet(silver_conversations_llm_df, silver_conversations_llm_path)

    return {
        "silver_runtime_df": silver_runtime_df,
        "silver_messages_runtime_df": silver_messages_runtime_df,
        "silver_df": silver_df,
        "silver_messages_df": silver_messages_df,
        "silver_conversations_llm_runtime_df": silver_conversations_llm_runtime_df,
        "silver_conversations_llm_df": silver_conversations_llm_df,
    }


def _run_gold_stage(
    silver_runtime_df: pd.DataFrame,
    silver_messages_runtime_df: pd.DataFrame,
    silver_conversations_llm_runtime_df: pd.DataFrame | None,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    spec: dict[str, Any],
    llm_call: Any,
) -> dict[str, Any]:
    from pipeline.agent.gold_designer import design_gold_columns

    gold_column_plan = design_gold_columns(
        silver_leads_df=silver_runtime_df,
        silver_messages_df=silver_messages_runtime_df,
        spec=spec,
        paths=paths,
        llm_call=llm_call,
        compiled_plan=compiled_plan,
    )
    log_event(
        logging.INFO,
        "gold_column_plan_designed",
        source=gold_column_plan.source,
        column_count=len(gold_column_plan.columns),
    )

    gold_runtime_df = build_gold(
        silver_runtime_df,
        silver_messages_runtime_df,
        silver_conversations_llm_runtime_df,
        compiled_plan=compiled_plan,
        gold_column_plan=gold_column_plan,
    )
    gold_df = sanitize_for_publication(gold_runtime_df, "gold")
    gold_path = paths.gold / "conversations_gold.parquet"
    write_parquet(gold_df, gold_path)
    log_event(logging.INFO, "gold_completed", gold_rows=int(len(gold_df)))

    gold_macro_df = build_gold_macro(gold_df)
    gold_macro_path = paths.gold / "conversations_gold_macro.parquet"
    write_parquet(gold_macro_df, gold_macro_path)
    log_event(logging.INFO, "gold_macro_completed", gold_macro_rows=int(len(gold_macro_df)))

    return {
        "gold_column_plan": gold_column_plan,
        "gold_runtime_df": gold_runtime_df,
        "gold_df": gold_df,
        "gold_macro_df": gold_macro_df,
    }


def _run_validation_suite(
    bronze_df: Any,
    silver_df: Any,
    silver_messages_df: Any,
    silver_conversations_llm_df: Any,
    gold_df: Any,
    gold_macro_df: Any,
    compiled_plan: dict[str, Any],
) -> list[ValidationResult]:
    return (
        validate_bronze(bronze_df, compiled_plan=compiled_plan)
        + validate_silver(silver_df, compiled_plan=compiled_plan)
        + validate_silver_messages(silver_messages_df, compiled_plan=compiled_plan)
        + validate_silver_conversations_llm(
            silver_conversations_llm_df,
            compiled_plan=compiled_plan,
        )
        + validate_gold(gold_df, compiled_plan=compiled_plan)
        + validate_cross_layer_consistency(
            silver_df,
            silver_messages_df,
            gold_df,
            compiled_plan=compiled_plan,
        )
        + validate_gold_macro(gold_macro_df, compiled_plan=compiled_plan)
    )


def _run_validation_stage(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    silver_conversations_llm_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    gold_macro_df: pd.DataFrame | None,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    validation_results = _run_validation_suite(
        bronze_df,
        silver_df,
        silver_messages_df,
        silver_conversations_llm_df,
        gold_df,
        gold_macro_df,
        compiled_plan,
    )
    validation_summary = summarize_validation_results(validation_results)
    return {
        "validation_results": validation_results,
        "validation_summary": validation_summary,
    }
