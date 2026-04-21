from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from pipeline.approval import is_proposal_approved
from pipeline.compiler import compile_pipeline_spec
from pipeline.config import PipelinePaths
from pipeline.io import read_json, write_json
from pipeline.llm_advisor import get_llm_advice
from pipeline.spec import load_pipeline_spec, save_pipeline_spec


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _discover_metadata_fields(bronze_df: pd.DataFrame) -> list[str]:
    discovered: set[str] = set()
    for value in bronze_df["metadata"].dropna().head(200):
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict):
            discovered.update(str(key) for key in parsed.keys())
    return sorted(discovered)


def plan_pipeline_spec(paths: PipelinePaths) -> dict[str, Any]:
    spec = load_pipeline_spec(paths.pipeline_spec)
    bronze_df = pd.read_parquet(paths.raw_bronze_source)
    observed_columns = sorted(bronze_df.columns.tolist())
    observed_metadata_fields = _discover_metadata_fields(bronze_df)

    proposed_spec = copy.deepcopy(spec)
    missing_columns = sorted(set(observed_columns) - set(spec["bronze"]["required_columns"]))
    missing_metadata_fields = sorted(
        set(observed_metadata_fields) - set(spec["silver"].get("metadata_fields", []))
    )

    proposal_id = f"proposal_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
    changes: list[dict[str, Any]] = []

    if missing_columns:
        changes.append(
            {
                "type": "bronze_required_columns_addition",
                "items": missing_columns,
                "safe_auto_apply": False,
            }
        )
        proposed_spec["bronze"]["required_columns"] = sorted(
            set(proposed_spec["bronze"]["required_columns"]) | set(missing_columns)
        )

    if missing_metadata_fields:
        changes.append(
            {
                "type": "silver_metadata_fields_addition",
                "items": missing_metadata_fields,
                "safe_auto_apply": True,
            }
        )
        proposed_spec["silver"]["metadata_fields"] = sorted(
            set(proposed_spec["silver"]["metadata_fields"]) | set(missing_metadata_fields)
        )

    requires_approval = any(not change["safe_auto_apply"] for change in changes)
    approved = is_proposal_approved(paths, proposal_id) if requires_approval else False
    auto_apply_safe = bool(spec["agent"]["planner"].get("auto_apply_safe_updates", False))
    should_apply = bool(changes) and (
        (not requires_approval and auto_apply_safe) or (requires_approval and approved)
    )

    if should_apply:
        save_pipeline_spec(proposed_spec, paths.pipeline_spec)
        history = read_json(paths.spec_history, default={"changes": []})
        history.setdefault("changes", []).append(
            {
                "proposal_id": proposal_id,
                "applied_at_utc": _utc_now_iso(),
                "changes": changes,
            }
        )
        write_json(history, paths.spec_history)
        active_spec = proposed_spec
    else:
        active_spec = spec

    compiled_plan = compile_pipeline_spec(active_spec)
    llm_advice = get_llm_advice(
        {
            "observed_columns": observed_columns,
            "observed_metadata_fields": observed_metadata_fields,
            "changes": changes,
        },
        compiled_plan,
    )

    report = {
        "generated_at_utc": _utc_now_iso(),
        "proposal_id": proposal_id,
        "observed_columns": observed_columns,
        "observed_metadata_fields": observed_metadata_fields,
        "changes": changes,
        "requires_approval": requires_approval,
        "approved": approved,
        "applied": should_apply,
        "llm_advice": llm_advice,
    }
    write_json(report, paths.monitoring / "latest_plan_report.json")
    return report
