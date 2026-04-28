from __future__ import annotations

import json
import logging
from typing import Any

from pipeline.runtime.env import env_flag
from pipeline.runtime.terminal_logging import log_event

_LLM_ADVISOR_PROMPT = """
You are a data pipeline planning advisor.
Review the following pipeline planning context and return a structured recommendation.

## Context
{context_json}

## Output Format (JSON only, no prose)
{{
  "priority_proposals": ["<proposal_type>", ...],
  "deferred_proposals": ["<proposal_type>", ...],
  "risk_flags": ["<free text>", ...],
  "summary": "<one sentence>"
}}

Rules:
- priority_proposals: proposal_types that should be evaluated first this cycle
- deferred_proposals: proposal_types that should be deferred (closed without action) this cycle
- A proposal_type can appear in only one list or in neither
- Return only proposal_types that appear in the context; do not invent new ones
- risk_flags: free-text warnings about observed risks (max 3)
"""


def get_llm_advice(context: dict[str, Any], compiled_plan: dict[str, Any]) -> dict[str, Any]:
    """
    Return LLM Advisor recommendations for the proposal set.
    Never raises an exception. Always includes 'status' in return.
    """
    llm_cfg = compiled_plan.get("llm") or {}
    enabled = env_flag("PIPELINE_ENABLE_LLM_ADVISOR", bool(llm_cfg.get("enabled")))
    provider = llm_cfg.get("provider")
    context_keys = sorted(context.keys())

    if not enabled:
        return {
            "enabled": False,
            "provider": provider,
            "status": "disabled",
            "priority_proposals": [],
            "deferred_proposals": [],
            "risk_flags": [],
            "summary": "LLM advisor disabled; using deterministic planning.",
            "context_keys": context_keys,
        }

    try:
        from pipeline.runtime.llm_runtime import call_llm

        proposal_summaries = [
            {
                "proposal_type": p.get("proposal_type"),
                "proposal_family": p.get("proposal_family"),
                "expected_impact": p.get("expected_impact"),
            }
            for p in context.get("proposals", [])
        ]
        context_payload = {
            "observed_columns": context.get("observed_columns", []),
            "observed_metadata_fields": context.get("observed_metadata_fields", []),
            "detected_contexts": [
                c.get("context_type") for c in context.get("detected_contexts", [])
            ],
            "proposals": proposal_summaries,
        }
        context_json = json.dumps(context_payload, ensure_ascii=False, indent=2)
        prompt = _LLM_ADVISOR_PROMPT.format(context_json=context_json)

        text = call_llm(prompt, compiled_plan, 15.0)
        raw = text.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw.strip())

        return {
            "enabled": True,
            "provider": provider,
            "status": "ok",
            "priority_proposals": list(parsed.get("priority_proposals", [])),
            "deferred_proposals": list(parsed.get("deferred_proposals", [])),
            "risk_flags": list(parsed.get("risk_flags", [])),
            "summary": str(parsed.get("summary", "")),
            "context_keys": context_keys,
        }

    except Exception as exc:
        log_event(logging.WARNING, "llm_advisor_failed", error=str(exc))
        return {
            "enabled": True,
            "provider": provider,
            "status": "llm_failed",
            "priority_proposals": [],
            "deferred_proposals": [],
            "risk_flags": [],
            "summary": "",
            "context_keys": context_keys,
        }
