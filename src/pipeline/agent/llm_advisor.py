from __future__ import annotations

import json
import logging
from typing import Any

from pipeline.runtime.env import env_flag
from pipeline.runtime.terminal_logging import log_event

_AGENT_SELF_REVIEW_PROMPT = """
You are a data pipeline governance agent performing a structured self-review.
A spec change proposal has passed all automated gates. Decide whether to self-approve it.

## Proposal
{proposal_json}

## Gate Results
{gate_results_json}

## Spec Diff
{diff_json}

## Output Format (JSON only, no prose)
{{
  "should_approve": <true|false>,
  "confidence": <0.0-1.0>,
  "rationale": "<reasoning>"
}}

Rules:
- Approve only if the change is additive, backward-compatible, and evidence is strong
- Reject if the change removes columns, alters semantics ambiguously, or has privacy implications
- confidence reflects how certain you are; err on the side of caution
"""

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


_SELF_REVIEW_SAFE_FALLBACK: dict[str, Any] = {
    "should_approve": False,
    "confidence": 0.0,
    "rationale": "llm_unavailable",
}


def agent_self_review_proposal(
    proposal: dict[str, Any],
    gate_results: dict[str, Any],
    diff: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    """
    LLM self-review for proposals that require human approval.
    Returns {"should_approve": bool, "confidence": float, "rationale": str}.
    Never raises; falls back to should_approve=False on any failure.
    """
    llm_cfg = compiled_plan.get("llm") or {}
    enabled = env_flag("PIPELINE_ENABLE_LLM_ADVISOR", bool(llm_cfg.get("enabled")))
    if not enabled:
        return {**_SELF_REVIEW_SAFE_FALLBACK, "rationale": "llm_disabled"}

    try:
        from pipeline.runtime.llm_runtime import call_llm

        proposal_summary = {
            k: proposal[k]
            for k in (
                "proposal_id",
                "proposal_type",
                "proposal_family",
                "title",
                "expected_impact",
                "risk",
                "impact_class",
                "proposed_change",
                "rationale",
                "items",
                "affected_layers",
                "privacy_impact",
            )
            if k in proposal
        }
        prompt = _AGENT_SELF_REVIEW_PROMPT.format(
            proposal_json=json.dumps(proposal_summary, ensure_ascii=False, indent=2),
            gate_results_json=json.dumps(gate_results, ensure_ascii=False),
            diff_json=json.dumps(diff.get("schema_diff", {}), ensure_ascii=False),
        )
        text = call_llm(prompt, compiled_plan, 10.0)
        raw = text.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw.strip())
        result = {
            "should_approve": bool(parsed.get("should_approve", False)),
            "confidence": float(parsed.get("confidence", 0.0)),
            "rationale": str(parsed.get("rationale", "")),
        }
        log_event(
            logging.INFO,
            "agent_self_review_completed",
            proposal_type=proposal.get("proposal_type"),
            should_approve=result["should_approve"],
            confidence=result["confidence"],
        )
        return result

    except Exception as exc:
        log_event(logging.WARNING, "agent_self_review_failed", error=str(exc))
        return {**_SELF_REVIEW_SAFE_FALLBACK, "rationale": f"llm_error: {exc}"}
