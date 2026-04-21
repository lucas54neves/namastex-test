from __future__ import annotations

import os
from typing import Any


def get_llm_advice(context: dict[str, Any], compiled_plan: dict[str, Any]) -> dict[str, Any]:
    llm_cfg = compiled_plan["llm"]
    enabled = bool(llm_cfg.get("enabled")) and os.getenv("PIPELINE_ENABLE_LLM_ADVISOR") == "1"
    if not enabled:
        return {
            "enabled": False,
            "provider": llm_cfg.get("provider"),
            "status": "disabled",
            "summary": "LLM advisor desabilitado; usando planejamento determinístico.",
            "context_keys": sorted(context.keys()),
        }

    return {
        "enabled": True,
        "provider": llm_cfg.get("provider"),
        "status": "not_implemented",
        "summary": "Interface opcional preparada, mas sem conector LLM configurado neste projeto.",
        "context_keys": sorted(context.keys()),
    }
