from __future__ import annotations

from pipeline.compiler import get_default_compiled_plan
from pipeline.llm_advisor import get_llm_advice


def test_llm_advice_defaults_to_disabled() -> None:
    advice = get_llm_advice({"changes": []}, get_default_compiled_plan())
    assert advice["enabled"] is False
    assert advice["status"] == "disabled"
