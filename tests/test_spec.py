from __future__ import annotations

from pipeline.spec import default_pipeline_spec, validate_pipeline_spec


def test_default_pipeline_spec_is_valid() -> None:
    spec = default_pipeline_spec()
    validate_pipeline_spec(spec)
    assert spec["gold"]["segmentation"]["personas"]["default"] == "lead_frio"
    assert "quoted_price" in spec["silver"]["derived_fields"]
    assert spec["agent"]["planner"]["proposal_defaults"]["default_status"] == "proposed"
