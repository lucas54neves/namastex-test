from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.quality.schema_drift import (
    DRIFT_CLASSES,
    PROPAGATION_POLICIES,
    DriftEvent,
    build_drift_report,
    classify_bronze_columns,
    classify_category_drift,
    classify_metadata_keys,
    classify_type_mismatch,
    namespaced_passthrough_name,
    resolve_policy,
    write_drift_report,
)
from pipeline.runtime.spec import default_pipeline_spec
from pipeline.transforms.bronze import build_bronze, detect_bronze_schema_drift
from pipeline.transforms.gold import build_gold
from pipeline.transforms.gold_macro import build_gold_macro
from pipeline.transforms.silver import (
    apply_bronze_passthrough_namespace,
    build_silver,
    build_silver_leads,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bronze_rows(extra_columns: dict[str, object] | None = None) -> list[dict[str, object]]:
    base = {
        "message_id": "m1",
        "conversation_id": "conv_1",
        "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
        "sender_phone": "+5511999999999",
        "sender_name": "Ana",
        "message_body": "quero cotacao",
        "campaign_id": "camp_1",
        "agent_id": "agent_1",
        "direction": "inbound",
        "message_type": "text",
        "status": "read",
        "channel": "whatsapp",
        "conversation_outcome": "em_negociacao",
        "metadata": json.dumps(
            {
                "device": "iphone",
                "city": "SP",
                "state": "SP",
                "response_time_sec": 60,
                "is_business_hours": True,
                "lead_source": "google",
            }
        ),
    }
    if extra_columns:
        base.update(extra_columns)
    return [base]


def _write_bronze(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    path = tmp_path / "bronze.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Pure classification & policy resolution
# ---------------------------------------------------------------------------


class TestPolicyResolution:
    def test_known_policy_taxonomies(self) -> None:
        assert "passthrough_with_alert" in PROPAGATION_POLICIES
        assert "block" in PROPAGATION_POLICIES
        assert "missing_required" in DRIFT_CLASSES
        assert "category_drift" in DRIFT_CLASSES

    def test_missing_required_resolves_to_block(self) -> None:
        contract = default_pipeline_spec()
        assert resolve_policy("missing_required", "campaign_id", contract) == "block"

    def test_unknown_top_level_default(self) -> None:
        contract = default_pipeline_spec()
        assert resolve_policy("unknown", "lead_segment_hint", contract) == "passthrough_with_alert"

    def test_unknown_metadata_default(self) -> None:
        contract = default_pipeline_spec()
        assert (
            resolve_policy("unknown", "utm_term", contract, scope="metadata_key")
            == "passthrough_with_alert"
        )

    def test_per_column_override_takes_precedence(self) -> None:
        contract = default_pipeline_spec()
        contract["bronze"]["column_policies"] = {"agent_team": "passthrough_silent"}
        assert resolve_policy("unknown", "agent_team", contract) == "passthrough_silent"

    def test_optional_known_resolves_to_passthrough_with_alert(self) -> None:
        contract = default_pipeline_spec()
        assert (
            resolve_policy("optional_known", "lead_segment_hint", contract)
            == "passthrough_with_alert"
        )


class TestClassifyBronzeColumns:
    def test_clean_input_emits_no_events(self) -> None:
        contract = default_pipeline_spec()
        observed = list(contract["bronze"]["required_columns"])
        events = classify_bronze_columns(observed, contract)
        assert events == []

    def test_unknown_top_level_column_detected(self) -> None:
        contract = default_pipeline_spec()
        observed = list(contract["bronze"]["required_columns"]) + ["lead_segment_hint"]
        events = classify_bronze_columns(observed, contract)
        kinds = {(e.drift_class, e.column) for e in events}
        assert ("unknown", "lead_segment_hint") in kinds
        assert all(e.applied_policy in PROPAGATION_POLICIES for e in events)

    def test_optional_known_emits_optional_event(self) -> None:
        contract = default_pipeline_spec()
        contract["bronze"]["optional_columns"] = ["lead_segment_hint"]
        observed = list(contract["bronze"]["required_columns"]) + ["lead_segment_hint"]
        events = classify_bronze_columns(observed, contract)
        kinds = {(e.drift_class, e.column) for e in events}
        assert ("optional_known", "lead_segment_hint") in kinds
        assert ("unknown", "lead_segment_hint") not in kinds

    def test_missing_required_emits_block_policy(self) -> None:
        contract = default_pipeline_spec()
        observed = [c for c in contract["bronze"]["required_columns"] if c != "campaign_id"]
        events = classify_bronze_columns(observed, contract)
        missing = [e for e in events if e.drift_class == "missing_required"]
        assert len(missing) == 1
        assert missing[0].column == "campaign_id"
        assert missing[0].applied_policy == "block"

    def test_renamed_column_yields_missing_and_unknown(self) -> None:
        contract = default_pipeline_spec()
        observed = [c for c in contract["bronze"]["required_columns"] if c != "sender_name"] + [
            "sender_display_name"
        ]
        events = classify_bronze_columns(observed, contract)
        kinds = {(e.drift_class, e.column) for e in events}
        assert ("missing_required", "sender_name") in kinds
        assert ("unknown", "sender_display_name") in kinds


class TestMetadataAndCategoryClassification:
    def test_metadata_keys_classified(self) -> None:
        contract = default_pipeline_spec()
        events = classify_metadata_keys(["device", "utm_term"], contract)
        unknown = [e for e in events if e.drift_class == "unknown"]
        assert {e.column for e in unknown} == {"utm_term"}

    def test_category_drift_detected(self) -> None:
        contract = default_pipeline_spec()
        event = classify_category_drift("direction", ["inbound", "system"], contract)
        assert event is not None
        assert event.drift_class == "category_drift"
        assert "system" in event.detail["unexpected_values_masked"]

    def test_category_drift_returns_none_when_clean(self) -> None:
        contract = default_pipeline_spec()
        event = classify_category_drift("direction", ["inbound", "outbound"], contract)
        assert event is None

    def test_type_mismatch_detected(self) -> None:
        contract = default_pipeline_spec()
        event = classify_type_mismatch("timestamp", "datetime64[ns]", "float64", contract)
        assert event is not None
        assert event.drift_class == "type_mismatch"
        assert event.detail["expected_dtype"] == "datetime64[ns]"

    def test_type_mismatch_returns_none_when_equal(self) -> None:
        contract = default_pipeline_spec()
        event = classify_type_mismatch("timestamp", "datetime64[ns]", "datetime64[ns]", contract)
        assert event is None


# ---------------------------------------------------------------------------
# DriftReport summary and emission
# ---------------------------------------------------------------------------


class TestDriftReport:
    def test_summary_counts_and_alert(self) -> None:
        contract = default_pipeline_spec()
        events = [
            DriftEvent(
                layer="bronze",
                scope="top_level",
                column="lead_segment_hint",
                drift_class="optional_known",
                applied_policy="passthrough_with_alert",
            ),
            DriftEvent(
                layer="bronze",
                scope="top_level",
                column="campaign_id",
                drift_class="missing_required",
                applied_policy="block",
            ),
        ]
        report = build_drift_report("run-1", contract, events)
        summary = report.summary()
        assert summary["by_class"]["optional_known"] == 1
        assert summary["by_class"]["missing_required"] == 1
        assert summary["highest_policy_applied"] == "block"
        assert summary["schema_drift_alert"] is True
        assert report.has_block is True

    def test_silent_only_does_not_alert(self) -> None:
        contract = default_pipeline_spec()
        events = [
            DriftEvent(
                layer="bronze",
                scope="top_level",
                column="agent_team",
                drift_class="unknown",
                applied_policy="passthrough_silent",
            )
        ]
        report = build_drift_report("run-2", contract, events)
        assert report.schema_drift_alert is False

    def test_write_drift_report_persists_json(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        report = build_drift_report("run-3", contract, [])
        out = tmp_path / "drift.json"
        write_drift_report(report, out)
        loaded = json.loads(out.read_text(encoding="utf-8"))
        assert loaded["run_id"] == "run-3"
        assert loaded["schema_contract_version"] == int(
            contract.get("schema_contract_version") or 0
        )
        assert loaded["summary"]["schema_drift_alert"] is False


# ---------------------------------------------------------------------------
# Bronze: drift detection through build_bronze
# ---------------------------------------------------------------------------


class TestBronzeDetection:
    def test_build_bronze_clean_reports_no_drift_with_contract(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        path = _write_bronze(tmp_path, _bronze_rows())
        result = build_bronze(path, contract=contract)
        assert result.validation_report.schema_ok is True
        assert result.validation_report.unknown_columns == []
        assert result.drift_events == []

    def test_build_bronze_unknown_column_event(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        path = _write_bronze(tmp_path, _bronze_rows(extra_columns={"internal_score": 0.5}))
        result = build_bronze(path, contract=contract)
        assert "internal_score" in result.validation_report.unknown_columns
        assert (
            result.validation_report.applied_policies["top_level:internal_score"]
            == "passthrough_with_alert"
        )
        assert result.validation_report.schema_ok is True

    def test_build_bronze_missing_required_blocks(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        rows = _bronze_rows()
        rows[0].pop("direction")
        path = _write_bronze(tmp_path, rows)
        result = build_bronze(path, contract=contract)
        assert result.validation_report.schema_ok is False
        assert "direction" in result.validation_report.missing_columns
        assert any(
            e.drift_class == "missing_required" and e.applied_policy == "block"
            for e in result.drift_events
        )

    def test_build_bronze_optional_known_preserved(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        contract["bronze"]["optional_columns"] = ["lead_segment_hint"]
        path = _write_bronze(
            tmp_path, _bronze_rows(extra_columns={"lead_segment_hint": "segment_a"})
        )
        result = build_bronze(path, contract=contract)
        assert "lead_segment_hint" in result.validation_report.optional_known_columns_present

    def test_detect_metadata_drift(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        rows = _bronze_rows()
        rows[0]["metadata"] = json.dumps(
            {"device": "iphone", "utm_term": "abc", "lead_source": "google"}
        )
        path = _write_bronze(tmp_path, rows)
        result = build_bronze(path, contract=contract)
        assert "utm_term" in result.validation_report.unknown_metadata_keys

    def test_no_contract_keeps_legacy_behavior(self, tmp_path: Path) -> None:
        path = _write_bronze(tmp_path, _bronze_rows(extra_columns={"foo": 1}))
        result = build_bronze(path)
        assert result.drift_events == []
        assert result.validation_report.unknown_columns == []


# ---------------------------------------------------------------------------
# Silver: namespacing and lead-level aggregation of extras
# ---------------------------------------------------------------------------


class TestSilverPropagation:
    def test_apply_bronze_passthrough_namespace_renames_undeclared(self) -> None:
        contract = default_pipeline_spec()
        df = pd.DataFrame({"internal_score": [1, 2, 3]})
        events = [
            DriftEvent(
                layer="bronze",
                scope="top_level",
                column="internal_score",
                drift_class="unknown",
                applied_policy="passthrough_with_alert",
            )
        ]
        out = apply_bronze_passthrough_namespace(df, contract, events)
        assert namespaced_passthrough_name("internal_score") in out.columns
        assert "internal_score" not in out.columns

    def test_declared_preserve_keeps_original_name(self) -> None:
        contract = default_pipeline_spec()
        contract["silver"]["preserve_extra_columns"] = ["lead_segment_hint"]
        df = pd.DataFrame({"lead_segment_hint": ["a", "b"]})
        events = [
            DriftEvent(
                layer="bronze",
                scope="top_level",
                column="lead_segment_hint",
                drift_class="optional_known",
                applied_policy="passthrough_with_alert",
            )
        ]
        out = apply_bronze_passthrough_namespace(df, contract, events)
        assert "lead_segment_hint" in out.columns
        assert namespaced_passthrough_name("lead_segment_hint") not in out.columns

    def test_build_silver_namespaces_unknown_column_and_lead_aggregates(
        self, tmp_path: Path
    ) -> None:
        contract = default_pipeline_spec()
        path = _write_bronze(tmp_path, _bronze_rows(extra_columns={"internal_score": 0.7}))
        result = build_bronze(path, contract=contract)
        raw = pd.read_parquet(path)
        silver = build_silver(raw, contract=contract, drift_events=result.drift_events)
        passthrough = namespaced_passthrough_name("internal_score")
        assert passthrough in silver.columns

        leads = build_silver_leads(silver, contract=contract)
        assert passthrough in leads.columns

    def test_build_silver_preserves_declared_extras_with_aggregation_rule(
        self, tmp_path: Path
    ) -> None:
        contract = default_pipeline_spec()
        contract["bronze"]["optional_columns"] = ["lead_segment_hint"]
        contract["silver"]["preserve_extra_columns"] = ["lead_segment_hint"]
        contract["silver"]["extra_aggregation_rules"] = {"lead_segment_hint": "first_non_null"}
        path = _write_bronze(
            tmp_path, _bronze_rows(extra_columns={"lead_segment_hint": "segment_a"})
        )
        result = build_bronze(path, contract=contract)
        raw = pd.read_parquet(path)
        silver = build_silver(raw, contract=contract, drift_events=result.drift_events)
        leads = build_silver_leads(silver, contract=contract)
        assert "lead_segment_hint" in silver.columns
        assert "lead_segment_hint" in leads.columns
        assert leads["lead_segment_hint"].iloc[0] == "segment_a"


# ---------------------------------------------------------------------------
# Gold: explicit partition with drift events
# ---------------------------------------------------------------------------


def _minimal_silver_inputs(tmp_path: Path, extra: dict[str, object] | None = None):
    """Build minimal Silver leads + messages frames for a Gold test."""
    contract = default_pipeline_spec()
    rows = _bronze_rows(extra_columns=extra)
    path = _write_bronze(tmp_path, rows)
    bronze = build_bronze(path, contract=contract)
    raw = pd.read_parquet(path)
    silver_messages = build_silver(raw, contract=contract, drift_events=bronze.drift_events)
    silver_leads = build_silver_leads(silver_messages, contract=contract)
    return contract, silver_leads, silver_messages, bronze.drift_events


class TestGoldPartition:
    def test_undeclared_passthrough_dropped_with_drift_event(self, tmp_path: Path) -> None:
        contract, silver_leads, silver_messages, _ = _minimal_silver_inputs(
            tmp_path, extra={"internal_score": 0.7}
        )
        drop_events: list[DriftEvent] = []
        gold = build_gold(
            silver_leads,
            silver_messages,
            contract=contract,
            drop_events=drop_events,
        )
        passthrough = namespaced_passthrough_name("internal_score")
        assert passthrough not in gold.columns
        assert any(
            e.column == passthrough and e.detail.get("reason") == "not_in_gold_contract"
            for e in drop_events
        )

    def test_declared_passthrough_preserved(self, tmp_path: Path) -> None:
        contract, silver_leads, silver_messages, _ = _minimal_silver_inputs(
            tmp_path, extra={"agent_team": "team_a"}
        )
        passthrough = namespaced_passthrough_name("agent_team")
        contract["gold"]["passthrough_columns"] = [passthrough]
        drop_events: list[DriftEvent] = []
        gold = build_gold(
            silver_leads,
            silver_messages,
            contract=contract,
            drop_events=drop_events,
        )
        assert passthrough in gold.columns

    def test_legacy_no_contract_path_preserves_existing_intermediates_drop(
        self, tmp_path: Path
    ) -> None:
        contract, silver_leads, silver_messages, _ = _minimal_silver_inputs(tmp_path)
        gold = build_gold(silver_leads, silver_messages)
        assert "quoted_price_mentions" not in gold.columns
        assert "lead_lifecycle_hours" not in gold.columns


# ---------------------------------------------------------------------------
# Gold Macro: contract-driven dimensions & schema_contract_version
# ---------------------------------------------------------------------------


def _gold_df_with_dimensions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lead_key": ["a", "b"],
            "persona_profile": ["lead_frio", "lead_frio"],
            "audience_segment": ["nutricao_basica", "nutricao_basica"],
            "lead_temperature": ["frio", "frio"],
            "engagement_bucket": ["lead_frio", "lead_frio"],
            "intent_stage": ["descoberta_inicial", "descoberta_inicial"],
            "conversation_sentiment_label": ["neutro", "neutro"],
            "closure_outcome_group": ["aberto", "aberto"],
            "competitor_pressure_level": ["nenhuma", "nenhuma"],
            "price_objection_intensity": ["nenhuma", "nenhuma"],
            "commercial_urgency_signal": ["nenhuma", "nenhuma"],
            "dominant_email_provider": [None, None],
            "contains_email": [False, False],
            "total_messages": [3, 5],
            "conversation_count": [1, 2],
            "data_shared_score": [0, 1],
            "mentioned_competitor": [False, True],
            "mentioned_sinistro": [False, False],
            "has_closed_outcome": [False, False],
        }
    )


class TestGoldMacroContractDriven:
    def test_missing_dimension_yields_sem_contrato_placeholder(self) -> None:
        contract = default_pipeline_spec()
        contract["gold_macro"]["categorical_dimensions"] = list(
            contract["gold_macro"]["categorical_dimensions"]
        ) + ["agent_team"]
        gold_df = _gold_df_with_dimensions()
        macro = build_gold_macro(gold_df, contract=contract)
        agent_team_rows = macro[macro["dimension"] == "agent_team"]
        assert len(agent_team_rows) == 1
        assert agent_team_rows.iloc[0]["dimension_value"] == "sem_contrato"
        assert int(agent_team_rows.iloc[0]["lead_count"]) == 0

    def test_schema_contract_version_embedded_when_contract_provided(self) -> None:
        contract = default_pipeline_spec()
        macro = build_gold_macro(_gold_df_with_dimensions(), contract=contract)
        assert "schema_contract_version" in macro.columns
        assert int(macro["schema_contract_version"].iloc[0]) == int(
            contract["schema_contract_version"]
        )

    def test_contract_version_absent_when_contract_not_provided(self) -> None:
        macro = build_gold_macro(_gold_df_with_dimensions())
        assert "schema_contract_version" not in macro.columns


# ---------------------------------------------------------------------------
# Contract guard
# ---------------------------------------------------------------------------


class TestContractGuard:
    def test_default_pipeline_spec_carries_contract_version(self) -> None:
        spec = default_pipeline_spec()
        version = spec.get("schema_contract_version")
        assert isinstance(version, int)
        assert version >= 1

    def test_repo_pipeline_spec_carries_contract_version(self) -> None:
        spec_path = Path(__file__).resolve().parents[1] / "config" / "pipeline_spec.json"
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        assert isinstance(payload.get("schema_contract_version"), int)

    def test_repo_contract_declares_propagation_keys(self) -> None:
        spec_path = Path(__file__).resolve().parents[1] / "config" / "pipeline_spec.json"
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        assert isinstance(payload["bronze"].get("optional_columns"), list)
        assert isinstance(payload["bronze"].get("category_domains"), dict)
        assert isinstance(payload["silver"].get("preserve_extra_columns"), list)
        assert isinstance(payload["gold"].get("optional_columns"), list)
        assert isinstance(payload["gold_macro"].get("categorical_dimensions"), list)
        assert isinstance(payload["gold_macro"].get("numeric_metrics"), list)


# ---------------------------------------------------------------------------
# detect_bronze_schema_drift end-to-end on synthetic frames
# ---------------------------------------------------------------------------


class TestDetectBronzeSchemaDrift:
    def test_detects_unknown_column_and_metadata_key(self, tmp_path: Path) -> None:
        contract = default_pipeline_spec()
        rows = _bronze_rows(extra_columns={"internal_score": 0.5})
        rows[0]["metadata"] = json.dumps(
            {"device": "iphone", "utm_term": "abc", "lead_source": "google"}
        )
        path = _write_bronze(tmp_path, rows)
        raw = pd.read_parquet(path)
        events = detect_bronze_schema_drift(raw, contract)

        kinds = {(e.scope, e.column, e.drift_class) for e in events}
        assert ("top_level", "internal_score", "unknown") in kinds
        assert ("metadata_key", "utm_term", "unknown") in kinds

    def test_returns_empty_when_contract_missing(self, tmp_path: Path) -> None:
        path = _write_bronze(tmp_path, _bronze_rows(extra_columns={"foo": 1}))
        raw = pd.read_parquet(path)
        assert detect_bronze_schema_drift(raw, None) == []
