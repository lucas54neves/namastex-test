"""
Schema drift detection, policy resolution and report emission.

Implements the schema evolution contract described in
``spec/spec-architecture-schema-evolution-detection-and-propagation.md``:

- Classifies every observed column or metadata key into one of the documented
  drift classes (``expected``, ``optional_known``, ``unknown``,
  ``missing_required``, ``type_mismatch``, ``category_drift``).
- Resolves a propagation policy for each event from the contract in
  ``config/pipeline_spec.json``.
- Builds a structured drift report consumable by the agentic governance layer.

This module owns no I/O of its own besides ``write_drift_report`` and is
deterministic given a fixed contract version and input.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pipeline.io.parquet_io import write_json

__all__ = [
    "DRIFT_CLASSES",
    "PROPAGATION_POLICIES",
    "DriftEvent",
    "DriftReport",
    "classify_bronze_columns",
    "classify_metadata_keys",
    "classify_category_drift",
    "classify_type_mismatch",
    "resolve_policy",
    "build_drift_report",
    "write_drift_report",
    "namespaced_passthrough_name",
    "BRONZE_PASSTHROUGH_PREFIX",
]

DRIFT_CLASSES: tuple[str, ...] = (
    "expected",
    "optional_known",
    "unknown",
    "missing_required",
    "type_mismatch",
    "category_drift",
)

PROPAGATION_POLICIES: tuple[str, ...] = (
    "passthrough_silent",
    "passthrough_with_alert",
    "quarantine",
    "block",
)

_POLICY_SEVERITY = {
    "passthrough_silent": 0,
    "passthrough_with_alert": 1,
    "quarantine": 2,
    "block": 3,
}

BRONZE_PASSTHROUGH_PREFIX = "bronze_passthrough__"


def namespaced_passthrough_name(column: str) -> str:
    return f"{BRONZE_PASSTHROUGH_PREFIX}{column}"


@dataclass
class DriftEvent:
    layer: str
    scope: str
    column: str
    drift_class: str
    applied_policy: str
    row_count: int = 0
    non_null_count: int = 0
    sample_values_masked: list[str] = field(default_factory=list)
    propagation: dict[str, str] = field(default_factory=dict)
    contract_version_at_event: int = 0
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DriftReport:
    run_id: str
    schema_contract_version: int
    events: list[DriftEvent] = field(default_factory=list)

    @property
    def schema_drift_alert(self) -> bool:
        return any(
            event.applied_policy in {"passthrough_with_alert", "quarantine", "block"}
            for event in self.events
        )

    @property
    def has_block(self) -> bool:
        return any(event.applied_policy == "block" for event in self.events)

    def summary(self) -> dict[str, Any]:
        by_class: dict[str, int] = {cls: 0 for cls in DRIFT_CLASSES}
        for event in self.events:
            by_class[event.drift_class] = by_class.get(event.drift_class, 0) + 1
        if self.events:
            highest_policy = max(
                self.events,
                key=lambda event: _POLICY_SEVERITY.get(event.applied_policy, 0),
            ).applied_policy
        else:
            highest_policy = "passthrough_silent"
        return {
            "by_class": by_class,
            "highest_policy_applied": highest_policy,
            "schema_drift_alert": self.schema_drift_alert,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "schema_contract_version": self.schema_contract_version,
            "summary": self.summary(),
            "events": [event.as_dict() for event in self.events],
        }


def _bronze_section(contract: Mapping[str, Any]) -> Mapping[str, Any]:
    section = contract.get("bronze") or {}
    return section if isinstance(section, Mapping) else {}


def _contract_version(contract: Mapping[str, Any]) -> int:
    raw = contract.get("schema_contract_version", 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def resolve_policy(
    drift_class: str,
    column: str,
    contract: Mapping[str, Any],
    *,
    scope: str = "top_level",
) -> str:
    """Resolve the propagation policy for a single drift event."""

    bronze = _bronze_section(contract)
    column_policies = bronze.get("column_policies") or {}
    if isinstance(column_policies, Mapping) and column in column_policies:
        policy = column_policies.get(column)
        if policy in PROPAGATION_POLICIES:
            return str(policy)

    if drift_class == "missing_required":
        return "block"
    if drift_class == "expected":
        return "passthrough_silent"
    if drift_class == "optional_known":
        return "passthrough_with_alert"
    if drift_class == "unknown":
        if scope == "metadata_key":
            default = bronze.get("unknown_metadata_default_policy")
        else:
            default = bronze.get("unknown_column_default_policy")
        return str(default) if default in PROPAGATION_POLICIES else "passthrough_with_alert"
    if drift_class == "type_mismatch":
        return "quarantine"
    if drift_class == "category_drift":
        return "quarantine"
    return "passthrough_with_alert"


def _mask_sample_values(values: list[Any], limit: int = 3) -> list[str]:
    """Produce safe, length-limited sample strings for the drift report."""

    safe: list[str] = []
    for raw in values[:limit]:
        if raw is None:
            continue
        text = str(raw)
        if len(text) > 32:
            text = text[:32] + "..."
        safe.append(text)
    return safe


def classify_bronze_columns(
    observed_columns: list[str],
    contract: Mapping[str, Any],
) -> list[DriftEvent]:
    """Classify the top-level Bronze schema against the contract.

    Returns events for ``optional_known`` (when present), ``unknown`` and
    ``missing_required``. ``expected`` columns are not emitted as events to
    keep the report focused on actionable findings.
    """

    bronze = _bronze_section(contract)
    required = list(bronze.get("required_columns") or [])
    optional = list(bronze.get("optional_columns") or [])
    passthrough = list(bronze.get("passthrough_columns") or [])
    declared = set(required) | set(optional) | set(passthrough)
    observed = set(observed_columns)
    contract_version = _contract_version(contract)

    events: list[DriftEvent] = []

    for column in optional:
        if column in observed:
            events.append(
                DriftEvent(
                    layer="bronze",
                    scope="top_level",
                    column=column,
                    drift_class="optional_known",
                    applied_policy=resolve_policy("optional_known", column, contract),
                    contract_version_at_event=contract_version,
                )
            )

    for column in observed_columns:
        if column not in declared:
            events.append(
                DriftEvent(
                    layer="bronze",
                    scope="top_level",
                    column=column,
                    drift_class="unknown",
                    applied_policy=resolve_policy("unknown", column, contract),
                    contract_version_at_event=contract_version,
                )
            )

    for column in required:
        if column not in observed:
            events.append(
                DriftEvent(
                    layer="bronze",
                    scope="top_level",
                    column=column,
                    drift_class="missing_required",
                    applied_policy=resolve_policy("missing_required", column, contract),
                    contract_version_at_event=contract_version,
                )
            )

    return events


def classify_metadata_keys(
    observed_keys: list[str],
    contract: Mapping[str, Any],
) -> list[DriftEvent]:
    """Classify metadata JSON keys against the contract."""

    bronze = _bronze_section(contract)
    silver = contract.get("silver") or {}
    silver_metadata = (
        list(silver.get("metadata_fields") or []) if isinstance(silver, Mapping) else []
    )
    declared_metadata = set(silver_metadata) | set(
        list(bronze.get("optional_metadata_fields") or [])
    )
    contract_version = _contract_version(contract)

    events: list[DriftEvent] = []
    for key in observed_keys:
        if key in declared_metadata:
            continue
        events.append(
            DriftEvent(
                layer="bronze",
                scope="metadata_key",
                column=key,
                drift_class="unknown",
                applied_policy=resolve_policy("unknown", key, contract, scope="metadata_key"),
                contract_version_at_event=contract_version,
            )
        )
    return events


def classify_category_drift(
    column: str,
    observed_values: list[Any],
    contract: Mapping[str, Any],
) -> DriftEvent | None:
    """Return a category_drift event when observed values violate the domain."""

    bronze = _bronze_section(contract)
    domains = bronze.get("category_domains") or {}
    declared = domains.get(column) if isinstance(domains, Mapping) else None
    if not isinstance(declared, list) or not declared:
        return None
    declared_set = {str(value) for value in declared}
    unexpected = sorted({str(v) for v in observed_values if str(v) not in declared_set})
    if not unexpected:
        return None
    return DriftEvent(
        layer="bronze",
        scope="top_level",
        column=column,
        drift_class="category_drift",
        applied_policy=resolve_policy("category_drift", column, contract),
        sample_values_masked=_mask_sample_values(unexpected),
        contract_version_at_event=_contract_version(contract),
        detail={"unexpected_values_masked": _mask_sample_values(unexpected, limit=10)},
    )


def classify_type_mismatch(
    column: str,
    expected_dtype: str,
    observed_dtype: str,
    contract: Mapping[str, Any],
) -> DriftEvent | None:
    """Return a type_mismatch event when observed dtype differs from expected."""

    if expected_dtype == observed_dtype:
        return None
    return DriftEvent(
        layer="bronze",
        scope="top_level",
        column=column,
        drift_class="type_mismatch",
        applied_policy=resolve_policy("type_mismatch", column, contract),
        contract_version_at_event=_contract_version(contract),
        detail={"expected_dtype": expected_dtype, "observed_dtype": observed_dtype},
    )


def build_drift_report(
    run_id: str,
    contract: Mapping[str, Any],
    events: list[DriftEvent],
) -> DriftReport:
    return DriftReport(
        run_id=run_id,
        schema_contract_version=_contract_version(contract),
        events=list(events),
    )


def write_drift_report(report: DriftReport, path: Path) -> Path:
    write_json(report.as_dict(), path)
    return path
