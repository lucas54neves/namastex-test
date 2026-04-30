from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.io.parquet_io import read_json, write_json


@dataclass(frozen=True)
class SourceCDCState:
    digest: str
    known_ids: frozenset[str]
    row_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "digest": self.digest,
            "known_ids": sorted(self.known_ids),
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SourceCDCState:
        ids = frozenset(data["known_ids"])
        return cls(digest=data["digest"], known_ids=ids, row_count=data["row_count"])


def _cdc_digest(ids: frozenset[str]) -> str:
    canonical = json.dumps(sorted(ids), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def build_cdc_state(source_path: Path) -> SourceCDCState:
    """Read only the message_id column from the source Parquet and build a SourceCDCState."""
    df = pd.read_parquet(source_path, columns=["message_id"])
    ids = frozenset(df["message_id"].astype(str).tolist())
    digest = _cdc_digest(ids)
    return SourceCDCState(digest=digest, known_ids=ids, row_count=len(ids))


def compute_new_ids(current: SourceCDCState, previous: SourceCDCState | None) -> frozenset[str]:
    """Return current.known_ids - previous.known_ids; returns full set if previous is None."""
    if previous is None:
        return current.known_ids
    return current.known_ids - previous.known_ids


def has_source_changed_cdc(current: SourceCDCState, previous: SourceCDCState | None) -> bool:
    """True if previous is None or current.digest != previous.digest."""
    if previous is None:
        return True
    return current.digest != previous.digest


# DEPRECATED: SourceFingerprint, build_source_fingerprint, and has_source_changed are superseded
# by SourceCDCState and the cdc_* functions above. Remove after incremental path is validated.
@dataclass(frozen=True)
class SourceFingerprint:
    path: str
    size_bytes: int
    modified_time: float

    def as_dict(self) -> dict[str, float | int | str]:
        return {
            "path": self.path,
            "size_bytes": self.size_bytes,
            "modified_time": self.modified_time,
        }


def build_source_fingerprint(path: Path) -> SourceFingerprint:
    stat = path.stat()
    return SourceFingerprint(
        path=str(path),
        size_bytes=int(stat.st_size),
        modified_time=float(stat.st_mtime),
    )


def load_pipeline_state(path: Path) -> dict:
    return read_json(path, default={"runs": []})


def save_pipeline_state(path: Path, state: dict) -> None:
    write_json(state, path)


def compute_quality_snapshot(
    bronze_df: pd.DataFrame,
    null_rate_cols: list[str],
    distribution_cols: list[str],
) -> dict[str, Any]:
    null_rates: dict[str, float] = {}
    for col in null_rate_cols:
        if col in bronze_df.columns:
            null_rates[col] = round(float(bronze_df[col].isna().mean()), 6)
    distribution: dict[str, dict[str, float]] = {}
    for col in distribution_cols:
        if col in bronze_df.columns:
            counts = bronze_df[col].dropna().astype(str).value_counts(normalize=True)
            distribution[col] = {k: round(float(v), 6) for k, v in counts.items()}
    return {
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "record_count": int(len(bronze_df)),
        "null_rates": null_rates,
        "distribution": distribution,
    }


def load_quality_baseline(paths: Any) -> dict[str, Any] | None:
    state_path = paths.state / "pipeline_state.json"
    state = load_pipeline_state(state_path)
    return state.get("quality_baseline")


def has_source_changed(current: SourceFingerprint, previous: dict | None) -> bool:
    if not previous:
        return True
    return current.as_dict() != previous
