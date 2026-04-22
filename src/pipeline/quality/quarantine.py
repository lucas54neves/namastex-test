from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.io.parquet_io import write_json, write_parquet


def _valid_metadata(value: object) -> bool:
    if value is None:
        return False
    try:
        json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return True


def quarantine_bronze_records(
    bronze_df: pd.DataFrame, quarantine_dir: Path, compiled_plan: dict[str, Any]
) -> dict[str, Any]:
    quarantine_cfg = compiled_plan["bronze_quarantine"]
    empty_mask = pd.Series(False, index=bronze_df.index)
    invalid_timestamp_mask = (
        bronze_df["timestamp"].isna()
        if quarantine_cfg.get("invalid_timestamp", False)
        else empty_mask
    )
    invalid_metadata_mask = (
        ~bronze_df["metadata"].map(_valid_metadata)
        if quarantine_cfg.get("invalid_metadata", False)
        else empty_mask
    )
    quarantine_mask = invalid_timestamp_mask | invalid_metadata_mask

    quarantined_df = bronze_df.loc[quarantine_mask].copy()
    clean_df = bronze_df.loc[~quarantine_mask].copy()

    report = {
        "applied": bool(quarantine_mask.any()),
        "quarantined_rows": int(quarantine_mask.sum()),
        "invalid_timestamp_rows": int(invalid_timestamp_mask.sum()),
        "invalid_metadata_rows": int(invalid_metadata_mask.sum()),
        "quarantine_path": str(quarantine_dir / "quarantined_bronze_rows.parquet"),
    }

    if report["applied"]:
        quarantined_df["quarantine_reason"] = "invalid_bronze_record"
        write_parquet(quarantined_df, quarantine_dir / "quarantined_bronze_rows.parquet")
    write_json(report, quarantine_dir / "latest_quarantine_report.json")

    return {
        "clean_df": clean_df,
        "quarantined_df": quarantined_df,
        "report": report,
    }
