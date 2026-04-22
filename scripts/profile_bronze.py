from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

import pandas as pd  # noqa: E402

from pipeline.config import build_paths, ensure_directories  # noqa: E402
from pipeline.io.parquet_io import write_json  # noqa: E402


def top_values(series: pd.Series, limit: int = 10) -> dict[str, int]:
    counts = series.fillna("<null>").astype(str).value_counts().head(limit)
    return {key: int(value) for key, value in counts.items()}


def main() -> None:
    paths = build_paths(ROOT)
    ensure_directories(paths)

    df = pd.read_parquet(paths.raw_bronze_source)

    profile = {
        "row_count": int(len(df)),
        "conversation_count": int(df["conversation_id"].nunique()),
        "column_count": int(len(df.columns)),
        "columns": list(df.columns),
        "null_counts": {column: int(df[column].isna().sum()) for column in df.columns},
        "top_message_types": top_values(df["message_type"]),
        "top_statuses": top_values(df["status"]),
        "top_outcomes": top_values(df["conversation_outcome"]),
        "top_campaigns": top_values(df["campaign_id"]),
        "top_agents": top_values(df["agent_id"]),
        "message_length_summary": {
            "min": int(df["message_body"].fillna("").str.len().min()),
            "mean": float(df["message_body"].fillna("").str.len().mean()),
            "p95": float(df["message_body"].fillna("").str.len().quantile(0.95)),
            "max": int(df["message_body"].fillna("").str.len().max()),
        },
    }

    write_json(profile, paths.reports / "bronze_profile.json")
    print(json.dumps(profile, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
