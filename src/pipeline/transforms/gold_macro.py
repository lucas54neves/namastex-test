from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

_CATEGORICAL_DIMENSIONS = [
    "persona_profile",
    "audience_segment",
    "dominant_email_provider",
    "lead_temperature",
    "engagement_bucket",
    "conversation_sentiment_label",
    "closure_outcome_group",
    "competitor_pressure_level",
    "price_objection_intensity",
    "commercial_urgency_signal",
    "intent_stage",
]

_NUMERIC_METRIC_NAMES = [
    "avg_total_messages",
    "avg_conversation_count",
    "avg_data_shared_score",
    "total_leads",
    "leads_with_email_pct",
    "leads_with_competitor_signal_pct",
    "leads_with_sinistro_signal_pct",
    "leads_closed_pct",
]


def _fill_null_str(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("sem_informacao").replace("", "sem_informacao")


def _dimension_rows(gold_df: pd.DataFrame, dimension: str, total_leads: int) -> pd.DataFrame:
    if dimension == "dominant_email_provider":
        subset = gold_df.loc[gold_df["contains_email"].astype(bool), dimension]
    else:
        subset = gold_df[dimension]

    filled = _fill_null_str(subset)
    counts_series = filled.value_counts(sort=False)

    if len(counts_series) == 0:
        return pd.DataFrame(
            {
                "dimension": [dimension],
                "dimension_value": ["sem_informacao"],
                "lead_count": [0],
                "lead_pct": [0.0],
                "rank": [1],
                "metric_value": [float("nan")],
            }
        )

    counts = pd.DataFrame(
        {
            "dimension_value": counts_series.index.tolist(),
            "lead_count": counts_series.to_numpy().tolist(),
        }
    )
    counts = counts.sort_values(
        ["lead_count", "dimension_value"], ascending=[False, True]
    ).reset_index(drop=True)
    counts["rank"] = range(1, len(counts) + 1)
    counts["lead_pct"] = counts["lead_count"] / total_leads if total_leads > 0 else 0.0
    counts["dimension"] = dimension
    counts["metric_value"] = float("nan")

    return counts[
        ["dimension", "dimension_value", "lead_count", "lead_pct", "rank", "metric_value"]
    ]


def _numeric_snapshot_rows(gold_df: pd.DataFrame) -> pd.DataFrame:
    n = len(gold_df)

    def _bool_pct(col: str) -> float:
        if col not in gold_df.columns or n == 0:
            return 0.0
        return float(gold_df[col].astype(bool).mean())

    def _col_mean(col: str) -> float:
        if col not in gold_df.columns or n == 0:
            return 0.0
        return float(gold_df[col].mean())

    metrics: list[tuple[str, float]] = [
        ("avg_total_messages", _col_mean("total_messages")),
        ("avg_conversation_count", _col_mean("conversation_count")),
        ("avg_data_shared_score", _col_mean("data_shared_score")),
        ("total_leads", float(n)),
        ("leads_with_email_pct", _bool_pct("contains_email")),
        ("leads_with_competitor_signal_pct", _bool_pct("mentioned_competitor")),
        ("leads_with_sinistro_signal_pct", _bool_pct("mentioned_sinistro")),
        ("leads_closed_pct", _bool_pct("has_closed_outcome")),
    ]

    rows = [
        {
            "dimension": "numeric_snapshot",
            "dimension_value": metric_name,
            "lead_count": 0,
            "lead_pct": 0.0,
            "rank": rank,
            "metric_value": round(value, 2),
        }
        for rank, (metric_name, value) in enumerate(metrics, start=1)
    ]

    return pd.DataFrame(rows)


def build_gold_macro(gold_df: pd.DataFrame) -> pd.DataFrame:
    total_leads = len(gold_df)
    computed_at = datetime.now(UTC).isoformat()

    blocks: list[pd.DataFrame] = []
    for dimension in _CATEGORICAL_DIMENSIONS:
        blocks.append(_dimension_rows(gold_df, dimension, total_leads))
    blocks.append(_numeric_snapshot_rows(gold_df))

    result = pd.concat(blocks, ignore_index=True)
    result["computed_at_utc"] = computed_at
    result["lead_count"] = result["lead_count"].fillna(0).astype("int64")
    result["lead_pct"] = result["lead_pct"].fillna(0.0).astype("float64")
    result["rank"] = result["rank"].fillna(0).astype("int64")
    result["metric_value"] = pd.to_numeric(result["metric_value"], errors="coerce").astype(
        "float64"
    )

    result = result.sort_values(["dimension", "rank"]).reset_index(drop=True)
    return result[
        [
            "dimension",
            "dimension_value",
            "lead_count",
            "lead_pct",
            "rank",
            "computed_at_utc",
            "metric_value",
        ]
    ]
