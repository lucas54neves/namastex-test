from __future__ import annotations

import json

import pandas as pd

from pipeline.transforms.silver_patterns import _safe_string

__all__ = [
    "_first_non_empty",
    "_json_sorted_unique",
    "_first_non_null",
    "_last_non_null",
    "_max_or_false",
    "_bool_series_or_default",
    "_int_series_or_default",
    "_value_series_or_default",
]


def _first_non_empty(values: pd.Series) -> str:
    for value in values:
        text = _safe_string(value).strip()
        if text:
            return text
    return ""


def _json_sorted_unique(values: pd.Series) -> str:
    collected = sorted(
        {_safe_string(value).strip() for value in values if _safe_string(value).strip()}
    )
    return json.dumps(collected, ensure_ascii=True)


def _first_non_null(values: pd.Series) -> object:
    for value in values:
        if pd.notna(value):
            return value
    return None


def _last_non_null(values: pd.Series) -> object:
    for value in values.iloc[::-1]:
        if pd.notna(value):
            return value
    return None


def _max_or_false(values: pd.Series) -> bool:
    return bool(values.fillna(False).astype(bool).max())


def _bool_series_or_default(
    frame: pd.DataFrame,
    column: str,
    fallback_column: str | None = None,
) -> pd.Series:
    if column in frame.columns:
        return frame[column].fillna(False).astype(bool)
    if fallback_column and fallback_column in frame.columns:
        fallback = frame[fallback_column].fillna(False).astype(bool)
        if "direction" not in frame.columns:
            return fallback
        return fallback & frame["direction"].eq("inbound")
    return pd.Series(False, index=frame.index, dtype=bool)


def _int_series_or_default(
    frame: pd.DataFrame,
    column: str,
    fallback_column: str | None = None,
) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    if fallback_column and fallback_column in frame.columns:
        fallback = pd.to_numeric(frame[fallback_column], errors="coerce").fillna(0).astype(int)
        if "direction" not in frame.columns:
            return fallback
        return fallback.where(frame["direction"].eq("inbound"), 0)
    return pd.Series(0, index=frame.index, dtype=int)


def _value_series_or_default(
    frame: pd.DataFrame,
    column: str,
    fallback_column: str | None = None,
) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    if fallback_column and fallback_column in frame.columns:
        if "direction" not in frame.columns:
            return frame[fallback_column]
        return frame[fallback_column].where(frame["direction"].eq("inbound"))
    return pd.Series([None] * len(frame), index=frame.index, dtype=object)
