from __future__ import annotations

from typing import cast

import pandas as pd

from pipeline.orchestration.compiler import get_default_compiled_plan
from pipeline.transforms.silver_patterns import STATUS_PRIORITY


def deduplicate_events(
    df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> pd.DataFrame:
    plan = compiled_plan or get_default_compiled_plan()
    dedupe_keys = cast(list[str], plan["dedupe_keys"])
    ranked = df.copy()
    ranked["status_priority"] = ranked["status"].map(STATUS_PRIORITY).fillna(-1).astype(int)
    ranked["duplicate_event_group_size"] = ranked.groupby(dedupe_keys, dropna=False)[
        "message_id"
    ].transform("size")
    ranked["had_status_duplication"] = ranked["duplicate_event_group_size"].gt(1)
    ranked = ranked.sort_values(
        ["conversation_id", "timestamp", "status_priority", "message_id"],
        ascending=[True, True, False, True],
    )
    deduped = ranked.drop_duplicates(subset=dedupe_keys, keep="first").copy()
    deduped["dropped_duplicate_events"] = deduped["duplicate_event_group_size"] - 1
    return deduped.drop(columns=["status_priority"]).reset_index(drop=True)
