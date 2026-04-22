from __future__ import annotations

import pandas as pd

from pipeline.orchestration.compiler import get_default_compiled_plan
from pipeline.quality.quarantine import quarantine_bronze_records


def test_quarantine_bronze_records_isolates_invalid_timestamp_and_metadata(tmp_path) -> None:
    bronze = pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-02-01 10:00:00"), "metadata": '{"city":"Sao Paulo"}'},
            {"timestamp": pd.NaT, "metadata": '{"city":"Sao Paulo"}'},
            {"timestamp": pd.Timestamp("2026-02-01 10:01:00"), "metadata": "not-json"},
        ]
    )

    result = quarantine_bronze_records(bronze, tmp_path, get_default_compiled_plan())

    assert result["report"]["quarantined_rows"] == 2
    assert len(result["clean_df"]) == 1
