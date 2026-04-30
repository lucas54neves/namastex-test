from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline.runtime.state import (
    SourceCDCState,
    _cdc_digest,
    build_cdc_state,
    compute_new_ids,
    has_source_changed_cdc,
)


def _write_parquet(tmp_path: Path, message_ids: list[str]) -> Path:
    p = tmp_path / "source.parquet"
    pd.DataFrame({"message_id": message_ids}).to_parquet(p, index=False)
    return p


# ---------------------------------------------------------------------------
# SourceCDCState serialisation round-trip
# ---------------------------------------------------------------------------


def test_source_cdc_state_roundtrip() -> None:
    ids = frozenset(["msg_1", "msg_2", "msg_3"])
    digest = _cdc_digest(ids)
    state = SourceCDCState(digest=digest, known_ids=ids, row_count=3)

    d = state.as_dict()
    assert d["digest"] == digest
    assert d["known_ids"] == sorted(ids)
    assert d["row_count"] == 3

    restored = SourceCDCState.from_dict(d)
    assert restored.digest == state.digest
    assert restored.known_ids == state.known_ids
    assert restored.row_count == state.row_count


def test_source_cdc_state_as_dict_sorts_ids() -> None:
    ids = frozenset(["z", "a", "m"])
    state = SourceCDCState(digest=_cdc_digest(ids), known_ids=ids, row_count=3)
    assert state.as_dict()["known_ids"] == ["a", "m", "z"]


# ---------------------------------------------------------------------------
# _cdc_digest determinism and cross-platform reproducibility
# ---------------------------------------------------------------------------


def test_cdc_digest_deterministic() -> None:
    ids = frozenset(["msg_001", "msg_002"])
    assert _cdc_digest(ids) == _cdc_digest(ids)


def test_cdc_digest_order_independent() -> None:
    a = frozenset(["x", "y", "z"])
    b = frozenset(["z", "x", "y"])
    assert _cdc_digest(a) == _cdc_digest(b)


def test_cdc_digest_known_value() -> None:
    ids = frozenset(["a", "b"])
    canonical = json.dumps(["a", "b"], ensure_ascii=False)
    expected = hashlib.sha256(canonical.encode()).hexdigest()
    assert _cdc_digest(ids) == expected


def test_cdc_digest_changes_with_new_id() -> None:
    ids_before = frozenset(["msg_1", "msg_2"])
    ids_after = frozenset(["msg_1", "msg_2", "msg_3"])
    assert _cdc_digest(ids_before) != _cdc_digest(ids_after)


# ---------------------------------------------------------------------------
# build_cdc_state
# ---------------------------------------------------------------------------


def test_build_cdc_state_basic(tmp_path: Path) -> None:
    src = _write_parquet(tmp_path, ["m1", "m2", "m3"])
    state = build_cdc_state(src)

    assert state.known_ids == frozenset(["m1", "m2", "m3"])
    assert state.row_count == 3
    assert state.digest == _cdc_digest(frozenset(["m1", "m2", "m3"]))


def test_build_cdc_state_digest_is_deterministic(tmp_path: Path) -> None:
    src = _write_parquet(tmp_path, ["m1", "m2"])
    assert build_cdc_state(src).digest == build_cdc_state(src).digest


def test_build_cdc_state_digest_changes_after_append(tmp_path: Path) -> None:
    src = _write_parquet(tmp_path, ["m1", "m2"])
    digest_before = build_cdc_state(src).digest

    src2 = tmp_path / "source2.parquet"
    pd.DataFrame({"message_id": ["m1", "m2", "m3"]}).to_parquet(src2, index=False)
    digest_after = build_cdc_state(src2).digest

    assert digest_before != digest_after


# ---------------------------------------------------------------------------
# compute_new_ids
# ---------------------------------------------------------------------------


def test_compute_new_ids_first_run() -> None:
    ids = frozenset(["m1", "m2"])
    current = SourceCDCState(digest=_cdc_digest(ids), known_ids=ids, row_count=2)
    assert compute_new_ids(current, None) == ids


def test_compute_new_ids_incremental() -> None:
    prev_ids = frozenset(["m1", "m2"])
    curr_ids = frozenset(["m1", "m2", "m3", "m4"])
    previous = SourceCDCState(digest=_cdc_digest(prev_ids), known_ids=prev_ids, row_count=2)
    current = SourceCDCState(digest=_cdc_digest(curr_ids), known_ids=curr_ids, row_count=4)

    new_ids = compute_new_ids(current, previous)
    assert new_ids == frozenset(["m3", "m4"])


def test_compute_new_ids_no_change() -> None:
    ids = frozenset(["m1", "m2"])
    state = SourceCDCState(digest=_cdc_digest(ids), known_ids=ids, row_count=2)
    assert compute_new_ids(state, state) == frozenset()


def test_compute_new_ids_returns_frozenset() -> None:
    ids = frozenset(["m1"])
    state = SourceCDCState(digest=_cdc_digest(ids), known_ids=ids, row_count=1)
    result = compute_new_ids(state, None)
    assert isinstance(result, frozenset)


# ---------------------------------------------------------------------------
# has_source_changed_cdc
# ---------------------------------------------------------------------------


def test_has_source_changed_cdc_first_run() -> None:
    ids = frozenset(["m1"])
    state = SourceCDCState(digest=_cdc_digest(ids), known_ids=ids, row_count=1)
    assert has_source_changed_cdc(state, None) is True


def test_has_source_changed_cdc_no_change() -> None:
    ids = frozenset(["m1", "m2"])
    state = SourceCDCState(digest=_cdc_digest(ids), known_ids=ids, row_count=2)
    assert has_source_changed_cdc(state, state) is False


def test_has_source_changed_cdc_changed() -> None:
    ids_old = frozenset(["m1"])
    ids_new = frozenset(["m1", "m2"])
    old_state = SourceCDCState(digest=_cdc_digest(ids_old), known_ids=ids_old, row_count=1)
    new_state = SourceCDCState(digest=_cdc_digest(ids_new), known_ids=ids_new, row_count=2)
    assert has_source_changed_cdc(new_state, old_state) is True
