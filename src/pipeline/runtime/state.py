from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipeline.io.parquet_io import read_json, write_json


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


def has_source_changed(current: SourceFingerprint, previous: dict | None) -> bool:
    if not previous:
        return True
    return current.as_dict() != previous
