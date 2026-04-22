from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from pipeline.config import build_paths  # noqa: E402
from pipeline.orchestration.jobs import build_monitor_snapshot  # noqa: E402


def main() -> None:
    snapshot = build_monitor_snapshot(build_paths(ROOT))
    print(json.dumps(snapshot, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
