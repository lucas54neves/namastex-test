from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from pipeline.agent.planner import plan_pipeline_spec  # noqa: E402
from pipeline.config import build_paths  # noqa: E402


def main() -> None:
    report = plan_pipeline_spec(build_paths(ROOT))
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
