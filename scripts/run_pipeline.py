from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from pipeline.config import build_paths  # noqa: E402
from pipeline.orchestration.jobs import artifacts_as_dict, run_pipeline  # noqa: E402
from pipeline.runtime.llm_runtime import shutdown_langfuse_client  # noqa: E402
from pipeline.runtime.terminal_logging import configure_terminal_logging  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the medalion pipeline.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force execution even when the source fingerprint did not change.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_terminal_logging()
    try:
        artifacts = run_pipeline(build_paths(ROOT), force=args.force)
        print(json.dumps(artifacts_as_dict(artifacts), indent=2, ensure_ascii=False))
    finally:
        shutdown_langfuse_client()


if __name__ == "__main__":
    main()
