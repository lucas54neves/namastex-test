from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from pipeline.config import build_paths  # noqa: E402
from pipeline.jobs import artifacts_as_dict, run_pipeline  # noqa: E402

DEFAULT_POLL_INTERVAL_SECONDS = 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the medalion pipeline continuously with polling."
    )
    parser.add_argument(
        "--force-first-run",
        action="store_true",
        help="Force execution on the first loop iteration.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.getenv("PIPELINE_POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS)),
        help="Seconds to wait between pipeline checks.",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Stop after N cycles. Use 0 to keep running indefinitely.",
    )
    return parser.parse_args()


def run_daemon(args: argparse.Namespace) -> None:
    cycle = 0
    while True:
        cycle += 1
        force = bool(args.force_first_run and cycle == 1)
        artifacts = run_pipeline(build_paths(ROOT), force=force)
        output = {"cycle": cycle, "poll_interval_seconds": args.poll_interval_seconds}
        output.update(artifacts_as_dict(artifacts))
        print(json.dumps(output, indent=2, ensure_ascii=False), flush=True)

        if args.max_cycles and cycle >= args.max_cycles:
            break

        time.sleep(max(args.poll_interval_seconds, 1))


def main() -> None:
    run_daemon(parse_args())


if __name__ == "__main__":
    main()
