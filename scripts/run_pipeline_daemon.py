from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from pipeline.config import build_paths  # noqa: E402
from pipeline.orchestration.jobs import artifacts_as_dict, run_pipeline  # noqa: E402
from pipeline.runtime.llm_runtime import shutdown_langfuse_client  # noqa: E402
from pipeline.runtime.terminal_logging import (  # noqa: E402
    configure_terminal_logging,
    log_event,
)

DEFAULT_POLL_INTERVAL_SECONDS = 60
DEFAULT_MAX_BACKOFF_SECONDS = 300


def _positive_int(value: str) -> int:
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {value!r}")
    return ivalue


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
    parser.add_argument(
        "--max-backoff-seconds",
        type=_positive_int,
        default=int(os.getenv("PIPELINE_MAX_BACKOFF_SECONDS", DEFAULT_MAX_BACKOFF_SECONDS)),
        help="Maximum backoff time between retries after failure (seconds).",
    )
    return parser.parse_args()


def run_daemon(args: argparse.Namespace) -> None:
    configure_terminal_logging()
    cycle = 0
    attempt = 0
    try:
        while True:
            cycle += 1
            force = bool(args.force_first_run and cycle == 1)
            log_event(
                logging.INFO,
                "daemon_cycle_started",
                cycle=cycle,
                force=force,
                poll_interval_seconds=args.poll_interval_seconds,
            )
            try:
                artifacts = run_pipeline(build_paths(ROOT), force=force)
                attempt = 0
                output = {"cycle": cycle, "poll_interval_seconds": args.poll_interval_seconds}
                output.update(artifacts_as_dict(artifacts))
                print(json.dumps(output, indent=2, ensure_ascii=False), flush=True)
            except Exception as exc:
                backoff = min(2**attempt, args.max_backoff_seconds)
                log_event(
                    logging.ERROR,
                    "daemon_cycle_failed",
                    cycle=cycle,
                    attempt=attempt,
                    backoff_seconds=backoff,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                attempt += 1
                if args.max_cycles and cycle >= args.max_cycles:
                    log_event(
                        logging.INFO, "daemon_stopped", cycle=cycle, reason="max_cycles_reached"
                    )
                    break
                log_event(
                    logging.WARNING, "daemon_backoff", backoff_seconds=backoff, attempt=attempt
                )
                time.sleep(backoff)
                continue

            if args.max_cycles and cycle >= args.max_cycles:
                log_event(logging.INFO, "daemon_stopped", cycle=cycle, reason="max_cycles_reached")
                break

            time.sleep(max(args.poll_interval_seconds, 1))
    finally:
        shutdown_langfuse_client()


def main() -> None:
    run_daemon(parse_args())


if __name__ == "__main__":
    main()
