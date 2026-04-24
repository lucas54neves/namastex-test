from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _resolve_root(file_name: str | None, env: dict[str, str] | None = None) -> Path:
    if file_name:
        return Path(file_name).resolve().parents[1]
    runtime_env = dict(os.environ if env is None else env)
    config_dir = runtime_env.get("PIPELINE_CONFIG_DIR", "").strip()
    if config_dir:
        return Path(config_dir).resolve().parent
    return Path.cwd().resolve()


ROOT = _resolve_root(globals().get("__file__"), dict(os.environ))
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
    parser.add_argument("--input-file", help="Override the Bronze input parquet path.")
    parser.add_argument("--data-dir", help="Override the pipeline data directory.")
    parser.add_argument("--reports-dir", help="Override the pipeline reports directory.")
    parser.add_argument("--state-dir", help="Override the pipeline state directory.")
    parser.add_argument("--runtime-dir", help="Override the pipeline runtime directory.")
    parser.add_argument("--config-dir", help="Override the pipeline config directory.")
    return parser.parse_args()


def _runtime_path_overrides(args: argparse.Namespace) -> dict[str, str]:
    overrides = {
        "PIPELINE_INPUT_FILE": args.input_file,
        "PIPELINE_DATA_DIR": args.data_dir,
        "PIPELINE_REPORTS_DIR": args.reports_dir,
        "PIPELINE_STATE_DIR": args.state_dir,
        "PIPELINE_RUNTIME_DIR": args.runtime_dir,
        "PIPELINE_CONFIG_DIR": args.config_dir,
    }
    return {key: value for key, value in overrides.items() if value}


def main() -> None:
    args = parse_args()
    configure_terminal_logging()
    try:
        artifacts = run_pipeline(
            build_paths(ROOT, env=_runtime_path_overrides(args)),
            force=args.force,
        )
        print(json.dumps(artifacts_as_dict(artifacts), indent=2, ensure_ascii=False))
    finally:
        shutdown_langfuse_client()


if __name__ == "__main__":
    main()
