from __future__ import annotations

import os
from pathlib import Path

_AUTOLOAD_DONE = False
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _strip_quotes(value: str) -> str:
    normalized = value.strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
        return normalized[1:-1]
    return normalized


def load_dotenv_file(path: Path, override: bool = False) -> bool:
    if not path.exists():
        return False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key:
            continue
        if not override and env_key in os.environ:
            continue
        os.environ[env_key] = _strip_quotes(value)
    return True


def autoload_env() -> Path | None:
    global _AUTOLOAD_DONE
    if _AUTOLOAD_DONE:
        return None

    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[2] / ".env",
    ]
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if load_dotenv_file(resolved):
            _AUTOLOAD_DONE = True
            return resolved

    _AUTOLOAD_DONE = True
    return None


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    return default
