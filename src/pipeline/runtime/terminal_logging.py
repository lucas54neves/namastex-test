from __future__ import annotations

import logging
import sys
from typing import Any

LOGGER_NAME = "pipeline.runtime"


def configure_terminal_logging() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))

    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(level: int, event: str, **context: Any) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.log(level, _format_message(event, context))


def _format_message(event: str, context: dict[str, Any]) -> str:
    if not context:
        return event

    parts = [event]
    for key, value in context.items():
        if value is None:
            continue
        parts.append(f"{key}={_format_value(value)}")
    return " ".join(parts)


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
