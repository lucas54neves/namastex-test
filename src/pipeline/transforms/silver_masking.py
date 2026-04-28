from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

from pipeline.transforms.silver_patterns import (
    CEP_PATTERN,
    CPF_PATTERN,
    EMAIL_PATTERN,
    PHONE_PATTERN,
    PLATE_PATTERN,
    SENSITIVE_PATTERNS,
    _normalize_for_match,
    _safe_string,
)


def _mask_digits(raw: str) -> str:
    return "".join("X" if char.isdigit() else char for char in raw)


def _mask_email(raw: str) -> str:
    return re.sub(r"[A-Za-z0-9]", "x", raw)


def _mask_alpha_numeric(raw: str) -> str:
    masked = []
    for char in raw:
        if char.isalpha():
            masked.append("X")
        elif char.isdigit():
            masked.append("9")
        else:
            masked.append(char)
    return "".join(masked)


def _mask_name_token(raw: str) -> str:
    return "".join("X" if char.isalpha() else char for char in raw)


def _is_masked_email(value: str) -> bool:
    return bool(re.fullmatch(r"[xX._%+-]+@[xX.-]+\.[xX]{2,}", value))


def _is_masked_plate(value: str) -> bool:
    normalized = value.upper()
    return bool(normalized) and set(normalized) <= {"X", "9", "-"}


def detect_sensitive_classes(value: object, classes: Iterable[str] | None = None) -> set[str]:
    text = _safe_string(value)
    requested = set(classes) if classes is not None else set(SENSITIVE_PATTERNS)
    return {
        name
        for name, pattern in SENSITIVE_PATTERNS.items()
        if name in requested and pattern.search(text)
    }


def detect_unmasked_sensitive_classes(
    value: object, classes: Iterable[str] | None = None
) -> set[str]:
    text = _safe_string(value)
    requested = set(classes) if classes is not None else set(SENSITIVE_PATTERNS)
    matches: set[str] = set()
    for name, pattern in SENSITIVE_PATTERNS.items():
        if name not in requested:
            continue
        for match in pattern.finditer(text):
            token = match.group(0)
            if name == "email" and _is_masked_email(token):
                continue
            if name == "plate" and _is_masked_plate(token):
                continue
            matches.add(name)
            break
    return matches


def _mask_known_names(text: str, names: Iterable[str]) -> str:
    masked = text
    for name in names:
        normalized = _normalize_for_match(name)
        if not normalized or len(normalized) < 3:
            continue
        tokens = [token for token in normalized.split(" ") if len(token) >= 2]
        if not tokens:
            continue
        pattern = re.compile(r"\b" + r"\s+".join(map(re.escape, tokens)) + r"\b", re.IGNORECASE)
        masked = pattern.sub(lambda match: _mask_name_token(match.group(0)), masked)
    return masked


def mask_sender_name(name: object) -> str:
    raw_name = _safe_string(name)
    return "".join("X" if char.isalpha() else char for char in raw_name)


def mask_message_body(row: pd.Series) -> str:
    text = _safe_string(row["message_body"])
    masked = text
    masked = EMAIL_PATTERN.sub(lambda match: _mask_email(match.group(0)), masked)
    masked = CPF_PATTERN.sub(lambda match: _mask_digits(match.group(0)), masked)
    masked = CEP_PATTERN.sub(lambda match: _mask_digits(match.group(0)), masked)
    masked = PHONE_PATTERN.sub(lambda match: _mask_digits(match.group(0)), masked)
    masked = PLATE_PATTERN.sub(lambda match: _mask_alpha_numeric(match.group(0).upper()), masked)
    known_names = {
        row.get("sender_name", ""),
        row.get("conversation_lead_name", ""),
        row.get("conversation_agent_name", ""),
    }
    masked = _mask_known_names(masked, known_names)
    return masked
