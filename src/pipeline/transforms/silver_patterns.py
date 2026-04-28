from __future__ import annotations

import re
import unicodedata
from typing import cast

import pandas as pd

EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])(?:\+55\s?)?(?:\(?\d{2}\)?\s?)?(?:9?\d{4})-?\d{4}(?![A-Za-z0-9_])"
)
CPF_PATTERN = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")
CEP_PATTERN = re.compile(r"\b\d{5}-?\d{3}\b")
PLATE_PATTERN = re.compile(r"\b[A-Z]{3}[0-9][A-Z0-9][0-9]{2}\b", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
PRICE_PATTERN = re.compile(r"r\$\s?(\d[\d.]*(?:,\d{2})?)", re.IGNORECASE)
PRICE_OBJECTION_PATTERN = re.compile(
    r"\b(caro|cara|preco alto|muito caro|acima do orçamento|acima do orcamento|desconto|"
    r"parcel[ao]s?|mensalidade|cotacao|cotação|orcamento|orçamento)\b",
    re.IGNORECASE,
)
URGENCY_STRONG_PATTERN = re.compile(
    r"\b(urgente|hoje mesmo|agora|imediat|quanto antes|pra hoje|para hoje|fechar hoje)\b",
    re.IGNORECASE,
)
URGENCY_MODERATE_PATTERN = re.compile(
    r"\b(essa semana|esta semana|amanha|amanhã|rapido|rápido|prioridade|preciso logo)\b",
    re.IGNORECASE,
)
COMPETITOR_COMPARISON_PATTERN = re.compile(
    r"\b(cobriu|cobrou|melhor que|mais barato|mais caro|compar|concorrente|outra seguradora)\b",
    re.IGNORECASE,
)
EMAIL_PROVIDER_DOMAIN_PATTERN = re.compile(
    r"\b[A-Z0-9._%+-]+@([A-Z0-9.-]+\.[A-Z]{2,})\b",
    re.IGNORECASE,
)
COMPETITOR_PATTERNS = {
    "porto_seguro": re.compile(r"\bporto seguro\b", re.IGNORECASE),
    "azul_seguros": re.compile(r"\bazul(?: seguros)?\b", re.IGNORECASE),
    "bradesco_seguros": re.compile(r"\bbradesco seguros\b", re.IGNORECASE),
    "sulamerica": re.compile(r"\bsul\s?america\b", re.IGNORECASE),
    "liberty_seguros": re.compile(r"\bliberty(?: seguros)?\b", re.IGNORECASE),
    "allianz": re.compile(r"\ballianz\b", re.IGNORECASE),
    "hdi_seguros": re.compile(r"\bhdi(?: seguros)?\b", re.IGNORECASE),
}
SINISTRO_PATTERNS = {
    "enchente": re.compile(r"\benchente|alagamento\b", re.IGNORECASE),
    "colisao": re.compile(r"\bbati\b|\bbatida\b|\bcolis[aã]o\b", re.IGNORECASE),
    "roubo_furto": re.compile(r"\broubaram\b|\bfurto\b|\broubo\b", re.IGNORECASE),
    "perda_total": re.compile(r"\bperda total\b", re.IGNORECASE),
    "sinistro_generico": re.compile(r"\bsinistro\b", re.IGNORECASE),
}
VEHICLE_MAKES = (
    "chevrolet",
    "volkswagen",
    "fiat",
    "ford",
    "toyota",
    "honda",
    "hyundai",
    "jeep",
    "renault",
    "nissan",
    "peugeot",
    "citroen",
    "mitsubishi",
    "kia",
    "bmw",
    "mercedes",
    "audi",
    "volvo",
    "byd",
    "gwm",
)
VEHICLE_MODELS = (
    "onix",
    "gol",
    "civic",
    "hb20",
    "corolla",
    "compass",
    "hr-v",
    "hrv",
    "kwid",
    "208",
    "toro",
    "argo",
    "mobi",
    "creta",
    "t cross",
    "t-cross",
    "nivus",
    "tracker",
    "pulse",
    "kicks",
)
STATUS_PRIORITY = {"failed": 0, "sent": 1, "delivered": 2, "read": 3}
SENSITIVE_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": EMAIL_PATTERN,
    "phone": PHONE_PATTERN,
    "cpf": CPF_PATTERN,
    "cep": CEP_PATTERN,
    "plate": PLATE_PATTERN,
}
POSITIVE_TONE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bobrigad[oa]s?\b"),
    re.compile(r"\b(valeu|agradeco|agradeco demais)\b"),
    re.compile(r"\b(perfeito|excelente|otim[oa]|maravilha|top)\b"),
    re.compile(r"\b(gostei|bom demais|muito bom)\b"),
    re.compile(r"\b(pode seguir|pode prosseguir|vamos seguir|quero seguir)\b"),
    re.compile(r"\b(vamos fechar|quero fechar|pode fechar|fechado|combinado)\b"),
    re.compile(r"\b(aprovado|aprovada|de acordo|ok pode)\b"),
)
NEGATIVE_TONE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(nao tenho interesse|sem interesse|nao quero|nao vou fechar)\b"),
    re.compile(r"\b(cancelar|cancelamento|desist\w*)\b"),
    re.compile(r"\b(reclam\w*|insatisfeit\w*|ruim|pessim\w*|horrivel)\b"),
    re.compile(r"\b(absurdo|um absurdo|surreal)\b"),
    re.compile(r"\b(desconfiad\w*|nao confio|falta de confianca)\b"),
    re.compile(r"\b(enrola\w*|enrolacao|demora demais|muito demorado)\b"),
    re.compile(r"\b(caro demais|muito caro)\b"),
)
CONVERSATION_SENTIMENT_LABELS = frozenset({"positivo", "neutro", "negativo", "sem_evidencia"})
CONVERSATION_SENTIMENT_SUPPORT_LEVELS = frozenset({"fraco", "moderado", "forte", "sem_evidencia"})


def _normalize_ascii(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return normalized.encode("ascii", errors="ignore").decode("ascii")


def _normalize_for_match(value: str) -> str:
    normalized = _normalize_ascii(value).lower()
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _safe_string(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(cast(float | int | str, value))
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int:
    converted = _safe_float(value)
    if converted is None:
        return 0
    return int(converted)
