from __future__ import annotations

import ast
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import pandas as pd

from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import write_json
from pipeline.runtime.terminal_logging import log_event

_SAFE_EVAL_BLOCKLIST = frozenset(
    {
        "import",
        "exec",
        "eval",
        "__",
        "lambda",
        "open",
        "os",
        "sys",
        "subprocess",
        "globals",
        "locals",
        "getattr",
        "setattr",
        "delattr",
    }
)

_SAFE_EVAL_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.UnaryOp,
    ast.Not,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Name,
    ast.Constant,
    ast.List,
    ast.Load,
)


def _eval_node(node: ast.AST, df: pd.DataFrame) -> Any:
    if isinstance(node, ast.BoolOp):
        parts = [_eval_node(v, df) for v in node.values]
        result = parts[0]
        if isinstance(node.op, ast.And):
            for p in parts[1:]:
                result = result & p if isinstance(result, pd.Series) else (result and p)
        else:
            for p in parts[1:]:
                result = result | p if isinstance(result, pd.Series) else (result or p)
        return result

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        val = _eval_node(node.operand, df)
        return ~val if isinstance(val, pd.Series) else not val

    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, df)
        op = node.ops[0]
        right = _eval_node(node.comparators[0], df)
        if isinstance(op, ast.Eq):
            return left == right
        if isinstance(op, ast.NotEq):
            return left != right
        if isinstance(op, ast.Lt):
            return left < right
        if isinstance(op, ast.LtE):
            return left <= right
        if isinstance(op, ast.Gt):
            return left > right
        if isinstance(op, ast.GtE):
            return left >= right
        if isinstance(op, ast.In):
            if isinstance(right, list):
                return left.isin(right) if isinstance(left, pd.Series) else left in right
        if isinstance(op, ast.NotIn):
            if isinstance(right, list):
                return (~left.isin(right)) if isinstance(left, pd.Series) else left not in right
        raise ValueError(f"unsupported_operator:{type(op).__name__}")

    if isinstance(node, ast.Name):
        name = node.id
        if name in df.columns:
            return df[name]
        raise KeyError(name)

    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.List):
        return [_eval_node(elt, df) for elt in node.elts]

    raise ValueError(f"unsupported_node:{type(node).__name__}")


def safe_eval_condition(expr: str, df: pd.DataFrame) -> pd.Series:
    """
    Evaluate a restricted boolean expression over a pandas DataFrame.
    Returns pd.Series[bool]. Returns all-False on invalid or unsafe expressions.
    Never calls eval() or exec().
    """
    expr = re.sub(r"\s+", " ", expr.strip())

    for token in _SAFE_EVAL_BLOCKLIST:
        if token == "__":
            # Match dunder prefix (e.g. __class__, __import__) with a leading word boundary
            matched = bool(re.search(r"\b__", expr))
        elif token.isidentifier():
            # Word boundary prevents false positives where the token is a substring
            # of a legitimate value (e.g. "os" inside "bradesco_seguros")
            matched = bool(re.search(r"\b" + token + r"\b", expr))
        else:
            matched = token in expr
        if matched:
            log_event(
                logging.WARNING,
                "gold_plan_unsafe_expr_rejected",
                reason=f"blocklist:{token}",
                expr=expr[:120],
            )
            return pd.Series(False, index=df.index)

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        log_event(
            logging.WARNING,
            "gold_plan_unsafe_expr_rejected",
            reason=f"syntax_error:{exc}",
            expr=expr[:120],
        )
        return pd.Series(False, index=df.index)

    for node in ast.walk(tree):
        if not isinstance(node, _SAFE_EVAL_ALLOWED_NODES):
            log_event(
                logging.WARNING,
                "gold_plan_unsafe_expr_rejected",
                reason=f"disallowed_node:{type(node).__name__}",
                expr=expr[:120],
            )
            return pd.Series(False, index=df.index)

    try:
        result = _eval_node(tree.body, df)
        if isinstance(result, pd.Series):
            return result.astype(bool)
        return pd.Series(bool(result), index=df.index)
    except KeyError as exc:
        log_event(
            logging.WARNING,
            "gold_plan_missing_column",
            column=str(exc),
            expr=expr[:120],
        )
        return pd.Series(False, index=df.index)
    except Exception as exc:
        log_event(
            logging.WARNING,
            "gold_plan_unsafe_expr_rejected",
            reason=str(exc),
            expr=expr[:120],
        )
        return pd.Series(False, index=df.index)


_LEADS_SAMPLE_ROWS = 10
_MESSAGES_SCHEMA_MAX_VALUES = 5
_GOLD_DESIGNER_PROMPT = """
You are a data analyst designing analytical columns for a Gold data layer.
You will receive a sample of Silver lead data and Silver message data from a vehicle insurance CRM.
Your task is to design novel, insightful analytical columns that go beyond basic examples.

## Data Dictionary Context
{data_dictionary}

## Silver Leads Sample (up to {sample_rows} rows)
{leads_sample}

## Silver Messages Schema (all columns — use these names for source_col and when expressions)
{messages_sample}

## Forbidden Columns (DO NOT use as source fields)
{forbidden_columns}

## Derivation Logic Types Allowed
- aggregation:
  {{"type": "aggregation", "agg_fn": "<sum|mean|max|min|count>", "source_col": "<column>"}}
  Note: source_col can be any column from Silver Leads OR Silver Messages samples above.
- conditional_bucket:
  {{"type": "conditional_bucket", "conditions": [{{"when": "<expr>", "then": "<label>"}},
  ..., {{"else": "<label>"}}]}}
- llm_enriched: {{"type": "llm_enriched", "field": "<existing_llm_column>"}}

## Expression Syntax for conditional_bucket (STRICT PYTHON SUBSET — NOT SQL)

"when" expressions are evaluated as Python boolean expressions. Only the following syntax is valid.
Expressions that violate these rules are silently treated as False (all rows fall to "else").

ALLOWED:
  - Comparisons:   ==   !=   <   <=   >   >=
  - Membership:    column in ["a", "b"]   /   column not in ["a"]
  - Boolean ops (lowercase only):  and   or   not
  - Boolean literals:  True   False   (capital first letter)
  - Column reference:  just the bare name, no quotes
  - String literals:   'value'
  - Number literals:   42   3.14

FORBIDDEN — these will be silently rejected:
  - Uppercase AND / OR / NOT  → use lowercase and / or / not
  - Single = for equality     → use ==
  - Lowercase true / false    → use True / False
  - SQL BETWEEN x AND y       → use: col >= x and col <= y
  - SQL LIKE / ILIKE          → use: 'substr' in col  (if col is a string column)
  - SQL IS NULL / IS NOT NULL → use: col == None  /  col != None
  - SQL EXTRACT / COALESCE / any function call
  - Arithmetic inside conditions (+, -, *, /)  → not supported

CORRECT examples:
  "message_count > 10 and has_competitor_signal == True"
  "engagement_bucket in ['media', 'longa']"
  "latest_outcome == 'venda_fechada'"
  "mentioned_sinistro"
  "message_count >= 5 and message_count <= 14"
  "not mentioned_competitor and data_shared_score >= 3"

WRONG examples (will be rejected):
  "message_count BETWEEN 5 AND 14"              → use: message_count >= 5 and message_count <= 14
  "has_competitor_signal = true"                → use: has_competitor_signal == True
  "message_count >= 5 AND avg_response_time_sec <= 120"  → use lowercase and
  "EXTRACT(HOUR FROM first_seen_at) >= 18"      → not supported
  "(a + b + c) >= 2"                            → arithmetic not supported

## Requirements
- Include at least 2 engagement-related columns
- Include at least 1 intent/commercial signal column
- Include at least 1 behavioral/persona column
- Include at least 1 contextual column
- Each conditional_bucket column MUST include a segment_values list
- Favor novel segmentation dimensions, not just repackaging existing columns
- Data types: float64, int64, string, or bool

## Output Format (JSON only, no prose)
{{
  "columns": [
    {{
      "name": "<column_name>",
      "data_type": "<float64|int64|string|bool>",
      "derivation_logic": {{}},
      "rationale": "<why this column is valuable>",
      "segment_values": ["<val1>", "<val2>"] or null
    }}
  ],
  "llm_rationale": "<overall rationale for this set of columns>"
}}
"""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class GoldColumnDefinition:
    name: str
    data_type: Literal["float64", "int64", "string", "bool"]
    derivation_logic: dict[str, Any]
    rationale: str
    segment_values: list[str] | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "derivation_logic": self.derivation_logic,
            "rationale": self.rationale,
            "segment_values": self.segment_values,
        }


@dataclass
class GoldColumnPlan:
    columns: list[GoldColumnDefinition]
    source: Literal["llm", "deterministic_fallback"]
    generated_at_utc: str
    llm_rationale: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "columns": [c.as_dict() for c in self.columns],
            "source": self.source,
            "generated_at_utc": self.generated_at_utc,
            "llm_rationale": self.llm_rationale,
        }


def _fallback_gold_plan() -> GoldColumnPlan:
    """Reproduces the deterministic segment columns from add_gold_segments()."""
    columns = [
        GoldColumnDefinition(
            name="lead_temperature",
            data_type="string",
            derivation_logic={
                "type": "conditional_bucket",
                "conditions": [
                    {
                        "when": "engagement_bucket == 'lead_frio' and data_shared_score == 0",
                        "then": "frio",
                    },
                    {
                        "when": "engagement_bucket in ['media', 'longa'] or data_shared_score >= 3",
                        "then": "quente",
                    },
                    {"else": "morno"},
                ],
            },
            rationale="Lead temperature based on engagement and data sharing.",
            segment_values=["frio", "morno", "quente"],
        ),
        GoldColumnDefinition(
            name="price_sensitivity",
            data_type="string",
            derivation_logic={
                "type": "conditional_bucket",
                "conditions": [
                    {
                        "when": "mentioned_competitor or avg_quoted_price == avg_quoted_price",
                        "then": "alta",
                    },
                    {"else": "baixa"},
                ],
            },
            rationale="Price sensitivity based on competitor mentions and quoted prices.",
            segment_values=["baixa", "alta"],
        ),
        GoldColumnDefinition(
            name="contact_readiness",
            data_type="string",
            derivation_logic={
                "type": "conditional_bucket",
                "conditions": [
                    {"when": "data_shared_score == 2", "then": "media"},
                    {"when": "data_shared_score >= 3", "then": "alta"},
                    {"else": "baixa"},
                ],
            },
            rationale="Contact readiness based on how much PII data was shared.",
            segment_values=["baixa", "media", "alta"],
        ),
        GoldColumnDefinition(
            name="risk_signal",
            data_type="string",
            derivation_logic={
                "type": "conditional_bucket",
                "conditions": [
                    {"when": "mentioned_sinistro and contains_cpf", "then": "alto"},
                    {"when": "mentioned_sinistro or duplicate_events_removed > 0", "then": "medio"},
                    {"else": "baixo"},
                ],
            },
            rationale="Risk signal based on sinistro mentions and data quality.",
            segment_values=["baixo", "medio", "alto"],
        ),
        GoldColumnDefinition(
            name="persona_profile",
            data_type="string",
            derivation_logic={
                "type": "conditional_bucket",
                "conditions": [
                    {"when": "mentioned_sinistro", "then": "sinistrado"},
                    {
                        "when": "mentioned_competitor and avg_quoted_price == avg_quoted_price",
                        "then": "comparador",
                    },
                    {"else": "prospect_basico"},
                ],
            },
            rationale="Persona classification for targeting.",
            segment_values=["prospect_basico", "sinistrado", "comparador", "engajado"],
        ),
        GoldColumnDefinition(
            name="audience_segment",
            data_type="string",
            derivation_logic={
                "type": "conditional_bucket",
                "conditions": [
                    {"when": "persona_profile == 'sinistrado'", "then": "pos_sinistro"},
                    {"when": "persona_profile == 'comparador'", "then": "comparacao_ativa"},
                    {"else": "nutricao_basica"},
                ],
            },
            rationale="Audience segment for campaign targeting.",
            segment_values=["nutricao_basica", "pos_sinistro", "comparacao_ativa", "alto_valor"],
        ),
    ]
    return GoldColumnPlan(
        columns=columns,
        source="deterministic_fallback",
        generated_at_utc=_utc_now(),
        llm_rationale=None,
    )


def _load_data_dictionary(paths: PipelinePaths) -> str:
    dd_path = paths.docs / "data-dictionary-data-ai-engineering.md"
    if dd_path.exists():
        content = dd_path.read_text(encoding="utf-8")
        return content[:4000]
    return "(data dictionary unavailable)"


def _sample_df(df: pd.DataFrame, n: int = _LEADS_SAMPLE_ROWS) -> str:
    sample = df.sample(min(n, len(df)), random_state=42) if len(df) > n else df
    cols_to_drop = [c for c in sample.columns if "body" in c.lower() or "masked" in c.lower()]
    sample = sample.drop(columns=cols_to_drop, errors="ignore")
    return str(sample.to_json(orient="records", date_format="iso"))


def _schema_df(df: pd.DataFrame, max_values: int = _MESSAGES_SCHEMA_MAX_VALUES) -> str:
    """Compact column schema with name, type, and representative unique values.
    Used for large tables (messages) to avoid blowing up the prompt token count."""
    cols_to_drop = [c for c in df.columns if "body" in c.lower() or "masked" in c.lower()]
    df = df.drop(columns=cols_to_drop, errors="ignore")
    schema: list[dict[str, Any]] = []
    for col in df.columns:
        series = df[col].dropna()
        dtype = str(df[col].dtype)
        raw_vals = series.unique()[:max_values].tolist()
        safe_vals: list[Any] = []
        for v in raw_vals:
            try:
                json.dumps(v)
                safe_vals.append(v)
            except (TypeError, ValueError):
                safe_vals.append(str(v))
        schema.append({"name": col, "type": dtype, "sample_values": safe_vals})
    return json.dumps(schema, ensure_ascii=False)


def _validate_plan(
    plan: GoldColumnPlan, spec: dict[str, Any], silver_columns: set[str]
) -> tuple[GoldColumnPlan, list[str]]:
    forbidden = set(spec.get("forbidden_columns", []))
    seen_names: set[str] = set()
    valid_columns: list[GoldColumnDefinition] = []
    violations: list[str] = []

    for col in plan.columns:
        if col.name in seen_names:
            violations.append(f"duplicate_column_name:{col.name}")
            continue
        seen_names.add(col.name)

        logic = col.derivation_logic
        logic_type = logic.get("type")

        if logic_type == "conditional_bucket" and not col.segment_values:
            violations.append(f"missing_segment_values:{col.name}")
            continue

        if logic_type == "aggregation":
            source_col = logic.get("source_col", "")
            if source_col in forbidden:
                violations.append(f"privacy_violation:{col.name}:uses_forbidden_col:{source_col}")
                log_event(
                    logging.WARNING,
                    "gold_plan_privacy_violation",
                    column=col.name,
                    source_col=source_col,
                )
                continue

        valid_columns.append(col)

    return GoldColumnPlan(
        columns=valid_columns,
        source=plan.source,
        generated_at_utc=plan.generated_at_utc,
        llm_rationale=plan.llm_rationale,
    ), violations


def _parse_llm_plan(text: str) -> GoldColumnPlan:
    raw = text.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    parsed = json.loads(raw.strip())
    columns = []
    for col_data in parsed.get("columns", []):
        columns.append(
            GoldColumnDefinition(
                name=str(col_data["name"]),
                data_type=col_data.get("data_type", "string"),
                derivation_logic=col_data.get("derivation_logic", {}),
                rationale=str(col_data.get("rationale", "")),
                segment_values=col_data.get("segment_values"),
            )
        )
    return GoldColumnPlan(
        columns=columns,
        source="llm",
        generated_at_utc=_utc_now(),
        llm_rationale=str(parsed.get("llm_rationale", "")),
    )


def _ensure_minimum_columns(
    plan: GoldColumnPlan,
    fallback: GoldColumnPlan,
) -> GoldColumnPlan:
    """Supplement with fallback columns if LLM plan doesn't meet the floor requirements."""
    plan_names = {c.name for c in plan.columns}
    engagement_cols = [c for c in plan.columns if "engagement" in c.name or "message" in c.name]
    commercial_cols = [
        c
        for c in plan.columns
        if any(k in c.name for k in ("price", "commercial", "intent", "competitor"))
    ]
    persona_cols = [
        c for c in plan.columns if any(k in c.name for k in ("persona", "audience", "profile"))
    ]
    contextual_cols = [
        c
        for c in plan.columns
        if any(k in c.name for k in ("context", "lifecycle", "latency", "provider", "risk"))
    ]

    extra: list[GoldColumnDefinition] = []
    for col in fallback.columns:
        if col.name not in plan_names:
            if len(engagement_cols) < 2 and "engagement" in col.name:
                extra.append(col)
                engagement_cols.append(col)
            elif len(commercial_cols) < 1 and any(
                k in col.name for k in ("price", "commercial", "intent")
            ):
                extra.append(col)
                commercial_cols.append(col)
            elif len(persona_cols) < 1 and any(k in col.name for k in ("persona", "audience")):
                extra.append(col)
                persona_cols.append(col)
            elif len(contextual_cols) < 1 and any(k in col.name for k in ("risk", "context")):
                extra.append(col)
                contextual_cols.append(col)

    if not extra:
        return plan

    return GoldColumnPlan(
        columns=plan.columns + extra,
        source=plan.source,
        generated_at_utc=plan.generated_at_utc,
        llm_rationale=plan.llm_rationale,
    )


def design_gold_columns(
    silver_leads_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    spec: dict[str, Any],
    paths: PipelinePaths,
    llm_call: Callable[..., str] | None = None,
    compiled_plan: dict[str, Any] | None = None,
    timeout: float = 90.0,
) -> GoldColumnPlan:
    plan_path = paths.agent_decisions / "latest_gold_column_plan.json"
    fallback = _fallback_gold_plan()

    if llm_call is not None:
        try:
            data_dict = _load_data_dictionary(paths)
            leads_sample = _sample_df(
                silver_leads_df, min(_LEADS_SAMPLE_ROWS, len(silver_leads_df))
            )
            messages_sample = _schema_df(silver_messages_df)
            forbidden = spec.get("forbidden_columns", [])

            prompt = _GOLD_DESIGNER_PROMPT.format(
                data_dictionary=data_dict,
                sample_rows=_LEADS_SAMPLE_ROWS,
                leads_sample=leads_sample,
                messages_sample=messages_sample,
                forbidden_columns=json.dumps(forbidden),
            )
            text = llm_call(prompt, compiled_plan or {}, timeout)
            raw_plan = _parse_llm_plan(text)
            silver_cols = set(silver_leads_df.columns) | set(silver_messages_df.columns)
            validated_plan, violations = _validate_plan(raw_plan, spec, silver_cols)

            if violations:
                log_event(logging.WARNING, "gold_plan_violations", violations=violations)

            if len(validated_plan.columns) < 5:
                log_event(
                    logging.WARNING, "gold_plan_too_few_columns", count=len(validated_plan.columns)
                )
                validated_plan = _ensure_minimum_columns(validated_plan, fallback)

            if len(validated_plan.columns) < 5:
                log_event(
                    logging.WARNING,
                    "gold_plan_fallback_used",
                    reason="minimum_column_floor_not_met",
                )
                plan = fallback
            else:
                plan = validated_plan

            log_event(
                logging.INFO,
                "gold_column_plan_built",
                source=plan.source,
                column_count=len(plan.columns),
            )
        except Exception as exc:
            log_event(logging.WARNING, "gold_designer_llm_failed", error=str(exc))
            plan = fallback
    else:
        plan = fallback

    write_json(plan.as_dict(), plan_path)
    return plan


def apply_gold_column_plan(
    gold_df: pd.DataFrame,
    plan: GoldColumnPlan,
    silver_leads_df: pd.DataFrame | None = None,
    silver_messages_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Apply GoldColumnPlan columns to an existing Gold DataFrame."""
    result = gold_df.copy()

    for col_def in plan.columns:
        if col_def.name in result.columns:
            continue

        logic = col_def.derivation_logic
        logic_type = logic.get("type")

        try:
            if logic_type == "aggregation":
                source_col = logic.get("source_col", "")
                agg_fn = logic.get("agg_fn", "sum")
                if source_col in result.columns:
                    # Gold is already aggregated per lead from Silver; agg_fn documents original
                    # semantic — do not re-aggregate scalar values
                    if agg_fn in ("sum", "count", "max", "min"):
                        result[col_def.name] = result[source_col]
                    elif agg_fn in ("mean", "avg"):
                        count_col = source_col + "_count"
                        if count_col in result.columns:
                            result[col_def.name] = result[source_col] / result[count_col].replace(
                                0, 1
                            )
                        else:
                            log_event(
                                logging.WARNING,
                                "gold_agg_mean_no_denominator",
                                column=col_def.name,
                                source_col=source_col,
                            )
                            result[col_def.name] = result[source_col]
                    else:
                        log_event(
                            logging.WARNING,
                            "gold_agg_unknown_fn",
                            agg_fn=agg_fn,
                            column=col_def.name,
                        )
                        result[col_def.name] = result[source_col]
                elif (
                    silver_messages_df is not None
                    and "lead_key" in silver_messages_df.columns
                    and source_col in silver_messages_df.columns
                ):
                    _agg_fn_map = {
                        "sum": "sum",
                        "count": "count",
                        "max": "max",
                        "min": "min",
                        "mean": "mean",
                        "avg": "mean",
                    }
                    pandas_agg = _agg_fn_map.get(agg_fn)
                    if pandas_agg is None:
                        log_event(
                            logging.WARNING,
                            "gold_agg_unknown_fn",
                            agg_fn=agg_fn,
                            column=col_def.name,
                        )
                        pandas_agg = "sum"
                    agg_result = (
                        silver_messages_df.groupby("lead_key", dropna=False)[source_col]
                        .agg(pandas_agg)
                        .rename(col_def.name)
                        .reset_index()
                    )
                    result = result.merge(agg_result, on="lead_key", how="left")
                else:
                    log_event(
                        logging.WARNING,
                        "gold_plan_missing_source_col",
                        column=col_def.name,
                        source_col=source_col,
                    )
                    result[col_def.name] = None

            elif logic_type == "conditional_bucket":
                conditions = logic.get("conditions", [])
                default_val = "unknown"
                for cond in conditions:
                    if "else" in cond:
                        default_val = cond["else"]
                        break
                series = pd.Series(default_val, index=result.index, dtype="object")
                # Apply in reverse so first (highest-priority) condition wins
                when_conditions = [c for c in conditions if "when" in c and "then" in c]
                for cond in reversed(when_conditions):
                    try:
                        mask = safe_eval_condition(cond["when"], result)
                        series[mask] = cond["then"]
                    except Exception:
                        pass
                result[col_def.name] = series

            elif logic_type == "llm_enriched":
                enriched_field = logic.get("field", "")
                if enriched_field and enriched_field in result.columns:
                    result[col_def.name] = result[enriched_field]
                else:
                    result[col_def.name] = None

        except Exception as exc:
            log_event(
                logging.WARNING,
                "gold_plan_column_apply_failed",
                column=col_def.name,
                error=str(exc),
            )
            result[col_def.name] = None

    return result
