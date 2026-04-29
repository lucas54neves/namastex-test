---
title: Silver Module Facade Documentation — Anchor Comment and Submodule Navigation Map
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [architecture, documentation, silver, maintainability, readability]
---

# Introduction

`src/pipeline/transforms/silver.py` is a deliberate re-export facade: after the Phase 1 and Phase 2 decompositions, the module's role changed from owning all Silver logic to serving as the canonical public entry point that reexports symbols from eight submodules. The file contains no transformational logic of its own.

A reader who opens `silver.py` expecting to find Silver transformation code encounters only imports. Without context, this looks like an incomplete or scaffolded file rather than a deliberate architectural choice. This spec defines the documentation anchor and navigation map that must appear at the top of `silver.py` to make the facade pattern immediately visible and self-explanatory.

## 1. Purpose & Scope

**Purpose:** Define the required content, placement, and format of the documentation anchor in `silver.py` that explains the facade pattern, lists the canonical submodule for each responsibility, and provides a navigation map for common entry points.

**Scope:**

Files modified:
- `src/pipeline/transforms/silver.py` — adds a module-level docstring and a `# FACADE` section header above the reexports

Files unchanged:
- All submodules (`silver_patterns.py`, `silver_masking.py`, `silver_signals.py`, `silver_context.py`, `silver_dedup.py`, `silver_aggregators.py`, `gold_segments.py`, `gold.py`)
- All tests
- `config/pipeline_spec.json`
- Any Parquet schema

**Out of scope:**
- Adding docstrings to submodule functions
- Generating HTML or Sphinx documentation
- Changes to `__all__` or exported symbols

**Intended audience:** Engineers reading `silver.py` for the first time, reviewers, and evaluators of the pipeline deliverable.

## 2. Definitions

| Term | Definition |
|---|---|
| Facade module | A module whose only role is to reexport symbols from submodules, providing a stable public surface while hiding internal decomposition. |
| Canonical owner | The submodule where a symbol is actually defined (not just reexported). |
| Navigation map | A structured comment or docstring that lists each responsibility and the file that owns it, enabling a reader to jump directly to the relevant submodule. |
| Module-level docstring | A string literal as the first statement in a Python module, returned by `module.__doc__`. Displayed by `help(module)` and picked up by documentation generators. |

## 3. Requirements, Constraints & Guidelines

### Module-level docstring

- **REQ-001**: `silver.py` MUST have a module-level docstring as its first statement (before `from __future__ import annotations`). The docstring MUST explain in one paragraph that the module is a re-export facade and state the reason.
- **REQ-002**: The docstring MUST include a table or structured list mapping each responsibility to its canonical submodule. The map MUST cover at minimum:

| Responsibility | Canonical module |
|---|---|
| Regex patterns and domain constants | `silver_patterns.py` |
| PII masking functions | `silver_masking.py` |
| Signal extraction and sentiment derivation | `silver_signals.py` |
| Conversation and lead context building | `silver_context.py` |
| Semantic deduplication | `silver_dedup.py` |
| Pandas aggregation helpers | `silver_aggregators.py` |
| Gold segmentation logic | `gold_segments.py` |
| Gold build pipeline | `gold.py` |
| Silver build pipeline (entry point) | `silver.py` (this file) |

- **REQ-003**: The docstring MUST list the three primary public entry points for callers: `build_silver`, `build_silver_leads`, and `load_bronze_frame` (or its Bronze-spec replacement `build_bronze`), with a one-line description of each.

### Section header comment

- **REQ-010**: Immediately before the first `from pipeline.transforms.silver_patterns import` line, a `# --- Reexports (facade) ---` comment MUST be placed to visually separate the docstring from the reexport block.
- **REQ-011**: The comment block MUST NOT repeat the full navigation map; it MUST reference the docstring: `# See module docstring for the canonical owner of each symbol.`

### Style constraints

- **CON-001**: The docstring MUST NOT exceed 40 lines. Conciseness is required; this is not a tutorial.
- **CON-002**: The docstring MUST be written in English.
- **CON-003**: The docstring MUST NOT reference issue numbers, PR titles, or specific commit hashes — those are ephemeral references.
- **CON-004**: No change may alter `silver.__all__` or any exported symbol.
- **CON-005**: All 351 existing tests MUST pass after the change (documentation-only change, no logic modification).

### Guidelines

- **GUD-001**: Use a reStructuredText or plain-prose style for the docstring. Avoid Markdown inside the docstring (Python's `help()` does not render Markdown).
- **GUD-002**: The navigation map inside the docstring SHOULD use a fixed-width aligned format so it is readable both in `help()` output and in a code editor without syntax highlighting.

## 4. Interfaces & Data Contracts

### Required docstring structure

```python
"""
Silver transformation layer — public facade.

This module is the stable public entry point for the Silver stage of the
Medallion pipeline. It contains no transformation logic of its own; all
logic lives in the submodules listed below. Imports are reexported here so
that callers always use ``pipeline.transforms.silver`` as the import path,
regardless of internal reorganisation.

Primary entry points
--------------------
build_silver(df, compiled_plan=None) -> pd.DataFrame
    Build the Silver messages view from a Bronze DataFrame.

build_silver_leads(silver_messages) -> pd.DataFrame
    Aggregate the messages view into a per-lead view.

load_bronze_frame(source_path) -> pd.DataFrame
    Read the raw Parquet source and coerce the timestamp column.

Canonical submodule map
-----------------------
Responsibility                          Module
--------------------------------------  --------------------------------
Regex patterns and domain constants     silver_patterns.py
PII masking functions                   silver_masking.py
Signal extraction and sentiment         silver_signals.py
Conversation and lead context           silver_context.py
Semantic deduplication                  silver_dedup.py
Pandas aggregation helpers              silver_aggregators.py
Gold segmentation logic                 gold_segments.py
Gold build pipeline                     gold.py
Silver build pipeline (this file)       silver.py
"""
```

### Placement in file

```python
"""...(docstring above)..."""

from __future__ import annotations

import json

import pandas as pd

# --- Reexports (facade) ---
# See module docstring for the canonical owner of each symbol.
from pipeline.transforms.silver_patterns import (  # noqa: F401
    ...
)
```

## 5. Acceptance Criteria

- **AC-001**: Given the docstring is added, when `python -c "import pipeline.transforms.silver; help(pipeline.transforms.silver)"` is executed, then the output contains the text `"Silver transformation layer — public facade"`.
- **AC-002**: Given the docstring is added, when `pipeline.transforms.silver.__doc__` is evaluated in a Python REPL, then it returns a non-empty string starting with `"Silver transformation layer"`.
- **AC-003**: Given the change is applied, when `venv/bin/python -m pytest -q` is executed, then all 351 existing tests pass without modification.
- **AC-004**: Given the change is applied, when `ruff check src/` is executed, then zero errors are reported.
- **AC-005**: Given the change is applied, when `wc -l src/pipeline/transforms/silver.py` is executed, then the result has increased by no more than 40 lines relative to the pre-change count.
- **AC-006**: Given the docstring is added, when any symbol previously importable from `silver.py` is imported, then the import succeeds without error (no regression in `__all__` or reexports).
- **AC-007**: Given the docstring is added, when `grep -n "build_silver\|build_silver_leads\|load_bronze_frame" src/pipeline/transforms/silver.py` is executed in the docstring section (lines 1–45), then all three entry points are mentioned.

## 6. Test Automation Strategy

- **Test levels**: Documentation change only. No new test file is required.
- **Docstring test**: Add one assertion to `tests/test_transforms.py`:
  ```python
  def test_silver_module_has_facade_docstring():
      import pipeline.transforms.silver as silver_mod
      assert silver_mod.__doc__ is not None
      assert "facade" in silver_mod.__doc__.lower()
      assert "build_silver" in silver_mod.__doc__
  ```
- **Regression**: `venv/bin/python -m pytest -q` — covers AC-003.
- **Ruff check**: `venv/bin/python -m ruff check src/` — covers AC-004.
- **CI/CD**: Both checks are already part of the existing CI pipeline. No new step required.

## 7. Rationale & Context

**Why this matters for evaluators:** A technical evaluator reading the repository for the first time will open `silver.py` expecting to find the Silver transformation logic. Encountering only imports without explanation creates ambiguity: is this a stub? Is the implementation missing? A clear docstring eliminates that question in the first five seconds of reading.

**Why a docstring and not a README or separate doc file:** The docstring is co-located with the code. It is the only documentation that cannot drift silently — if the facade pattern changes, the docstring is in the same file that changes. A separate doc file would require a separate update discipline.

**Why the navigation map belongs in the docstring:** The submodule map is the most useful piece of information a reader needs when landing on `silver.py`. Putting it in the docstring makes it accessible via `help()`, IDEs' hover documentation, and code review tools, without requiring the reader to locate a separate file.

**Why this is a separate spec and not part of a larger refactoring spec:** The change is zero-risk (documentation only, no logic), can be implemented in under 30 minutes, and has immediate evaluability impact. Bundling it with a structural change would delay its delivery unnecessarily.

## 8. Dependencies & External Integrations

### Technology Platform Dependencies
- **PLT-001**: Python 3.11 — standard docstring mechanism. No new packages or tools required.

### Internal Module Dependencies
- **INT-001**: No module dependencies change. The docstring is a string literal in `silver.py`; it has no impact on the import graph.

## 9. Examples & Edge Cases

```python
# Verify in a Python session
import pipeline.transforms.silver as s
print(s.__doc__)
# Expected output starts with:
# "Silver transformation layer — public facade."
# ...followed by the entry point list and submodule map.

# Edge case: docstring encoding
# The docstring MUST NOT contain non-ASCII characters.
# The navigation map uses plain ASCII dashes and spaces for alignment,
# not Unicode box-drawing characters, to ensure terminal compatibility.

# Edge case: ruff E501 (line too long)
# All lines in the docstring MUST be at most 88 characters wide
# to comply with the ruff configuration in pyproject.toml.
```

## 10. Validation Criteria

- `venv/bin/python -m pytest -q` → all 351 tests pass
- `python -c "import pipeline.transforms.silver; print(pipeline.transforms.silver.__doc__)"` → prints non-empty string containing "facade"
- `venv/bin/python -m ruff check src/` → zero errors
- `wc -l src/pipeline/transforms/silver.py` → increased by no more than 40 lines
- No symbol previously importable from `pipeline.transforms.silver` becomes unavailable

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-module-cyclomatic-decomposition.md` — Phase 1 that created the facade pattern
- `spec/spec-architecture-decomposition-phase-two.md` — Phase 2 that completed the facade
- `spec/spec-architecture-bronze-layer-enrichment.md` — migration of `load_bronze_frame` to `bronze.py`, which will update the facade's entry point list
