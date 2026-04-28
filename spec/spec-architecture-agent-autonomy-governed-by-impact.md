---
title: Architecture Specification for Governed-By-Impact Agent Autonomy in the Medallion Pipeline
version: 1.0
date_created: 2026-04-23
last_updated: 2026-04-23
owner: Lucas Neves
tags: [architecture, agent, autonomy, pipeline, governance, quality, llm]
---

# Introduction

This specification defines the target architecture required to evolve the current deterministic operational agent into a truly autonomous pipeline agent governed by impact-based controls.

The intended result is an agent that does not stop at diagnosis. It must be able to observe runtime evidence, formulate bounded structural or semantic changes, materialize candidate mutations, validate them in isolation, promote safe changes automatically, request approval for high-impact mutations, and revert changes when they degrade runtime or artifact quality.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, and validation rules for implementing governed-by-impact autonomy in the repository-local medallion pipeline.

Scope:

- Add an explicit autonomous change lifecycle for the pipeline agent.
- Define mutation classes, risk scoring, approval requirements, and promotion rules.
- Allow the agent to autonomously evolve selected parts of configuration, prompts, validation, and transformation behavior.
- Introduce candidate execution, artifact comparison, rollback, and impact measurement.
- Expand observability so each autonomous decision is auditable and attributable to evidence.
- Preserve the current Bronze, Silver, Gold architecture while extending the agent beyond static playbooks.

Out of scope:

- Replacing the existing medallion architecture with a different data platform.
- Granting unrestricted autonomy over destructive or privacy-sensitive mutations.
- Requiring external cloud orchestration as a baseline repository-local dependency.
- Eliminating deterministic fallback behavior or operator auditability.
- Requiring live third-party LLM providers for the baseline local runtime path.

Intended audience:

- Engineers implementing agent autonomy in this repository
- Reviewers evaluating alignment with the technical test
- Future maintainers responsible for operational governance and safety

Assumptions:

- The repository already contains deterministic Bronze, Silver, Gold, validation, planner, alerting, and runtime state modules.
- The baseline path with local execution and deterministic fallback remains a first-class supported mode.
- Published artifacts must continue to protect PII and preserve operational auditability.

## 2. Definitions

- **Autonomous Change Lifecycle**: The end-to-end sequence `observe -> diagnose -> propose -> classify -> materialize -> validate -> compare -> promote/revert -> record`.
- **Mutation Proposal**: A structured candidate change derived from runtime evidence and expressed in machine-readable form.
- **Impact Class**: The policy classification of a proposed change as low, medium, or high impact.
- **Low-Impact Mutation**: A change that does not break published contracts and is eligible for automatic promotion when safeguards pass.
- **Medium-Impact Mutation**: A change that affects runtime behavior or additive contracts, but remains backward compatible and may be auto-promoted only when explicitly allowed by policy.
- **High-Impact Mutation**: A change that alters privacy policy, published schema compatibility, destructive behavior, or other core contracts and always requires human approval before promotion.
- **Candidate Workspace**: An isolated execution context where the agent applies and validates a proposed mutation before promotion.
- **Promotion**: The act of accepting a candidate mutation into the active runtime contract.
- **Rollback**: The act of restoring the last known-good runtime contract and published artifact lineage after a failed or harmful mutation.
- **Policy Engine**: The ruleset that determines mutation eligibility, autonomy level, and approval requirements.
- **Evidence Bundle**: The structured set of runtime signals, metrics, diffs, and artifacts used to justify or reject a mutation.
- **Analytical Blind Spot**: A recurring pattern in source data or Gold outputs that indicates missing business signals, weak taxonomy coverage, or under-modeled semantics.
- **Change Efficacy**: The measured quality of a promoted mutation based on tests, validation outcomes, and post-promotion runtime metrics.
- **PII**: Personally Identifiable Information.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The system shall implement an explicit autonomous change lifecycle that continues beyond diagnosis into change proposal, validation, promotion, and rollback.
- **REQ-002**: Every mutation proposal shall be represented as structured data rather than prose-only reasoning.
- **REQ-003**: Every mutation proposal shall include a machine-readable rationale, expected impact, affected artifacts, and evidence summary.
- **REQ-004**: The agent shall classify each mutation as low, medium, or high impact before any candidate materialization occurs.
- **REQ-005**: Low-impact mutations shall be eligible for automatic promotion when all required gates pass.
- **REQ-006**: Medium-impact mutations shall be eligible for automatic promotion only when the policy engine marks the mutation family as safe for autonomous promotion and all required gates pass.
- **REQ-007**: High-impact mutations shall never be auto-promoted and shall instead persist a proposal for explicit human approval.
- **REQ-008**: The agent shall be able to autonomously mutate at least the following domains: prompt versions, non-breaking validation rules, additive heuristics, retry policies, cache policies, alert thresholds, non-breaking Gold feature definitions, and bounded transformation rules.
- **REQ-009**: The agent shall be able to propose but not auto-promote at least the following domains: privacy policy changes, destructive reprocessing behavior, published-field removals or renames, non-backward-compatible schema changes, and core taxonomy rewrites.
- **REQ-010**: The runtime shall materialize each candidate mutation in isolation and run validation before any promotion.
- **REQ-011**: Candidate validation shall include targeted tests, runtime validation checks, privacy checks, and a candidate pipeline execution over repository-local data.
- **REQ-012**: The runtime shall compare candidate artifacts against the active baseline and compute structured diffs before promotion.
- **REQ-013**: The comparison stage shall evaluate both contract safety and analytical impact.
- **REQ-014**: The system shall support automatic rollback when a candidate or promoted mutation causes test failures, validation failures, privacy violations, or policy-defined regressions.
- **REQ-015**: The system shall persist a full decision record for every proposal, including rejected, approved, promoted, rolled-back, and approval-pending outcomes.
- **REQ-016**: The system shall measure post-promotion efficacy for autonomous mutations and persist those outcomes as runtime evidence for future decisions.
- **REQ-017**: The system shall allow the agent to identify analytical blind spots and generate bounded proposals for new signals, segments, or conversation-level semantic classifications.
- **REQ-018**: The system shall preserve deterministic fallback behavior whenever LLM-derived planning or semantic inference is disabled, unavailable, invalid, or policy-restricted.
- **REQ-019**: The system shall keep the repository-local baseline executable without requiring network access or external credentials.
- **REQ-020**: The system shall expose a clear approval workflow for high-impact proposals, including proposal status, approver identity, approval timestamp, and applied outcome.
- **REQ-021**: The system shall maintain lineage between promoted mutations, runtime state, and the artifact versions produced under that mutation set.
- **REQ-022**: The system shall support automatic generation of targeted regression tests for mutation families that change runtime behavior, provided those generated tests are validated before promotion.
- **REQ-023**: The system shall support policy-driven autonomous evolution of additive Gold analytics that are privacy-safe and backward compatible.
- **REQ-024**: The system shall expose effectiveness metrics for the autonomous agent, including proposal volume, promotion rate, rollback rate, unresolved failure rate, and mutation efficacy by family.
- **REQ-025**: The agent shall support bounded LLM-assisted proposal generation for unknown failures or blind spots, but every promoted change shall still pass deterministic validation gates.

- **SEC-001**: No autonomous mutation shall weaken masking, publication safety, or privacy validation without explicit human approval.
- **SEC-002**: No autonomous mutation shall introduce raw PII into published Silver, `silver_conversations_llm`, Gold, reports, or decision records.
- **SEC-003**: Every decision record shall persist enough information to explain why a mutation was proposed, promoted, rejected, or rolled back.
- **SEC-004**: The policy engine shall treat destructive operations and non-backward-compatible publication changes as high-impact by default.
- **SEC-005**: Candidate execution shall not overwrite the active last-known-good artifacts until promotion is explicitly approved by policy.

- **CON-001**: The architecture shall preserve the current Bronze -> Silver -> Gold layering model.
- **CON-002**: The architecture shall not require the LLM to be the sole planner, executor, or validator of mutations.
- **CON-003**: The architecture shall preserve deterministic validation as the final authority for publish-safe and contract-safe promotion.
- **CON-004**: The runtime shall not auto-promote mutations when required evidence is missing, ambiguous, or inconsistent with policy.
- **CON-005**: The architecture shall remain operable in a repository-local virtual environment using the checked-in dataset and configuration.
- **CON-006**: Promotion and rollback behavior shall be idempotent for the same mutation identifier and baseline state.
- **CON-007**: Mutation classification shall be policy-based and machine-readable rather than encoded only in natural-language instructions.

- **GUD-001**: Prefer autonomous changes that are additive, reversible, and evidence-backed before allowing broader mutation families.
- **GUD-002**: Prefer explicit mutation templates for known change families over free-form code synthesis whenever the same result is achievable.
- **GUD-003**: Use LLM assistance for proposal generation, taxonomy exploration, and unknown-failure triage, but gate all resulting mutations with deterministic checks.
- **GUD-004**: Keep mutation scope small and isolated so candidate evaluation can attribute improvement or regression clearly.
- **GUD-005**: Store risk classification, evidence, and outcome in machine-readable records so the autonomy system can itself be audited and tuned.
- **GUD-006**: Favor backward-compatible additive analytics in Gold to demonstrate agentic improvement without destabilizing the delivery contract.

- **PAT-001**: Recommended autonomy pattern: runtime observation -> structured proposal -> policy classification -> isolated candidate materialization -> targeted validation -> artifact diff -> promotion or rollback -> efficacy monitoring.
- **PAT-002**: Recommended safety pattern: deterministic gates own privacy, schema safety, and publishability; LLM assistance may suggest but not override those gates.
- **PAT-003**: Recommended promotion pattern: low-impact auto-promote, medium-impact conditional auto-promote, high-impact approval-required.
- **PAT-004**: Recommended rollback pattern: restore last-known-good spec, mutation set, and published artifact lineage atomically.

## 4. Interfaces & Data Contracts

### 4.1 Relevant Files and Modules

| Path | Role | Required Outcome |
| --- | --- | --- |
| `src/pipeline/agent/planner.py` | Proposal generation and planning | Must evolve from passive proposal reporting into structured mutation planning |
| `src/pipeline/agent/approval.py` | Approval workflow | Must support impact classes, statuses, and promotion authorization |
| `src/pipeline/agent/agent.py` | Diagnosis and remediation | Must orchestrate autonomous change lifecycle decisions |
| `src/pipeline/orchestration/operator.py` | Pipeline execution | Must support candidate execution, diffing, and promotion/rollback control |
| `src/pipeline/runtime/state.py` | Persistent runtime state | Must persist baseline lineage, mutation history, and promotion outcomes |
| `src/pipeline/runtime/spec.py` | Pipeline contract management | Must support candidate spec mutations and promotion-safe persistence |
| `src/pipeline/quality/quality.py` | Validation and quality gates | Must expose promotion-blocking and regression-blocking checks |
| `src/pipeline/quality/publication.py` | Publish-safe contract | Must remain the authority on privacy-safe publication gates |
| `src/pipeline/transforms/` | Silver and Gold derivation logic | Must support bounded autonomous mutation families defined by policy |
| `tests/` | Automated validation | Must cover mutation classification, promotion, rollback, and efficacy tracking |
| `README.md` | Operator documentation | Must describe the governed autonomy model and operational limits |

### 4.2 Mutation Proposal Contract

Each mutation proposal shall be persisted as a structured record. Recommended artifact: `reports/agent_decisions/proposals/<proposal_id>.json`

Required fields:

| Field | Type | Description |
| --- | --- | --- |
| `proposal_id` | `string` | Stable mutation identifier |
| `planning_run_id` | `string` | Planning session identifier |
| `created_at_utc` | `datetime` | Proposal creation timestamp |
| `proposal_family` | `string` | Mutation family such as `gold_feature_addition` or `validator_tuning` |
| `proposal_type` | `string` | Specific mutation type |
| `trigger_kind` | `string` | Source event such as `validation_failure`, `schema_drift`, `blind_spot`, or `quality_regression` |
| `impact_class` | `string` | `low`, `medium`, or `high` |
| `safe_auto_promote` | `boolean` | Whether policy allows automatic promotion for this proposal if gates pass |
| `requires_approval` | `boolean` | Whether human approval is mandatory |
| `status` | `string` | Proposal lifecycle status |
| `rationale` | `string` | Bounded summary of why the mutation is proposed |
| `expected_outcome` | `string` | Expected operational or analytical improvement |
| `affected_paths` | `array[string]` | Files or contracts expected to change |
| `affected_layers` | `array[string]` | Bronze, Silver, Gold, runtime, or agent domains impacted |
| `evidence_bundle` | `object` | Structured evidence inputs |
| `candidate_actions` | `array[object]` | Concrete mutation actions to materialize |
| `approval_context` | `object` | Approval metadata if required |
| `policy_snapshot` | `object` | Policy state used for classification |

Allowed proposal statuses:

```text
proposed
candidate_materialized
validation_failed
awaiting_approval
approved
rejected
promoted
rolled_back
closed_no_action
```

### 4.3 Mutation Action Contract

Each proposal shall contain one or more mutation actions.

```json
{
  "action_id": "action_01",
  "action_kind": "spec_patch",
  "target_path": "config/pipeline_spec.json",
  "target_selector": "gold.derived_fields",
  "operation": "add_item",
  "payload": {
    "item": "negotiation_style"
  },
  "reversible": true,
  "validation_scope": ["tests/test_transforms.py", "tests/test_quality.py", "candidate_run"]
}
```

Allowed `action_kind` values shall include at least:

- `spec_patch`
- `prompt_version_bump`
- `validator_rule_patch`
- `heuristic_rule_patch`
- `taxonomy_addition`
- `gold_feature_addition`
- `test_generation`
- `config_patch`

### 4.4 Policy Engine Contract

The policy engine shall classify mutation families and enforce promotion behavior from a canonical machine-readable source. Recommended artifact: `config/agent_autonomy_policy.json`

Minimum policy structure:

```json
{
  "mutation_families": {
    "gold_feature_addition": {
      "default_impact_class": "medium",
      "auto_promote": true,
      "requires_backward_compatibility": true,
      "requires_privacy_scan": true
    },
    "validator_tuning": {
      "default_impact_class": "low",
      "auto_promote": true,
      "requires_backward_compatibility": true,
      "requires_privacy_scan": false
    },
    "publication_contract_change": {
      "default_impact_class": "high",
      "auto_promote": false,
      "requires_approval": true
    }
  }
}
```

### 4.5 Candidate Execution Contract

Candidate execution shall use an isolated workspace and isolated artifact roots. Recommended root: `runtime/candidates/<proposal_id>/`

Minimum candidate outputs:

| Artifact | Purpose |
| --- | --- |
| `candidate_spec.json` | Candidate contract after mutation |
| `candidate_run_report.json` | Validation and runtime result |
| `candidate_agent_report.json` | Agent decision context |
| `candidate_diff.json` | Baseline vs candidate artifact comparison |
| `candidate_metrics.json` | Structured metrics used for promotion |
| `candidate_test_report.json` | Targeted test results |

Candidate execution shall not overwrite active artifact roots.

### 4.6 Artifact Diff Contract

The system shall compute structured diffs between active and candidate outputs.

Minimum diff dimensions:

| Dimension | Description |
| --- | --- |
| `schema_diff` | Added, removed, renamed, or type-shifted fields |
| `value_distribution_diff` | Categorical or numeric shifts beyond threshold |
| `null_rate_diff` | Changes in null coverage |
| `privacy_diff` | New privacy findings or removed protections |
| `taxonomy_diff` | Additions or removals in controlled vocabularies |
| `quality_diff` | Validation pass/fail and severity deltas |
| `business_signal_diff` | Changes in analytical fields relevant to Gold |

### 4.7 Promotion Decision Contract

Promotion shall be recorded in a structured decision artifact. Recommended artifact: `reports/agent_decisions/latest_autonomy_decision.json`

Required fields:

| Field | Type | Description |
| --- | --- | --- |
| `decision_id` | `string` | Stable decision identifier |
| `proposal_id` | `string` | Related proposal |
| `decision_at_utc` | `datetime` | Decision time |
| `decision` | `string` | `promote`, `hold_for_approval`, `rollback`, or `reject` |
| `decision_reason` | `string` | Bounded explanation |
| `gate_results` | `object` | Result of tests, validation, diff checks, and policy checks |
| `baseline_reference` | `object` | Last-known-good lineage reference |
| `candidate_reference` | `object` | Candidate lineage reference |
| `post_promotion_monitoring_required` | `boolean` | Whether efficacy follow-up is mandatory |

### 4.8 Efficacy Metrics Contract

The system shall persist autonomy quality metrics in a machine-readable store. Recommended artifact: `reports/monitoring/agent_autonomy_metrics.json`

Minimum metrics:

| Metric | Meaning |
| --- | --- |
| `proposal_count_total` | Number of structured proposals generated |
| `promotion_count_total` | Number of proposals promoted |
| `rollback_count_total` | Number of promoted or candidate mutations rolled back |
| `approval_required_count_total` | Number of proposals requiring human approval |
| `unresolved_failure_count_total` | Number of failures the agent could not safely resolve |
| `promotion_rate_by_family` | Promotions divided by proposals by family |
| `rollback_rate_by_family` | Rollbacks divided by promotions by family |
| `privacy_block_rate` | Proposals blocked by privacy gates |
| `median_candidate_validation_time_sec` | Candidate validation runtime |
| `post_promotion_regression_count` | Regressions detected after promotion |

## 5. Acceptance Criteria

- **AC-001**: Given a low-impact validator tuning proposal with sufficient evidence, When candidate validation passes all required gates, Then the proposal shall be auto-promoted without human approval.
- **AC-002**: Given a medium-impact additive Gold feature proposal marked as safe for auto-promotion by policy, When candidate validation passes and no backward-compatibility regression is detected, Then the proposal shall be auto-promoted.
- **AC-003**: Given a high-impact publication contract change proposal, When the candidate is generated, Then the system shall persist the proposal and stop at `awaiting_approval` rather than promoting automatically.
- **AC-004**: Given a proposal that causes a privacy validation failure in candidate execution, When gate evaluation runs, Then the proposal shall be rejected or rolled back and shall not modify active artifacts.
- **AC-005**: Given a proposal that changes runtime behavior, When targeted tests fail, Then promotion shall be blocked and the failure shall be recorded in the decision artifact.
- **AC-006**: Given a candidate mutation that introduces a non-backward-compatible schema removal, When diff analysis runs, Then the proposal shall be reclassified or blocked according to policy before promotion.
- **AC-007**: Given an analytical blind spot detected from source or Gold evidence, When the agent generates a bounded additive analytics proposal and all gates pass, Then the new feature may be promoted autonomously if policy allows.
- **AC-008**: Given a promoted mutation, When post-promotion monitoring detects a policy-defined regression, Then the system shall execute rollback to the last-known-good lineage and record the regression cause.
- **AC-009**: Given repository-local execution with external providers disabled, When the autonomy runtime runs, Then proposal generation, candidate validation, and deterministic gating shall still function.
- **AC-010**: Given an unknown failure not mapped to an existing deterministic playbook, When LLM-assisted proposal generation is enabled and returns a structured candidate, Then the candidate shall still require the same deterministic gates before promotion.
- **AC-011**: Given any promoted mutation, When the operator inspects decision records, Then the proposal, evidence, candidate result, promotion reason, and lineage reference shall be traceable.
- **AC-012**: Given a medium- or high-impact proposal affecting a published contract, When approval is required by policy, Then the system shall record who approved or rejected the proposal and when.

## 6. Test Automation Strategy

- **Test Levels**: Unit, integration, candidate-runtime integration, repository-local end-to-end
- **Frameworks**: `pytest` executed through `venv/bin/python -m pytest`
- **Test Data Management**: Use synthetic DataFrame fixtures for mutation families, repository-local parquet input for runtime candidate execution, and isolated candidate directories for promotion tests
- **CI/CD Integration**: The same `venv/bin/python -m pytest -q` command shall remain valid for automated execution; additional targeted autonomy tests may be split by module
- **Coverage Requirements**: Add regression coverage for policy classification, proposal contracts, candidate execution, artifact diffs, promotion gating, rollback, and efficacy metric persistence
- **Performance Testing**: Candidate validation runtime shall be measured; no separate load framework is required for the initial implementation

Required automated checks:

- A policy-engine test shall verify low, medium, and high impact classification for representative mutation families.
- A proposal-contract test shall verify required fields and allowed statuses.
- A candidate-execution test shall verify isolated artifact roots and absence of active artifact overwrite before promotion.
- A diff-engine test shall verify schema and quality regressions are detected.
- A promotion test shall verify low-impact passing changes are auto-promoted.
- A promotion test shall verify high-impact changes remain approval-gated.
- A rollback test shall verify restoration of the last-known-good lineage after failed post-promotion monitoring.
- A privacy-gate test shall verify privacy-unsafe mutations are blocked.
- A runtime test shall verify the repository-local baseline still operates with network-disabled deterministic fallback.
- A metrics test shall verify promotion and rollback outcomes update autonomy metrics.

Recommended validation commands:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_agent.py -q
venv/bin/python -m pytest tests/test_jobs.py -q
venv/bin/python -m pytest tests/test_quality.py -q
venv/bin/python -m pytest tests/test_requirements_adherence.py -q
```

## 7. Rationale & Context

The current project already demonstrates strong operational engineering, but its agent is primarily deterministic and bounded to known playbooks. That makes it credible and safe, yet it still falls short of the strongest interpretation of an AI agent that creates and maintains a pipeline as living infrastructure.

The governed-by-impact model is chosen because it raises the level of autonomy without collapsing governance. This is important for three reasons:

1. The technical test asks for an agent that creates and manages the pipeline rather than simply running analysis.
2. Full unrestricted autonomy is hard to defend in a production-minded engineering evaluation because it can appear unsafe or under-governed.
3. The current repository already has the foundations needed for an incremental evolution toward bounded self-improvement: deterministic validation, state persistence, approval tracking, operational reporting, and optional LLM support.

This specification therefore aims for the strongest defensible posture:

- the agent can autonomously evolve bounded parts of the system;
- the system can prove that promotions are evidence-backed and safe;
- structural or privacy-sensitive changes remain approval-gated;
- every decision remains replayable and auditable.

This model is also the best fit for the eight capability improvements required to make the project exceptional:

- rule and contract evolution from evidence
- closed-loop autonomy
- self-healing beyond fallback
- bounded autonomy over spec and runtime behavior
- less-generic, more valuable Gold analytics
- evaluation of the agent itself
- production-like continuous operation semantics
- central but governed use of LLM assistance

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Repository-local parquet Bronze source - canonical transactional input for candidate and baseline runs
- **EXT-002**: Local filesystem state store - required for lineage, proposal, candidate, and decision persistence

### Third-Party Services

- **SVC-001**: Optional LLM provider runtime - used for bounded proposal generation or semantic classification when enabled, but not required for baseline local autonomy
- **SVC-002**: Optional observability backend such as Langfuse - used for tracing prompts, model decisions, and runtime telemetry when enabled

### Infrastructure Dependencies

- **INF-001**: Isolated candidate artifact directories - required to execute autonomous mutations safely before promotion
- **INF-002**: Persistent runtime state and report directories - required for approval workflow, lineage, rollback, and efficacy tracking

### Data Dependencies

- **DAT-001**: `docs/conversations_bronze.parquet` or configured Bronze source - required for baseline and candidate pipeline execution
- **DAT-002**: Current pipeline spec and state history - required to classify diffs and restore last-known-good runtime state

### Technology Platform Dependencies

- **PLT-001**: Local Python runtime with project virtual environment - required because repository rules mandate execution through `venv/bin/python`
- **PLT-002**: Deterministic test and validation framework - required as the final promotion authority regardless of LLM assistance

### Compliance Dependencies

- **COM-001**: Repository privacy and masking contract - autonomous mutations must preserve publish-safe behavior and anti-leak guarantees
- **COM-002**: Repository documentation policy - material behavioral changes must update `README.md` and operational documentation in the same work cycle

## 9. Examples & Edge Cases

```json
{
  "edge_case": "medium_impact_additive_gold_feature",
  "trigger": {
    "kind": "blind_spot",
    "evidence": {
      "missing_signal_family": "negotiation_style",
      "observed_patterns": ["parcelamento", "desconto", "fechar hoje"],
      "affected_conversation_count": 1832
    }
  },
  "proposal": {
    "proposal_family": "gold_feature_addition",
    "impact_class": "medium",
    "safe_auto_promote": true,
    "candidate_actions": [
      {
        "action_kind": "spec_patch",
        "target_path": "config/pipeline_spec.json",
        "target_selector": "gold.required_columns",
        "operation": "add_item",
        "payload": {
          "item": "negotiation_style"
        }
      },
      {
        "action_kind": "heuristic_rule_patch",
        "target_path": "src/pipeline/transforms/gold.py",
        "target_selector": "derive_negotiation_style",
        "operation": "add_rule_family",
        "payload": {
          "taxonomy": ["sensivel_preco", "urgente", "comparador", "consultivo"]
        }
      },
      {
        "action_kind": "test_generation",
        "target_path": "tests/test_transforms.py",
        "operation": "add_regression_case",
        "payload": {
          "scenario": "negotiation_style_classification"
        }
      }
    ]
  },
  "expected_runtime_outcome": "candidate validation passes, Gold gains one backward-compatible column, privacy gates remain green, proposal auto-promotes"
}
```

Additional edge cases:

- A proposal improves classification coverage but increases privacy risk: privacy gate must block promotion.
- A proposal is low impact by family but produces a breaking schema diff: candidate must be blocked or reclassified.
- An LLM-generated proposal has plausible reasoning but missing structured evidence: promotion must be blocked.
- Post-promotion monitoring shows a rise in unresolved validation failures: rollback must execute and record the regression.
- The same proposal is retried against the same baseline lineage: the decision must be idempotent rather than duplicated unsafely.

## 10. Validation Criteria

The implementation shall be considered compliant with this specification only if all of the following conditions are met:

- The runtime can generate structured mutation proposals from runtime evidence.
- The policy engine can classify mutation families into low, medium, and high impact using a machine-readable contract.
- Candidate mutations execute in isolated artifact roots and do not overwrite active outputs before promotion.
- Promotion decisions are gated by tests, validation, privacy checks, and artifact diffs.
- High-impact mutations cannot be auto-promoted.
- Automatic rollback can restore the last-known-good lineage after candidate or post-promotion regression.
- Decision records and efficacy metrics are persisted and traceable.
- The repository-local baseline path still works with deterministic fallback and without mandatory network access.
- The automated test suite covers policy classification, promotion, rollback, privacy blocking, and efficacy metrics.
- Documentation is updated whenever the autonomy model changes runtime behavior materially.

## 11. Related Specifications / Further Reading

- [spec-architecture-llm-conversation-enrichment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-conversation-enrichment.md)
- [spec-architecture-semantic-contract-separation.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-semantic-contract-separation.md)
- [spec-architecture-pipeline-validation-contract-alignment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-pipeline-validation-contract-alignment.md)
- [docs/technical-test-data-ai-engineering.md](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md)
