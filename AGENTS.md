# AGENTS

## Commit rules

Every commit message must follow the `Conventional Commits` standard and must be written in English.

## Expected format

Use the structure:

- `<type>(<optional scope>): <short description>`

Valid examples:

- `feat(pipeline): add gold stage`
- `fix(transform): fix deduplication by conversation_id`
- `docs(readme): update execution instructions`

## Documentation rules

Every relevant change to the pipeline, transformation rules, generated artifacts, or execution mode must update the repository documentation in the same work cycle.

## Spec rules

Specification files, design drafts, or planning documents must be created only in `spec/`, following the pattern defined by the `create-specification` skill.

Do not create specs in `docs/superpowers/specs/` or other directories unless explicitly requested by the user.

## Minimum required updates

When a relevant change occurs, review and adjust:

- `README.md` to reflect the current state, execution, artifacts, and limitations
- any additional operational documentation created during the project, if it exists

## Scope of the rule

Consider the following as relevant changes:

- new pipeline stage
- schema changes in Bronze, Silver, or Gold
- new cleaning, masking, or deduplication rules
- new analytical extractions
- changes to execution scripts
- new reports, directories, or persisted outputs

## Objective

Prevent documentation from falling behind the code and keep the repository usable as a technical deliverable and operational reference.

## Running tests

Always run tests using the repository's local virtual environment.

Standard commands:

- `venv/bin/python -m pytest -q`
- `venv/bin/python -m pytest tests/test_jobs.py -q`

Avoid relying on `python`, `pytest`, or global tools in `PATH`, because the correct project environment lives in `venv/`.
