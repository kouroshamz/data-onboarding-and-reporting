# Phase 01 - Repo Hygiene and Canonicalization

## Objective
Remove documentation ambiguity, establish canonical structure, and archive superseded materials.

## Scope
- Create handover folders and root README entrypoint
- Move conflicting root and subproject docs into archive
- Remove backup file artifacts (`*.bak`)
- Consolidate to one curated sample artifact set
- Enforce ignore policy for runtime output directories

## Work Breakdown
1. Directory setup
- Ensure `handover/`, `handover/plans/`, `handover/archive/`, `handover/templates/` exist.
2. Legacy doc archival
- Move all listed root status docs into `handover/archive/root-legacy/`.
- Move all listed subproject docs into `handover/archive/subproject-legacy/`.
3. Artifact cleanup
- Keep one sample set in `data-onboarding-system/examples/diagnostic_pack/`.
- Archive duplicate sample set under `handover/archive/`.
4. Hygiene enforcement
- Remove `data-onboarding-system/app/cli.py.bak`.
- Update ignore rules in root and subproject contexts.
5. Canonical entrypoint
- Add root `README.md` and subproject `HANDOVER_LINK.md` pointer.

## Deliverables
- Canonical folder structure committed
- Root and subproject docs archived
- Single curated diagnostic sample set
- Ignore policy updated

## Acceptance Criteria
1. No conflicting status docs remain at repository root.
2. No `*.bak` file remains.
3. `data-onboarding-system/examples/diagnostic_pack/` exists with complete sample pack.
4. Canonical documentation entrypoints exist.
