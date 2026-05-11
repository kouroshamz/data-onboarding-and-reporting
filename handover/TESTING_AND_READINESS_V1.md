# Testing and Readiness v1

## 1. Verification Baseline (Local)
**Date:** February 13, 2026

### Verified command
```bash
cd data-onboarding-system
PYTHONPATH=. pytest tests -q -m 'not integration'
```

### Verified result
- `20 passed, 6 deselected`

### Additional smoke verification
```bash
cd data-onboarding-system
PYTHONPATH=. pytest tests/test_config.py tests/test_iot_template.py tests/test_html_report.py -q
```
Result: `7 passed`

## 2. Integration Status
Integration tests were attempted locally but not verifiable in this sandbox due blocked DB access on `localhost:5433` (`Operation not permitted`).

Required environment for valid integration evidence:
1. Docker-enabled host
2. `test-postgres` container from `docker-compose.test.yml`
3. Network access from test process to container port

## 3. CI Evidence Source
- CI workflow: `.github/workflows/ci.yml`
- CI steps include image build, DB startup, full pytest, E2E smoke, and artifact upload

## 4. Readiness Gate Criteria
1. Gate A (Documentation): canonical docs exist in `handover/` and legacy docs only in archive
2. Gate B (Local quality): non-integration tests pass
3. Gate C (Integration quality): integration tests pass in Docker-enabled environment
4. Gate D (E2E): smoke run produces expected artifacts
5. Gate E (Security): logging/PII/secret controls checked against baseline

## 5. Local vs CI Truth Policy
1. Local results are authoritative only for local environment scope
2. CI is authoritative for clean-environment regression checks
3. Production-readiness claims require both local and CI evidence
4. Any missing evidence must be marked `not_verified`

## 6. Evidence Recording Template
Use this template in PR descriptions or release notes:

```markdown
## Verification Evidence
- Date:
- Commit:
- Local non-integration: pass/fail + command
- Local integration: pass/fail/not_verified + command
- Local E2E: pass/fail/not_verified + command
- CI workflow run URL:
- Artifacts reviewed:
- Known gaps:
```

## 7. Current Readiness Statement
As of February 13, 2026:
- Documentation cleanup and canonicalization: complete
- Local non-integration verification: complete
- Integration and E2E verification in this sandbox: not verifiable
