# Operations Runbook v1

## 1. Prerequisites
1. Python 3.11+
2. Docker Desktop (for integration and E2E verification)
3. Access to repository root

## 2. Environment Setup
```bash
cd data-onboarding-system
pip install -r requirements.txt
```

Optional environment file:
```bash
cp .env.example .env
```

## 3. Local Non-Integration Verification
```bash
cd data-onboarding-system
PYTHONPATH=. pytest tests -q -m 'not integration'
```

Expected: non-integration suite passes.

## 4. Integration Verification (Docker Required)
```bash
cd data-onboarding-system
docker compose -f docker-compose.test.yml up -d test-postgres
PYTHONPATH=. pytest tests/test_integration.py -v -m integration
```

> **Note:** Do *not* tear down the database yet if you plan to run the E2E smoke (Step 5).
> Tear down only after Step 5, or skip Step 5 first.

## 5. E2E Smoke Run (Docker Network)

Requires the test database from Step 4 to still be running.

```bash
cd data-onboarding-system
# If the DB is not already up from Step 4:
# docker compose -f docker-compose.test.yml up -d test-postgres
python -m app.cli run --config config.test.yaml
```

Expected output locations:
- runtime: `data-onboarding-system/test_reports/` or configured output path
- curated sample reference: `data-onboarding-system/examples/diagnostic_pack/`

## 6. Teardown
```bash
cd data-onboarding-system
docker compose -f docker-compose.test.yml down -v
```

## 7. Troubleshooting Matrix
1. `psycopg2 OperationalError on localhost:5433`
Cause: test database unavailable or blocked environment.
Action: start Docker DB, verify mapping/permissions, rerun integration.
2. Missing `report.pdf`
Cause: WeasyPrint system dependencies absent.
Action: install runtime libraries or run Docker-based test image.
3. Pipeline exits after partial table processing
Cause: source permission or query failure.
Action: inspect logs, confirm table permissions, rerun with partial failure allowed.

## 8. Release Checklist
1. Confirm docs under `handover/` are updated
2. Run local non-integration verification
3. Run integration and E2E in Docker-enabled environment
4. Confirm no duplicate status docs outside `handover/archive/`
5. Confirm `examples/diagnostic_pack/` remains stable

## 9. Recovery Procedure
1. If doc structure drifts, restore canonical layout from this runbook
2. If sample artifacts are overwritten, restore from version control
3. If security incident occurs, follow `SECURITY_BASELINE_V1.md`
