# Architecture v1

## 1. Runtime Components
1. Connector layer (`app/connectors/`)
2. Ingestion layer (`app/ingestion/`)
3. Analysis layer (`app/analysis/`)
4. KPI layer (`app/kpi/`)
5. Reporting layer (`app/reporting/`)
6. CLI orchestrator (`app/cli.py`)

## 2. Data Flow
1. CLI loads config and validates settings
2. Connector opens read-only connection
3. Schema extraction enumerates assets and metadata
4. Sampling and profiling compute column/table statistics
5. Quality checks evaluate rule outcomes and score
6. PII scanner marks sensitive columns (metadata-only exposure)
7. Relationship inference finds join candidates
8. KPI detector maps domain template and readiness
9. Reporting renderer emits machine and human outputs

## 3. Storage and Artifacts
- Runtime outputs (generated at run-time): `data-onboarding-system/reports/`, `data-onboarding-system/logs/`
- Curated sample artifacts (tracked): `data-onboarding-system/examples/diagnostic_pack/`
- Legacy archived artifacts: `handover/archive/ci-artifacts-legacy/`

## 4. Failure Domains
### Connector failures
- Symptoms: credential rejection, network timeout
- Handling: mark source failed, emit actionable error, do not leak secret values

### Query/sample failures
- Symptoms: table permission errors, type casting failures
- Handling: mark table as partial failure, continue remaining tables when safe

### Report render failures
- Symptoms: template error or PDF dependency missing
- Handling: always produce JSON and text; HTML/PDF failures are non-fatal by default

## 5. Retry and Idempotency Rules
1. Retries only for transient classes (network, timeout, temporary unavailable)
2. Maximum retry count configurable and bounded
3. Re-run with same input writes to deterministic output directory by run ID
4. Partial artifacts must include failure metadata

## 6. Operational Boundaries
1. v1 is batch-first and single-run focused
2. v1 prioritizes correctness and observability over throughput
3. Large datasets are sampling-first unless explicit full-scan approval is provided
4. Integration validation requires Docker-enabled PostgreSQL environment

## 7. CI Mapping
- Canonical CI workflow file: `.github/workflows/ci.yml`
- CI builds test image, starts test DB, runs tests, runs E2E smoke, publishes artifacts
