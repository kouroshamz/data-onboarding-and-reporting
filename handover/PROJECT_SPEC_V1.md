# Project Specification v1

## 1. Product Goal
Deliver a diagnostic package within 24 hours of validated data access that includes:
1. Data inventory and freshness
2. Data quality scorecard and top issues
3. Relationship and KPI candidate map
4. Seven-day remediation plan
5. Optional starter dashboard link when source stability is confirmed

## 2. SLA and Intake Gate
The 24-hour SLA starts only after these inputs are complete:
1. Read-only credentials to at least one priority source
2. Client goals (minimum five bullets)
3. Timezone and reporting cadence
4. Core business glossary (customer, order, conversion)
5. Named primary contacts for technical and business questions

If any intake requirement is missing, the run is marked `blocked_intake` and SLA does not start.

## 3. v1 Scope
### In scope
- Connectors: PostgreSQL, MySQL, S3 (CSV/Parquet), local CSV/XLSX
- Profiling: column stats, nulls, uniqueness, numeric/date summaries
- Quality checks: completeness, freshness, duplicates, consistency
- PII scan: heuristic detection and redacted reporting
- KPI recommendations: deterministic templates by domain
- Reporting: JSON + text + HTML + optional PDF

### Out of scope (v1)
- Fully managed SaaS connectors (Salesforce, HubSpot, GA4)
- Real-time ingestion and streaming diagnostics
- Autonomous metric deployment to production BI tools
- LLM-generated formulas without deterministic validation

## 4. Pipeline Stages and Deliverables
1. Connection and Inventory
Deliverables: `assets_inventory.json`, `source_connection_status.json`, `sampling_manifest.json`
2. Schema and Profiling
Deliverables: profile payloads per asset, relationship candidates
3. Data Quality
Deliverables: expectations and results payload, severity labels
4. KPI Candidate Generation
Deliverables: `kpi_candidates.json`, dashboard outline
5. Diagnostic Report Assembly
Deliverables: `report_data.json`, `report.txt`, `report.html`, optional `report.pdf`
6. Optional Starter Dashboard
Deliverables: dashboard links and export metadata when available

## 5. Quality Scoring Model
Quality score is 0-100 and uses weighted categories:
- Missingness: 30 points
- Validity and rule pass rate: 30 points
- Uniqueness and duplicate health: 20 points
- Freshness: 10 points
- Integrity checks: 10 points

Severity labels:
- Critical: blocks core KPI computation
- Major: materially distorts trend or decisions
- Minor: cosmetic or low-impact cleanup

## 6. LLM Guardrails
LLM support is optional and constrained:
1. Input is summarized metadata only (no raw sensitive rows)
2. Output must be strict JSON
3. Non-JSON output is rejected and retried
4. KPI formulas remain deterministic authority in v1
5. LLM suggestions are tagged with `source=llm_assist` and human-review-required

## 7. Reliability and Operations Requirements
1. Every run has a stable run ID and persisted status (`queued`, `running`, `blocked`, `failed`, `completed`)
2. Steps are idempotent by run ID and source snapshot
3. Retries use bounded backoff for transient failures
4. Per-table failure does not crash whole run unless configured by `fail_on_partial`
5. Secrets and PII are never written to logs

## 8. Acceptance Criteria for v1
1. Engineering can run the pipeline from clean setup using runbook docs only
2. Non-integration test suite passes locally
3. Integration and E2E commands are runnable in Docker-enabled environment
4. All public contracts in `INTERFACE_CONTRACTS_V1.md` are documented and versioned
5. No contradictory status docs exist outside `handover/archive/`
