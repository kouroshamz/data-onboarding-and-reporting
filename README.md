# Data Onboarding & Reporting

Production-grade pipeline that connects to a client's database or CSV, profiles the data, flags PII / GDPR concerns, detects KPI candidates, and renders a branded analyst-ready report (HTML + PDF) — in minutes.

> Built to compress what most data teams spend the first two weeks of an engagement doing into a single repeatable run.

---

## What it does

Point it at a data source, get back a complete onboarding report:

| Stage | What happens |
|---|---|
| **Connect** | PostgreSQL, MySQL, S3, or CSV. Connection status logged with structured audit trail. |
| **Sample** | Deterministic sampling strategies sized to the source. |
| **Profile** | Column-level types, distributions, missingness, uniqueness, freshness. |
| **Quality scoring** | Weighted overall score (0–100) across missingness, validity, uniqueness, consistency, freshness. |
| **PII scan** | Detects 20+ PII categories with severity scoring. |
| **GDPR assessment** | Lawful-basis prompts, retention recommendations, controller/processor flags, DPA suggestions. |
| **KPI candidates** | Detects metrics the data can support (growth, retention, freshness, etc.) with readiness flags. |
| **LLM-assisted narrative** *(optional)* | OpenAI/Anthropic with prompt-level caching and per-run budget caps. Writes executive summary, insight narrative, and emphasis-tagged section commentary. |
| **Render** | HTML report (10 sections, designed for client-facing delivery) + PDF export. |

All artifacts are written to a structured output directory (`assets_inventory.json`, `schema.json`, `profile.json`, `kpi_candidates.json`, `report.html`, `report.pdf`, ...) so downstream tools can consume the run.

---

## Quick start

```bash
cd data-onboarding-system
pip install -r requirements.txt

# One-liner against any CSV
python -m app.cli quick path/to/data.csv --client-name "Acme Co"

# With LLM-assisted reporting (optional)
export OPENAI_API_KEY=...
python -m app.cli quick path/to/data.csv --client-name "Acme Co" --llm

# Full config-driven run (databases, S3, multi-table)
python -m app.cli run --config config.yaml
```

Generated artifacts land in `reports/<run-id>/`. Open `report.html` in a browser.

### Interactive comparison dashboard

```bash
python -m app.dashboard_server --port 8787
# → http://127.0.0.1:8787/compare.html
```

Lists prior runs, lets you upload new datasets, side-by-side compare reports.

---

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌────────────────┐    ┌──────────────┐
│  Connectors  │ -> │  Ingestion   │ -> │   Analysis     │ -> │  Reporting   │
│              │    │              │    │                │    │              │
│ • postgres   │    │ • schema     │    │ • profiling    │    │ • renderer   │
│ • mysql      │    │   extract    │    │ • quality      │    │   (HTML)     │
│ • s3         │    │ • sampling   │    │ • pii_scan     │    │ • pdf export │
│ • csv        │    │              │    │ • gdpr         │    │              │
└──────────────┘    └──────────────┘    │ • kpi_detector │    └──────────────┘
                                        │ • readiness    │           │
                                        └────────────────┘           │
                                                │                    │
                                                v                    │
                                        ┌────────────────┐           │
                                        │  LLM service   │ ──────────┘
                                        │ (optional)     │
                                        │ • cached       │
                                        │ • budget-capped│
                                        │ • multi-prompt │
                                        └────────────────┘
```

Orchestrated by `app/orchestration/engine.py` with deterministic pipeline-status tracking and per-stage failure isolation.

---

## Tech

- **Language**: Python 3.11+
- **Connectors**: SQLAlchemy (Postgres, MySQL), boto3 (S3), pandas (CSV)
- **Profiling**: pandas + scipy
- **Reporting**: Jinja2 (HTML), WeasyPrint (PDF)
- **LLM**: provider-agnostic interface, lazy-imported, response caching, cost tracking
- **Security**: configurable column masking, structured audit logging
- **Tests**: pytest with ~30 test modules covering connectors, pipeline, security, LLM contracts, reporting, golden e2e

---

## Engineering notes

This repository was prepared as a handover-ready artifact. The `handover/` directory contains the full set of V1 documents that accompanied delivery:

- [PROJECT_SPEC_V1](handover/PROJECT_SPEC_V1.md) — scope, SLAs, acceptance criteria
- [ARCHITECTURE_V1](handover/ARCHITECTURE_V1.md) — runtime architecture, failure domains
- [INTERFACE_CONTRACTS_V1](handover/INTERFACE_CONTRACTS_V1.md) — stable contracts between stages
- [SECURITY_BASELINE_V1](handover/SECURITY_BASELINE_V1.md) — required controls, data handling
- [OPERATIONS_RUNBOOK_V1](handover/OPERATIONS_RUNBOOK_V1.md) — setup, troubleshooting, release
- [TESTING_AND_READINESS_V1](handover/TESTING_AND_READINESS_V1.md) — verification evidence
- [REPORTING_STYLE_AND_BRAND_V1](handover/REPORTING_STYLE_AND_BRAND_V1.md) — report standards

If you're evaluating whether the codebase is built to professional standards, start there.

---

## Status

Stable. Used in production on a private engagement. Open-sourced as a portfolio reference and as a usable starting point for anyone running data-onboarding work in a consultancy or in-house setting.

Not currently maintained as a community OSS project — PRs welcome but response times are not guaranteed.

---

## About

Built by [Kourosh Amouzgar](https://www.linkedin.com/in/kouroshamouzgar/) — operations data → measurable savings. Production AI/ML for logistics, IoT, supply chain.

Available for fractional / project work via [Deep Network Solutions](https://kouroshamouzgar.com).

## License

MIT
