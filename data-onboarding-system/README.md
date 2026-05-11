# Data Onboarding System

Automated data discovery and diagnostic reporting system.  
Connects to a client data source, profiles every table, scores quality, detects PII,
recommends KPIs, and delivers a **consulting-grade 10-section HTML/PDF report** —
all within the 24-hour SLA defined in the project spec.

## Quick Start

```bash
# 1. Create & activate a virtual environment
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy & edit configuration
cp config.example.yaml config.yaml   # then set connection details, client info, etc.

# 4. Run the pipeline
python -m app.cli run --config config.yaml

# Output → reports/{client_id}/report.html  report.pdf  report_data.json  report.txt
```

### CSV Quick Test (no database required)

```bash
python -m app.cli run --config config_llm_demo.yaml
# Uses tests/fixtures/datasets/01_titanic as the CSV source
```

## Architecture

```
app/
├── cli.py                  # Click CLI — run / validate / schema commands
├── config.py               # Pydantic config model
├── connectors/             # PostgreSQL, MySQL, CSV, S3
├── ingestion/              # Schema extraction + intelligent sampling
├── analysis/               # 9 analysis modules:
│   ├── profiling.py        #   Column-level profiling
│   ├── quality_checks.py   #   5-component quality scoring (0-100)
│   ├── pii_scan.py         #   PII / sensitive data detection
│   ├── relationships.py    #   FK / join inference
│   ├── structural_overview.py
│   ├── gdpr_assessment.py
│   ├── column_classifier.py
│   ├── interesting_columns.py
│   ├── missing_strategy.py
│   └── readiness_score.py
├── kpi/                    # Industry templates + auto-detection
├── llm/                    # Optional LLM layers (L1–L3):
│   ├── client.py           #   OpenAI / Anthropic / Local providers
│   ├── service.py          #   Type inspector, insight detector, report advisor
│   ├── cache.py            #   Response caching
│   ├── cost_tracker.py     #   Token & cost tracking
│   ├── schemas.py          #   Pydantic output schemas
│   └── prompts/            #   Structured prompt templates
├── orchestration/          # Pipeline engine with retries + step tracking
├── security/               # Data masking + append-only audit logging
├── storage/                # Local artifact storage
└── reporting/
    ├── renderer_html.py    # 10-section consulting-grade HTML report
    ├── export_pdf.py       # WeasyPrint PDF export (optional)
    └── renderer.py         # Jinja-based text renderer
```

## Pipeline Stages

| Stage | Description | Deliverables |
|-------|-------------|--------------|
| 1 | Connect & Inventory | `source_connection_status.json`, `assets_inventory.json` |
| 2 | Schema Discovery | `schema.json` |
| 3 | Profile, Quality, PII | `profile.json`, `sampling_manifest.json` |
| 3b | LLM Type Inspector (L1) | `type_inspector_results.json` |
| 4 | Relationship Inference | embedded in report_data |
| 5 | KPI Recommendations | `kpi_candidates.json` |
| 5b | LLM Insight Detector (L2) | `insights.json` |
| 5c | Advanced Analysis | structural, GDPR, columns, missing, readiness |
| 5d | Security Masking | redacts PII from report artefacts |
| 6 | Report Generation | `report_data.json`, `report.txt`, `report.html` |
| 6b | LLM Report Advisor (L3) | `report_layout.json` |
| — | PDF Export (optional) | `report.pdf` |

## Report Sections

1. **Executive Summary** — key metrics, readiness score, LLM narrative
2. **Dataset Structural Overview** — row/column counts, duplicates, type distribution
3. **Column-Level Profiling** — stats, nulls, cardinality, categoricals
4. **Data Quality Flags** — severity-sorted issues with LLM type mismatch detection
5. **Sensitive Data & GDPR Assessment** — PII categories, risk ratings, recommendations
6. **Business Insight Discovery** — column classifications + LLM-detected insights
7. **Recommended KPIs** — industry-specific with readiness checks
8. **Interesting Columns Detection** — statistically notable columns + correlations
9. **Missing Data Strategy** — per-column remediation recommendations
10. **Data Readiness Score** — 5-component weighted score (0–100) with grading

## Configuration

See [config.example.yaml](config.example.yaml) for all options including:
- Connection settings (PostgreSQL, MySQL, CSV, S3)
- Sampling thresholds and strategy
- Analysis toggles and quality thresholds
- KPI detection confidence and limits
- LLM provider/model/budget (optional)
- Output format and directory

## LLM Integration (Optional)

Set `llm.enabled: true` in config and provide an API key via environment variable:

```bash
export OPENAI_API_KEY="sk-..."
```

Three LLM layers enhance the deterministic pipeline:
- **L1 Type Inspector** — detects semantic type mismatches (e.g. phone stored as int)
- **L2 Insight Detector** — discovers cross-table patterns humans might miss
- **L3 Report Advisor** — produces executive narrative and section emphasis

All LLM output is cached, budget-tracked, and tagged `source=llm_assist`.

## Development

```bash
# Run unit tests (no database required)
pytest tests/ -q -m 'not integration'

# Run with coverage
pytest tests/ --cov=app --cov-report=term-missing -m 'not integration'

# Validate a config file
python -m app.cli validate --config config.yaml

# Extract schema only
python -m app.cli schema --config config.yaml
```

## Docker

```bash
docker build -f docker/Dockerfile -t data-onboarding:latest .
docker run -v $(pwd)/config.yaml:/app/config.yaml \
           -e DB_PASSWORD=secret \
           data-onboarding:latest
```

## PDF Export Dependencies

PDF export uses WeasyPrint which requires system libraries:
- **macOS:** `brew install cairo pango gdk-pixbuf libffi`
- **Ubuntu:** `apt-get install libcairo2 libpango-1.0-0 libgdk-pixbuf2.0-0`
- Then: `pip install weasyprint`

If these are absent, the pipeline completes normally — PDF is skipped with a warning.

## Handover Documentation

Canonical project documentation: [`../handover/INDEX.md`](../handover/INDEX.md)
