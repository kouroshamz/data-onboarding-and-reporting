# Phase 6 — LLM Integration Plan

**Status:** Planning  
**Date:** 2026-02-13  
**Depends on:** Phase 1–5 complete (99 tests passing)

---

## Overview

Three LLM layers that bolt onto the existing pipeline without breaking the
non-LLM path.  When no API key is configured the pipeline works exactly as
today — LLM layers are **opt-in enhancements**.

| Layer | Name | Pipeline Position | Purpose |
|-------|------|-------------------|---------|
| **L1** | Data Type Inspector | After Stage 3 (profiling) | Catch hidden types, misclassified columns |
| **L2** | Insight Detector | After Stage 5 (KPIs) | Surface anomalies, "good-to-know" facts |
| **L3** | Report Advisor | During Stage 6 (reporting) | Choose KPIs, pick charts, write narratives |

---

## Architecture Principles

### 1. Provider-agnostic LLM client
```
app/llm/
    __init__.py          # public API
    client.py            # abstract BaseLLMClient + factory
    providers/
        __init__.py
        openai.py        # OpenAI / Azure OpenAI
        anthropic.py     # Claude
        local.py         # Ollama / vLLM / local endpoint
    prompts/
        type_inspector.py
        insight_detector.py
        report_advisor.py
    cache.py             # SHA-256 content hash → cached response
```

### 2. Graceful degradation
- `Config.llm.enabled: bool` (default `false`)
- If disabled, every layer returns `{"skipped": true, "reason": "llm_disabled"}`
- Pipeline never blocks on LLM failure — wrap every call in try/except, log warning, continue

### 3. Token budget management
- Never send raw DataFrames — send **compact summaries**
  - Column name, dtype, null %, unique %, sample values (≤5)
  - For string columns: min/max length, top-5 values, regex pattern hits
- **Hard token cap** per call (configurable, default 4 000 input / 2 000 output)
- For large schemas (>30 tables), batch into groups of 10

### 4. Structured output
- All prompts request JSON output with a defined schema
- Use `response_format: { type: "json_object" }` (OpenAI) or equivalent
- Validate LLM output against Pydantic models before merging into pipeline

### 5. Cost tracking
- Each call logs: model, input_tokens, output_tokens, latency_ms, cost_estimate
- Stored in `llm_usage.json` alongside other deliverables
- Config: `llm.budget_limit_usd: 1.00` — stops calling once budget exhausted

---

## Config Changes

```yaml
llm:
  enabled: true
  provider: "openai"            # openai | anthropic | local
  model: "gpt-4o-mini"          # cost-efficient default
  api_key_env: "OPENAI_API_KEY" # env var name for API key
  base_url: null                # override for Azure/local endpoints
  temperature: 0.1              # low for deterministic analysis
  budget_limit_usd: 1.00        # per-run cost cap
  layers:
    type_inspector: true
    insight_detector: true
    report_advisor: true
  cache:
    enabled: true
    directory: ".llm_cache"
```

---

## Layer 1 — Data Type Inspector

### Problem it solves
The current profiler classifies columns as `numeric`, `string`, `boolean`,
`datetime`.  But string columns often hide:
- **JSON blobs** — `{"key": "value", ...}` stored as text
- **Arrays / lists** — `[1, 2, 3]` or `"a,b,c"` stored as text
- **Dates** — `"2024-01-15"` stored as object/string
- **Numbers** — `"42.5"` stored as object after CSV import
- **Categoricals** — only 5 unique values in 10,000 rows = probably an enum
- **IDs / codes** — `"SKU-12345"` that looks like string but is a structured identifier

### Where it plugs in
```
Stage 3 (profile) → L1 Type Inspector → enriched profile → Stage 4
```

### What the LLM sees (prompt input)
For each table, a compact summary of **string/object columns only**:

```json
{
  "table": "orders",
  "columns": [
    {
      "name": "metadata",
      "dtype": "object",
      "null_pct": 2.1,
      "unique_pct": 98.5,
      "min_len": 15,
      "max_len": 4200,
      "avg_len": 340.2,
      "sample_values": [
        "{\"source\":\"web\",\"utm\":\"google\"}",
        "{\"source\":\"api\",\"client_ver\":\"2.1\"}",
        "{\"source\":\"mobile\"}",
        "{\"source\":\"web\",\"coupon\":\"SAVE10\"}",
        "{\"source\":\"api\"}"
      ]
    },
    {
      "name": "status",
      "dtype": "object",
      "null_pct": 0.0,
      "unique_pct": 0.04,
      "min_len": 4,
      "max_len": 10,
      "avg_len": 6.8,
      "sample_values": ["active", "pending", "cancelled", "active", "shipped"]
    }
  ]
}
```

### What the LLM returns (structured output)
```json
{
  "findings": [
    {
      "column": "metadata",
      "current_type": "object",
      "detected_type": "json_object",
      "confidence": 0.95,
      "severity": "warning",
      "recommendation": "Column contains JSON objects. Consider parsing to extract nested fields (source, utm, coupon, client_ver).",
      "extracted_keys": ["source", "utm", "coupon", "client_ver"],
      "action": "parse_json"
    },
    {
      "column": "status",
      "current_type": "object",
      "detected_type": "categorical_enum",
      "confidence": 0.98,
      "severity": "info",
      "recommendation": "Only 4 unique values across all rows. This is a categorical/enum field, not free text. Consider converting to a category or enum type.",
      "possible_values": ["active", "pending", "cancelled", "shipped"],
      "action": "convert_categorical"
    }
  ]
}
```

### Pydantic response model
```python
class TypeFinding(BaseModel):
    column: str
    current_type: str
    detected_type: Literal[
        "json_object", "json_array", "csv_list", "numeric_as_string",
        "date_as_string", "boolean_as_string", "categorical_enum",
        "structured_id", "free_text", "html_xml", "base64_encoded",
    ]
    confidence: float = Field(ge=0, le=1)
    severity: Literal["critical", "warning", "info"]
    recommendation: str
    action: str
    details: dict = {}

class TypeInspectorResult(BaseModel):
    findings: list[TypeFinding]
```

### Heuristic pre-filter (before LLM)
We don't need the LLM for every string column.  Pre-filter first:

```python
def _needs_llm_inspection(col_profile: dict, sample_values: list) -> bool:
    """Quick regex check — only send to LLM if ambiguous."""
    # JSON-like: starts with { or [
    if any(str(v).strip().startswith(("{", "[")) for v in sample_values[:10]):
        return True
    # Numeric-as-string: all values parse as float
    try:
        [float(v) for v in sample_values[:10] if v is not None]
        return True
    except (ValueError, TypeError):
        pass
    # Low cardinality (< 1% unique) — LLM confirms if enum
    if col_profile.get("unique_percent", 100) < 1:
        return True
    # Date-like patterns
    import re
    date_pattern = re.compile(r"\d{4}[-/]\d{2}[-/]\d{2}")
    if any(date_pattern.match(str(v)) for v in sample_values[:10]):
        return True
    return False
```

This keeps LLM calls minimal: only "interesting" columns go to the LLM.

### Output integration
Findings get added to `profile.json` under each column:
```json
{
  "columns": {
    "metadata": {
      "type_category": "string",
      "llm_type_analysis": {
        "detected_type": "json_object",
        "confidence": 0.95,
        "recommendation": "...",
        "action": "parse_json"
      }
    }
  }
}
```

And a new deliverable: `type_inspector_results.json`

---

## Layer 2 — Insight Detector

### Problem it solves
After profiling + quality + KPIs, there may be patterns a human would notice
but our deterministic checks miss:
- "95% of rows have `country = 'US'` — is this dataset US-only?"
- "Column `age` has min=0, max=999 — likely has sentinel values"
- "Table `transactions` has 12 columns but `audit_log` has 45 — unusual imbalance"
- "Strong correlation between `discount` and `region` — pricing may vary by region"
- "The `created_at` column only spans 4 days — this looks like a test extract"

### Where it plugs in
```
Stage 5 (KPIs) → L2 Insight Detector → insights payload → Stage 6 (report)
```

### What the LLM sees
A condensed **dataset summary** (not raw data):

```json
{
  "dataset_overview": {
    "total_tables": 5,
    "total_rows": 125000,
    "total_columns": 47,
    "overall_quality_score": 72.3,
    "detected_industry": "e_commerce",
    "date_range": "2024-01-01 to 2024-01-04"
  },
  "tables": [
    {
      "name": "orders",
      "rows": 50000,
      "columns": 12,
      "quality_score": 68.5,
      "completeness": 82.1,
      "column_summaries": [
        {"name": "total", "type": "numeric", "min": 0.0, "max": 99999.99, "mean": 142.5, "null_pct": 0.2},
        {"name": "country", "type": "string", "unique_count": 3, "unique_pct": 0.006, "top_value": "US (95%)"},
        {"name": "age", "type": "numeric", "min": 0, "max": 999, "mean": 35.2, "std": 48.7, "null_pct": 5.1}
      ],
      "pii_found": ["email", "phone"],
      "quality_issues": ["5.1% nulls in age", "duplicates detected (3.2%)"]
    }
  ],
  "relationships": [
    {"from": "orders.customer_id", "to": "customers.id", "type": "fk", "match_rate": 0.98}
  ],
  "kpi_recommendations": ["Revenue per Order", "Customer Lifetime Value"]
}
```

### What the LLM returns
```json
{
  "insights": [
    {
      "category": "data_scope",
      "severity": "warning",
      "title": "Extremely narrow date range",
      "detail": "The dataset spans only 4 days (Jan 1–4, 2024). This is likely a test extract, not production data. Quality and KPI calculations may not be representative.",
      "affected_tables": ["orders", "transactions"],
      "recommendation": "Request a full month or quarter of data for meaningful KPI analysis."
    },
    {
      "category": "distribution_anomaly",
      "severity": "info",
      "title": "Dataset is 95% US-based",
      "detail": "The 'country' column in orders shows 95% of records are from the US, with only 3 unique countries. This may be intentional (US-focused business) or may indicate a filtered export.",
      "affected_tables": ["orders"],
      "recommendation": "Confirm with the client whether this is a US-only dataset or if international data is missing."
    },
    {
      "category": "sentinel_values",
      "severity": "warning",
      "title": "Possible sentinel values in 'age' column",
      "detail": "The age column has max=999 and high std deviation (48.7) compared to the mean (35.2), suggesting placeholder/sentinel values like 0 or 999 for missing data.",
      "affected_tables": ["orders"],
      "recommendation": "Consider filtering age values outside 1–120 range before analysis. Affects quality score."
    }
  ],
  "good_to_know": [
    "The dataset has strong referential integrity — 98% of order customer_ids match a customer record.",
    "PII was found in 2 columns (email, phone). These should be masked before any shared reporting.",
    "The most recommendable KPI is 'Revenue per Order' — all required fields are present and clean."
  ]
}
```

### Pydantic response model
```python
class Insight(BaseModel):
    category: Literal[
        "data_scope", "distribution_anomaly", "sentinel_values",
        "schema_oddity", "referential_integrity", "pii_risk",
        "quality_concern", "positive_signal", "cross_table_pattern",
    ]
    severity: Literal["critical", "warning", "info"]
    title: str
    detail: str
    affected_tables: list[str] = []
    recommendation: str = ""

class InsightDetectorResult(BaseModel):
    insights: list[Insight]
    good_to_know: list[str]           # bullet-point facts
    executive_summary: str = ""       # 2-3 sentence overview
```

### Output integration
New deliverable: `insights.json`  
Merged into `report_data.json` under a new `"llm_insights"` key.

---

## Layer 3 — Report Advisor

### Problem it solves
The current HTML report has a fixed layout: schema → quality → PII → relationships → KPIs.
Every dataset gets the same treatment regardless of what's interesting.

A dataset with critical PII should lead with PII warnings. A clean dataset with
great KPI readiness should lead with the KPI dashboard. A dataset with quality
issues should put the quality section front and center.

### Where it plugs in
```
report_data.json assembled → L3 Report Advisor → layout + narratives → HTML render
```

### What the LLM decides

#### A. Section ordering and emphasis
```json
{
  "report_layout": {
    "hero_metric": {
      "label": "Overall Data Quality",
      "value": "72.3 / 100",
      "color": "amber",
      "commentary": "Moderate quality — key issues in completeness and sentinel values need attention before production use."
    },
    "section_order": [
      "executive_summary",
      "quality_dashboard",
      "pii_warnings",
      "key_insights",
      "kpi_recommendations",
      "schema_overview",
      "relationships",
      "appendix"
    ],
    "sections": {
      "executive_summary": {
        "narrative": "This dataset contains 5 tables with 125K total rows from what appears to be a US-focused e-commerce operation. While the data shows good referential integrity (98% FK match rate), there are quality concerns: a narrow 4-day date range suggests this may be a test extract, and sentinel values in the age column inflate error rates. Two PII columns (email, phone) require masking before shared reporting.",
        "emphasis": "high"
      },
      "quality_dashboard": {
        "emphasis": "high",
        "narrative": "Quality scored 72.3/100 overall. Main deductions come from completeness (82%) and consistency (sentinel values in age). Freshness and uniqueness scores are healthy.",
        "visualizations": [
          {"type": "radar_chart", "data_key": "quality.components", "title": "Quality Components"},
          {"type": "bar_chart", "data_key": "quality.tables", "title": "Quality by Table"}
        ]
      },
      "kpi_recommendations": {
        "emphasis": "medium",
        "top_kpis": ["Revenue per Order", "Customer Lifetime Value"],
        "narrative": "6 KPIs recommended for e-commerce industry. Revenue per Order is immediately actionable — all required fields are present and clean. Customer Lifetime Value requires the date range issue to be resolved first.",
        "visualizations": [
          {"type": "status_table", "title": "KPI Readiness Matrix"}
        ]
      }
    }
  }
}
```

#### B. Figure and table choices
For each section, the LLM picks the most useful visualization:

| Data shape | LLM can choose from |
|---|---|
| Quality components (5 scores) | Radar chart, bar chart, gauge cluster |
| Quality by table | Horizontal bar, heatmap, table |
| Column profiles | Histogram, box plot, value distribution |
| PII locations | Table with risk highlighting |
| Relationships | Network graph, ER diagram, table |
| KPI readiness | Traffic-light table, Gantt-like readiness |
| Insights | Callout cards, timeline, priority list |

### Pydantic response model
```python
class Visualization(BaseModel):
    type: Literal[
        "radar_chart", "bar_chart", "horizontal_bar", "gauge",
        "heatmap", "table", "status_table", "network_graph",
        "callout_cards", "histogram", "box_plot",
    ]
    data_key: str           # dot-path into report_data.json
    title: str
    description: str = ""

class SectionDirective(BaseModel):
    emphasis: Literal["high", "medium", "low"]
    narrative: str          # LLM-written commentary for this section
    visualizations: list[Visualization] = []
    top_items: list[str] = []  # for KPIs: which to highlight

class ReportLayout(BaseModel):
    hero_metric: dict       # {label, value, color, commentary}
    section_order: list[str]
    sections: dict[str, SectionDirective]
    executive_summary: str  # 2-3 paragraph overview

class ReportAdvisorResult(BaseModel):
    layout: ReportLayout
    generation_notes: str = ""  # LLM's reasoning about why this layout
```

### Output integration
- New deliverable: `report_layout.json`
- The HTML renderer reads layout directives and generates accordingly
- Falls back to current fixed layout if LLM layer is disabled/fails

---

## Implementation Plan

### Step 1 — Foundation (`app/llm/`)
| Task | Effort | Priority |
|------|--------|----------|
| `BaseLLMClient` abstract class + `create_llm_client()` factory | 2h | P0 |
| OpenAI provider (sync, with retry + timeout) | 2h | P0 |
| Anthropic provider | 1h | P1 |
| Local/Ollama provider | 1h | P2 |
| Response cache (hash prompt → cached JSON) | 1h | P0 |
| Token counter + cost tracker | 1h | P0 |
| Config schema (`LLMConfig` Pydantic model) | 0.5h | P0 |
| Tests: mock provider, cache hit/miss, budget limit | 2h | P0 |

### Step 2 — Layer 1: Type Inspector
| Task | Effort | Priority |
|------|--------|----------|
| Heuristic pre-filter (`_needs_llm_inspection()`) | 1h | P0 |
| Prompt builder (compact column summaries) | 1h | P0 |
| `TypeInspectorResult` Pydantic model + validator | 0.5h | P0 |
| Integration in `cli.py` (between Stage 3 & 4) | 1h | P0 |
| Enrich `profile.json` with LLM findings | 0.5h | P0 |
| Tests: mock LLM, verify finding merges, disabled path | 2h | P0 |

### Step 3 — Layer 2: Insight Detector
| Task | Effort | Priority |
|------|--------|----------|
| Dataset summary builder (condense all pipeline data) | 1.5h | P0 |
| Prompt builder | 1h | P0 |
| `InsightDetectorResult` Pydantic model | 0.5h | P0 |
| Integration in `cli.py` (after Stage 5) | 1h | P0 |
| New deliverable `insights.json` | 0.5h | P0 |
| Tests: mock LLM, verify insights shape, good_to_know | 2h | P0 |

### Step 4 — Layer 3: Report Advisor
| Task | Effort | Priority |
|------|--------|----------|
| Report summary builder (profile + quality + KPI → compact) | 1.5h | P0 |
| Prompt builder | 1h | P0 |
| `ReportAdvisorResult` Pydantic model | 0.5h | P0 |
| HTML renderer: dynamic section ordering | 3h | P0 |
| HTML renderer: visualization selector | 3h | P1 |
| HTML renderer: LLM narrative injection | 1h | P0 |
| Fallback to static layout when disabled | 0.5h | P0 |
| Tests: mock LLM, static vs dynamic layout, all viz types | 3h | P0 |

### Step 5 — Quality & Polish
| Task | Effort | Priority |
|------|--------|----------|
| Re-run 10-dataset validation with LLM enabled | 2h | P0 |
| `llm_usage.json` deliverable + cost summary | 1h | P1 |
| Budget-exceeded graceful exit | 0.5h | P0 |
| Prompt tuning based on real results | 2h | P1 |
| Documentation: LLM config section in README | 1h | P0 |

**Total estimated effort: ~38 hours**

### Delivery order
```
Step 1 (foundation)  →  Step 2 (type inspector)  →  Step 3 (insights)
                                                  →  Step 4 (report advisor)
                                                  →  Step 5 (polish)
```
Steps 3 and 4 can be parallelised once foundation is in place.

---

## Cost Estimates (per pipeline run)

Using **gpt-4o-mini** ($0.15/M input, $0.60/M output):

| Layer | Input tokens (est.) | Output tokens (est.) | Cost |
|-------|--------------------:|---------------------:|-----:|
| L1: Type Inspector (10 cols) | ~2,000 | ~800 | $0.0008 |
| L2: Insight Detector | ~3,000 | ~1,500 | $0.0014 |
| L3: Report Advisor | ~2,500 | ~2,000 | $0.0016 |
| **Total per run** | **~7,500** | **~4,300** | **~$0.004** |

With **gpt-4o** ($2.50/M input, $10/M output):

| Layer | Cost |
|-------|-----:|
| **Total per run** | **~$0.06** |

Very cheap.  Even at 100 runs/day the monthly bill would be ~$12 (mini) or ~$180 (4o).

---

## Design Decisions to Confirm

| # | Decision | Options | Recommendation |
|---|----------|---------|----------------|
| 1 | Default LLM provider | OpenAI / Anthropic / both | **OpenAI** (widest compat, cheapest mini model) |
| 2 | Default model | gpt-4o-mini / gpt-4o / claude-3.5-haiku | **gpt-4o-mini** (cost/quality sweet spot) |
| 3 | Sync vs async calls | sync / async / both | **Sync** first (simpler), async later if perf matters |
| 4 | LLM output in reports | separate section / inline / both | **Both** — inline enrichment + dedicated "AI Insights" section |
| 5 | Cache scope | per-run / per-dataset / global | **Per-dataset hash** (same data = same insights) |
| 6 | Report layout | fully LLM-driven / LLM suggests, code decides | **LLM suggests, code decides** (safer, deterministic fallback) |

---

## File Inventory (new files)

```
app/llm/
    __init__.py                    # LLMService facade
    client.py                      # BaseLLMClient, create_llm_client()
    cache.py                       # ResponseCache (hash → JSON)
    cost_tracker.py                # token/cost accounting
    providers/
        __init__.py
        openai_provider.py
        anthropic_provider.py
        local_provider.py
    prompts/
        __init__.py
        type_inspector.py          # L1 prompt builder + response model
        insight_detector.py        # L2 prompt builder + response model
        report_advisor.py          # L3 prompt builder + response model
    schemas.py                     # All Pydantic response models

tests/
    test_llm_client.py             # provider mocking, cache, budget
    test_type_inspector.py         # L1 unit tests
    test_insight_detector.py       # L2 unit tests
    test_report_advisor.py         # L3 unit tests
    test_llm_integration.py        # full pipeline with mock LLM
```

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| LLM hallucination (invents columns) | Validate findings against actual column names; discard unknown |
| LLM downtime | 3 retries with exponential backoff; graceful skip after |
| Cost explosion | Hard budget cap; token counting before send; refuse if over |
| Slow responses | 30s timeout per call; async option for parallel layers |
| Non-determinism | Low temperature (0.1); cache identical inputs; seed parameter |
| Prompt injection via data | Sanitize sample values (truncate, escape); never execute returned code |
