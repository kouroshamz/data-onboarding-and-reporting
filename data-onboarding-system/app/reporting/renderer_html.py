"""HTML Report Renderer — 10-Section Consulting-Grade Report.

Sections:
  Cover Page
  1. Executive Summary
  2. Dataset Structural Overview
  3. Column-Level Profiling
  4. Data Quality Flags
  5. Sensitive Data & GDPR Assessment
  6. Business Insight Discovery
  7. Recommended KPIs
  8. Interesting Columns Detection
  9. Missing Data Strategy Recommendation
  10. Data Readiness Score
  Appendix — Full Column Statistics

Content-first design — visual template will be applied later.
"""

from __future__ import annotations

import html as html_mod
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger


# ── Public API ────────────────────────────────────────────────────────────────

def generate_html_report(data: Dict[str, Any], output_path: Path) -> None:
    """Generate the full 10-section HTML report."""
    chart_data = _prepare_chart_data(data)
    sections = [
        _render_cover(data),
        _render_s1_executive_summary(data),
        _render_s2_structural_overview(data),
        _render_s3_column_profiling(data),
        _render_s4_quality_flags(data),
        _render_s5_gdpr(data),
        _render_s6_business_insights(data),
        _render_s7_kpis(data),
        _render_s8_interesting_columns(data),
        _render_s9_missing_strategy(data),
        _render_s10_readiness_score(data),
        _render_visualizations(data, chart_data),
        _render_appendix(data),
    ]

    body = "\n".join(sections)
    html = _wrap_page(data, body, chart_data)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("HTML report written to {}", output_path)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _esc(val: Any) -> str:
    """HTML-escape any value."""
    return html_mod.escape(str(val)) if val is not None else "—"


def _fmt_num(val: Any, decimals: int = 1) -> str:
    """Format a number with thousands separator."""
    if val is None:
        return "—"
    try:
        n = float(val)
        if n == int(n) and decimals == 0:
            return f"{int(n):,}"
        return f"{n:,.{decimals}f}"
    except (ValueError, TypeError):
        return str(val)


def _badge(text: str, variant: str = "neutral") -> str:
    """Inline badge HTML."""
    return f'<span class="badge badge-{variant}">{_esc(text)}</span>'


def _risk_badge(risk: str) -> str:
    variants = {"critical": "error", "high": "error", "medium": "warning", "low": "success"}
    return _badge(risk.upper(), variants.get(risk.lower(), "neutral"))


def _score_variant(score: float, good: float = 80, warn: float = 60) -> str:
    if score >= good:
        return "success"
    elif score >= warn:
        return "warning"
    return "error"


def _pct_bar(pct: float, label: str = "") -> str:
    """Small inline percentage bar."""
    variant = "success" if pct < 10 else ("warning" if pct < 40 else "error")
    capped = min(max(pct, 0), 100)
    return (
        f'<div class="pct-bar-wrap">'
        f'<div class="pct-bar pct-bar-{variant}" style="width:{capped:.1f}%"></div>'
        f'<span class="pct-bar-label">{pct:.1f}% {_esc(label)}</span>'
        f'</div>'
    )


def _metric_card(label: str, value: str, variant: str = "", info: str = "") -> str:
    vc = f" metric-{variant}" if variant else ""
    info_html = (
        f'<span class="info-icon">i'
        f'<span class="info-tip">{_esc(info)}</span></span>'
    ) if info else ""
    return (
        f'<div class="metric{vc}">'
        f'<div class="metric-label">{_esc(label)}{info_html}</div>'
        f'<div class="metric-value">{value}</div>'
        f'</div>'
    )


def _section_open(num: int | str, title: str, id_: str = "") -> str:
    id_attr = f' id="{id_}"' if id_ else ""
    if isinstance(num, int):
        num_display = f'<span class="section-num">{num:02d}</span>'
    else:
        num_display = f'<span class="section-num">{num}</span>'
    return f'<section class="report-section"{id_attr}>{num_display}<h2>{_esc(title)}</h2>'


def _section_close() -> str:
    return "</section>"


# ── Cover Page ────────────────────────────────────────────────────────────────

def _render_cover(data: Dict[str, Any]) -> str:
    client = data.get("client", {})
    schema = data.get("schema", {})
    so = data.get("structural_overview", {})
    gen_at = data.get("generated_at", datetime.now().isoformat())

    source_type = data.get("source_type", "")
    if not source_type:
        source_type = data.get("connection_type", "CSV")

    tables = schema.get("table_count", len(schema.get("tables", {})))

    return f"""
<nav class="report-nav">
  <a href="/compare.html" class="nav-link">&larr; Dashboard</a>
  <div class="nav-actions">
    <a href="report.pdf" download class="nav-btn nav-btn-download">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v12m0 0l-4-4m4 4l4-4M5 20h14"/></svg>
      Download PDF
    </a>
    <a href="report_data.json" download class="nav-btn nav-btn-json">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
      JSON
    </a>
  </div>
</nav>
<header class="cover">
    <div class="cover-hero">
        <div class="cover-brand">DATA ONBOARDING</div>
        <h1 class="cover-title">DATA<br>ONBOARDING<br>REPORT</h1>
    </div>
    <div class="cover-meta">
        <div class="cover-label">PREPARED FOR</div>
        <div class="cover-client">{_esc(client.get('name', 'Client'))}</div>
        <div class="cover-label">DATE</div>
        <div class="cover-date">{gen_at[:10]}</div>
        <div class="cover-label">SOURCE</div>
        <div class="cover-source">{_esc(source_type)} &middot; {tables} table(s)</div>
    </div>
</header>
"""


# ── Section 1: Executive Summary ─────────────────────────────────────────────

def _render_s1_executive_summary(data: Dict[str, Any]) -> str:
    schema = data.get("schema", {})
    so = data.get("structural_overview", {})
    quality = data.get("quality", {})
    readiness = data.get("readiness_score", {})
    pii_summary = data.get("pii", {}).get("summary", data.get("pii", {}))
    llm_insights = data.get("llm_insights", {})

    total_rows = so.get("total_rows", sum(
        t.get("row_count", 0) for t in schema.get("tables", {}).values()
    ))
    total_cols = so.get("total_columns", sum(
        len(t.get("columns", [])) for t in schema.get("tables", {}).values()
    ))
    overall_quality = quality.get("overall_score", 0)
    readiness_total = readiness.get("total_score", "—")
    readiness_grade = readiness.get("grade", "")
    file_size = so.get("file_size_mb")
    mem_mb = so.get("estimated_memory_mb", "—")

    grade_variant = {"green": "success", "yellow": "warning", "red": "error"}.get(readiness_grade, "neutral")

    parts = [_section_open(1, "Executive Summary", "s1")]
    parts.append('<div class="metric-grid">')
    parts.append(_metric_card("Total Rows", _fmt_num(total_rows, 0)))
    parts.append(_metric_card("Total Columns", _fmt_num(total_cols, 0)))
    parts.append(_metric_card("Tables", str(schema.get("table_count", "—"))))
    parts.append(_metric_card("Quality Score", f"{_fmt_num(overall_quality)}/100", _score_variant(overall_quality)))
    parts.append(_metric_card("Readiness Score", f"{readiness_total}/100" if readiness_total != "—" else "—", grade_variant))
    if file_size:
        parts.append(_metric_card("File Size", f"{file_size} MB"))
    parts.append(_metric_card("Memory (in sample)", f"{mem_mb} MB"))
    parts.append(_metric_card("PII Detected", "Yes" if pii_summary.get("has_pii") else "No",
                              "error" if pii_summary.get("has_pii") else "success",
                              info="Personally Identifiable Information \u2014 names, emails, phone numbers, IDs or other data that could identify individuals"))
    parts.append("</div>")

    # LLM executive summary
    exec_summary = llm_insights.get("executive_summary", "")
    if exec_summary:
        parts.append(f'<div class="exec-narrative"><p>{_esc(exec_summary)}</p></div>')

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 2: Structural Overview ───────────────────────────────────────────

def _render_s2_structural_overview(data: Dict[str, Any]) -> str:
    so = data.get("structural_overview", {})
    if not so:
        return _section_open(2, "Dataset Structural Overview", "s2") + "<p>Not computed.</p>" + _section_close()

    parts = [_section_open(2, "Dataset Structural Overview", "s2")]

    parts.append('<div class="metric-grid">')
    parts.append(_metric_card("Duplicate Rows", f"{so.get('total_duplicate_rows', 0):,} ({so.get('duplicate_pct', 0):.1f}%)",
                              "warning" if so.get('duplicate_pct', 0) > 5 else "success",
                              info="Rows that appear more than once. High duplication may indicate ETL issues."))
    parts.append(_metric_card("Columns with Nulls", str(so.get("columns_with_nulls", 0)),
                              info="Number of columns containing at least one missing value."))
    parts.append(_metric_card("Fully Null Columns", str(so.get("columns_fully_null", 0)),
                              "error" if so.get("columns_fully_null", 0) > 0 else "success",
                              info="Columns where every value is missing — usually safe to drop."))
    parts.append(_metric_card("Constant Columns", str(len(so.get("constant_columns", []))),
                              "warning" if so.get("constant_columns") else "success",
                              info="Columns with only one unique value — provide zero information."))
    parts.append(_metric_card("Memory Usage", f"{so.get('estimated_memory_mb', 0)} MB",
                              info="Estimated in-memory footprint of the dataset."))
    parts.append("</div>")

    # Data type distribution
    dtype_dist = so.get("dtype_distribution", {})
    if dtype_dist:
        parts.append("<h3>Data Type Distribution</h3>")
        parts.append('<div class="chart-container" style="max-height:200px"><canvas id="chart-dtype-dist"></canvas></div>')
        parts.append('<table class="data-table"><thead><tr><th>Type</th><th>Count</th></tr></thead><tbody>')
        for dt, count in sorted(dtype_dist.items(), key=lambda x: -x[1]):
            parts.append(f"<tr><td>{_esc(dt)}</td><td>{count}</td></tr>")
        parts.append("</tbody></table>")

    # Suspicious ID columns
    suspicious = so.get("suspicious_id_columns", [])
    if suspicious:
        parts.append("<h3>Potential ID / Primary Key Columns</h3>")
        parts.append(f'<p>{", ".join(_esc(c) for c in suspicious)}</p>')

    # Constant columns detail
    constants = so.get("constant_columns", [])
    if constants:
        parts.append("<h3>Constant Columns (zero information)</h3>")
        parts.append(f'<p>{", ".join(_esc(c) for c in constants)}</p>')

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 3: Column-Level Profiling ────────────────────────────────────────

def _render_s3_column_profiling(data: Dict[str, Any]) -> str:
    profiles = data.get("profiles", data.get("profiling", {}))
    if not profiles:
        return _section_open(3, "Column-Level Profiling", "s3") + "<p>No profile data.</p>" + _section_close()

    parts = [_section_open(3, "Column-Level Profiling", "s3")]

    for table_name, profile in profiles.items():
        cols = profile.get("columns", {})
        if not cols:
            continue

        parts.append(f"<h3>{_esc(table_name)} ({len(cols)} columns)</h3>")
        parts.append('<div class="table-container"><table class="data-table">')
        parts.append(
            "<thead><tr>"
            "<th>Column</th><th>Type</th><th>Null %</th><th>Unique</th>"
            "<th>Min</th><th>Max</th><th>Mean</th><th>Std</th><th>Example</th>"
            "</tr></thead><tbody>"
        )

        for col_name, cp in cols.items():
            null_pct = cp.get("null_percent", 0)
            unique = cp.get("unique_count", "—")
            type_cat = cp.get("type_category", cp.get("dtype", "—"))
            stats = cp.get("stats", {})
            top_values = cp.get("top_values", [])
            example = top_values[0].get("value", "—") if top_values else "—"

            null_bar = _pct_bar(null_pct, "null")

            parts.append(
                f"<tr>"
                f"<td><strong>{_esc(col_name)}</strong></td>"
                f"<td>{_esc(type_cat)}</td>"
                f"<td>{null_bar}</td>"
                f"<td>{_fmt_num(unique, 0)}</td>"
                f"<td>{_fmt_num(stats.get('min'))}</td>"
                f"<td>{_fmt_num(stats.get('max'))}</td>"
                f"<td>{_fmt_num(stats.get('mean'))}</td>"
                f"<td>{_fmt_num(stats.get('std'))}</td>"
                f"<td class='example-cell'>{_esc(str(example)[:40])}</td>"
                f"</tr>"
            )

        parts.append("</tbody></table></div>")

        # Categorical top-5 / cardinality detail
        categoricals = {k: v for k, v in cols.items()
                        if v.get("type_category", v.get("dtype", "")).lower() in ("categorical", "object", "string", "bool")}
        if categoricals:
            parts.append("<h4>Categorical Column Detail</h4>")
            for col_name, cp in categoricals.items():
                top = cp.get("top_values", [])[:5]
                unique = cp.get("unique_count", 0)
                if not top:
                    continue
                parts.append(f'<div class="cat-detail"><strong>{_esc(col_name)}</strong> — {unique} unique values')
                parts.append('<table class="data-table compact"><thead><tr><th>Value</th><th>Count</th><th>%</th></tr></thead><tbody>')
                total = sum(v.get("count", 0) for v in top)
                for v in top:
                    cnt = v.get("count", 0)
                    pct = (cnt / total * 100) if total else 0
                    parts.append(f"<tr><td>{_esc(v.get('value', ''))}</td><td>{cnt:,}</td><td>{pct:.1f}%</td></tr>")
                parts.append("</tbody></table></div>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 4: Data Quality Flags ────────────────────────────────────────────

def _render_s4_quality_flags(data: Dict[str, Any]) -> str:
    quality = data.get("quality", {})
    profiles = data.get("profiles", data.get("profiling", {}))
    so = data.get("structural_overview", {})
    llm_type_findings = data.get("llm_type_findings", {})

    parts = [_section_open(4, "Data Quality Flags", "s4")]

    overall = quality.get("overall_score", 0)
    parts.append('<div class="metric-grid">')
    parts.append(_metric_card("Overall Quality", f"{_fmt_num(overall)}/100", _score_variant(overall),
                              info="Composite score (0–100) aggregating completeness, consistency, and validity."))
    _quality_tips = {
        "completeness": "Measures how few missing values exist across all columns.",
        "consistency": "Checks for format uniformity and logical coherence between fields.",
        "validity": "Assesses whether values fall within expected ranges and types.",
        "uniqueness": "Evaluates the proportion of distinct, non-duplicated records.",
    }
    components = quality.get("components", {})
    for comp_name, comp_val in components.items():
        tip = _quality_tips.get(comp_name, "")
        parts.append(_metric_card(comp_name.replace("_", " ").title(), f"{_fmt_num(comp_val)}/100", info=tip))
    parts.append("</div>")

    # Quality components chart
    if components:
        parts.append('<div class="chart-container" style="max-height:220px"><canvas id="chart-quality-components"></canvas></div>')

    # Collect flags
    flags: List[Dict[str, Any]] = []

    for table_name, profile in profiles.items():
        cols = profile.get("columns", {})
        for col_name, cp in cols.items():
            null_pct = cp.get("null_percent", 0)
            unique_pct = cp.get("unique_percent", 0)
            unique_count = cp.get("unique_count", 0)

            # High null
            if null_pct > 50:
                flags.append({
                    "table": table_name, "column": col_name,
                    "flag": "High Null Rate", "severity": "high",
                    "detail": f"{null_pct:.1f}% null",
                })
            # High cardinality
            type_cat = cp.get("type_category", cp.get("dtype", "")).lower()
            if type_cat in ("object", "string", "categorical") and unique_pct > 90:
                flags.append({
                    "table": table_name, "column": col_name,
                    "flag": "High Cardinality", "severity": "medium",
                    "detail": f"{unique_pct:.0f}% unique — may be an identifier",
                })
            # Constant
            if unique_count <= 1 and null_pct < 100:
                flags.append({
                    "table": table_name, "column": col_name,
                    "flag": "Constant Column", "severity": "medium",
                    "detail": "Single unique value — zero information",
                })

    # LLM type mismatches as flags
    for table_name, findings_data in llm_type_findings.items():
        for f in findings_data.get("findings", []):
            flags.append({
                "table": table_name, "column": f.get("column", "?"),
                "flag": "Type Mismatch", "severity": f.get("severity", "warning"),
                "detail": f"Stored as {f.get('current_type')}, detected as {f.get('detected_type')} — {f.get('recommendation', '')}",
            })

    # Per-table quality issues
    for table_name, tq in quality.get("tables", {}).items():
        for check in tq.get("checks", []):
            if check.get("status") in ("warning", "error"):
                flags.append({
                    "table": table_name, "column": check.get("column", "—"),
                    "flag": check.get("check", "Quality Issue"),
                    "severity": "high" if check.get("status") == "error" else "medium",
                    "detail": check.get("message", ""),
                })

    if flags:
        sev_order = {"high": 0, "critical": 0, "medium": 1, "low": 2, "warning": 1}
        flags.sort(key=lambda x: sev_order.get(x["severity"], 3))

        parts.append(f"<h3>{len(flags)} Flag(s) Detected</h3>")
        parts.append('<div class="table-container"><table class="data-table">')
        parts.append("<thead><tr><th>Table</th><th>Column</th><th>Flag</th><th>Severity</th><th>Detail</th></tr></thead><tbody>")
        for fl in flags:
            sev_variant = {"high": "error", "critical": "error", "medium": "warning"}.get(fl["severity"], "neutral")
            parts.append(
                f"<tr><td>{_esc(fl['table'])}</td>"
                f"<td><strong>{_esc(fl['column'])}</strong></td>"
                f"<td>{_esc(fl['flag'])}</td>"
                f"<td>{_badge(fl['severity'].upper(), sev_variant)}</td>"
                f"<td>{_esc(fl['detail'])}</td></tr>"
            )
        parts.append("</tbody></table></div>")
    else:
        parts.append('<p class="no-issues">No quality flags detected.</p>')

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 5: Sensitive Data & GDPR ─────────────────────────────────────────

def _render_s5_gdpr(data: Dict[str, Any]) -> str:
    gdpr = data.get("gdpr_assessment", {})
    pii = data.get("pii", {})
    pii_summary = pii.get("summary", pii) if isinstance(pii, dict) else {}

    parts = [_section_open(5, "Sensitive Data & GDPR Assessment", "s5")]

    n_findings = gdpr.get("total_pii_findings", pii_summary.get("total_pii_columns", 0))
    overall_risk = gdpr.get("overall_risk", pii_summary.get("risk_score", "none"))

    parts.append('<div class="metric-grid">')
    parts.append(_metric_card("PII Columns Detected", str(n_findings),
                              "error" if n_findings > 0 else "success",
                              info="Columns flagged as containing Personally Identifiable Information (names, emails, IDs, etc.)."))
    parts.append(_metric_card("Overall Risk", overall_risk.upper(),
                              {"critical": "error", "high": "error", "medium": "warning", "low": "success", "none": "success"}.get(overall_risk, "neutral"),
                              info="Combined GDPR risk level based on PII type, volume, and sensitivity."))
    if gdpr.get("has_special_category_data"):
        parts.append(_metric_card("Special Category (Art. 9)", "DETECTED", "error"))
    parts.append("</div>")

    # GDPR categories
    categories = gdpr.get("gdpr_categories", {})
    if categories:
        parts.append("<h3>GDPR Data Categories</h3>")
        parts.append('<table class="data-table"><thead><tr><th>Category</th><th>Risk</th><th>Columns</th><th>Count</th></tr></thead><tbody>')
        for cat_key, cat_data in categories.items():
            parts.append(
                f"<tr><td>{_esc(cat_data.get('label', cat_key))}</td>"
                f"<td>{_risk_badge(cat_data.get('risk', 'medium'))}</td>"
                f"<td>{', '.join(_esc(c) for c in cat_data.get('columns', []))}</td>"
                f"<td>{cat_data.get('count', 0)}</td></tr>"
            )
        parts.append("</tbody></table>")

    # PII findings detail
    by_table = pii.get("by_table", {})
    all_pii_cols = []
    for tbl, tpii in by_table.items():
        for col in tpii.get("pii_columns", []):
            all_pii_cols.append({**col, "table": tbl})

    if all_pii_cols:
        parts.append("<h3>Detected Sensitive Columns</h3>")
        parts.append('<table class="data-table"><thead><tr><th>Table</th><th>Column</th><th>PII Type</th><th>Sensitivity</th><th>Recommendation</th></tr></thead><tbody>')
        for col in all_pii_cols:
            sens = col.get("sensitivity", "medium")
            sev_v = {"high": "error", "medium": "warning", "low": "success"}.get(sens, "neutral")
            parts.append(
                f"<tr><td>{_esc(col.get('table', ''))}</td>"
                f"<td><strong>{_esc(col.get('column', ''))}</strong></td>"
                f"<td>{_esc(col.get('pii_type', ''))}</td>"
                f"<td>{_badge(sens.upper(), sev_v)}</td>"
                f"<td>{_esc(col.get('recommendation', ''))}</td></tr>"
            )
        parts.append("</tbody></table>")

    # GDPR Recommendations
    recs = gdpr.get("recommendations", [])
    if recs:
        parts.append("<h3>GDPR Compliance Recommendations</h3>")
        for rec in recs:
            pri = rec.get("priority", "medium")
            pri_v = {"critical": "error", "high": "error", "medium": "warning", "low": "success"}.get(pri, "neutral")
            parts.append(
                f'<div class="rec-card">'
                f'<div class="rec-header">'
                f'{_badge(pri.upper(), pri_v)} '
                f'<strong>{_esc(rec.get("area", ""))}</strong>'
                f'</div>'
                f'<p>{_esc(rec.get("recommendation", ""))}</p>'
                f'</div>'
            )

    if not categories and not all_pii_cols:
        parts.append('<p class="no-issues">No sensitive data detected. Confirm with domain expert.</p>')

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 6: Business Insight Discovery ────────────────────────────────────

def _render_s6_business_insights(data: Dict[str, Any]) -> str:
    classifications = data.get("column_classifications", {})
    llm_insights = data.get("llm_insights", {})

    parts = [_section_open(6, "Business Insight Discovery", "s6")]

    by_cat = classifications.get("by_category", {})
    summary = classifications.get("summary", {})

    if by_cat:
        parts.append(f'<p>{summary.get("total_classified", 0)} column(s) classified across '
                      f'{len(summary.get("categories_found", []))} business categories.</p>')

        category_labels = {
            "revenue": ("Revenue / Monetary", "Columns representing financial values"),
            "timestamp": ("Timestamp / Date", "Time-related columns enabling trend analysis"),
            "geographic": ("Geographic", "Location-related columns"),
            "status_lifecycle": ("Status / Lifecycle", "State or phase columns"),
            "device_identifier": ("Device Identifiers", "Hardware or IoT device references"),
            "customer_identifier": ("Customer Identifiers", "Person or account references"),
        }

        for cat_key, entries in by_cat.items():
            label, desc = category_labels.get(cat_key, (cat_key.replace("_", " ").title(), ""))
            parts.append(f'<div class="insight-category">')
            parts.append(f'<h4>{_esc(label)}</h4>')
            if desc:
                parts.append(f'<p class="cat-desc">{_esc(desc)}</p>')
            parts.append('<table class="data-table compact"><thead><tr><th>Table</th><th>Column</th><th>Confidence</th></tr></thead><tbody>')
            for e in entries:
                parts.append(f"<tr><td>{_esc(e.get('table', ''))}</td><td><strong>{_esc(e.get('column', ''))}</strong></td><td>{e.get('confidence', 0):.0%}</td></tr>")
            parts.append("</tbody></table></div>")
    else:
        parts.append("<p>No strong business column classifications detected.</p>")

    # LLM insights
    insights_list = llm_insights.get("insights", [])
    good_to_know = llm_insights.get("good_to_know", [])

    if insights_list:
        parts.append("<h3>AI-Detected Insights</h3>")
        for ins in insights_list:
            sev = ins.get("severity", "info")
            sev_v = {"critical": "error", "warning": "warning", "info": "neutral"}.get(sev, "neutral")
            parts.append(
                f'<div class="insight-card">'
                f'{_badge(ins.get("category", "insight").upper(), sev_v)} '
                f'<strong>{_esc(ins.get("title", ""))}</strong>'
                f'<p>{_esc(ins.get("detail", ""))}</p>'
                f'</div>'
            )

    if good_to_know:
        parts.append("<h3>Good to Know</h3>")
        parts.append("<ul>")
        for item in good_to_know:
            parts.append(f"<li>{_esc(item)}</li>")
        parts.append("</ul>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 7: Recommended KPIs ──────────────────────────────────────────────

def _render_s7_kpis(data: Dict[str, Any]) -> str:
    kpis = data.get("kpis", [])
    industry = data.get("industry", {})

    parts = [_section_open(7, "Recommended KPIs", "s7")]

    ind_name = industry.get("industry", "general")
    ind_conf = industry.get("confidence", 0)

    parts.append('<div class="metric-grid">')
    parts.append(_metric_card("Detected Industry", ind_name.upper()))
    parts.append(_metric_card("Confidence", f"{ind_conf:.0%}"))
    parts.append(_metric_card("KPIs Recommended", str(len(kpis))))
    parts.append("</div>")

    if kpis:
        for kpi in kpis:
            status = kpi.get("status", "unknown")
            status_v = {"ready": "success", "partial": "warning"}.get(status, "error")
            parts.append(
                f'<div class="kpi-card">'
                f'<div class="kpi-header">'
                f'<span class="kpi-title">{_esc(kpi.get("name", ""))}</span>'
                f'{_badge(status.upper(), status_v)}'
                f'</div>'
                f'<div class="kpi-category">{_esc(kpi.get("category", ""))}</div>'
                f'<p>{_esc(kpi.get("description", ""))}</p>'
            )
            readiness = kpi.get("readiness", {})
            if readiness:
                cols = readiness.get("required_columns", [])
                missing = readiness.get("missing", [])
                if cols:
                    found = [c for c in cols if c not in missing]
                    parts.append(f'<div class="kpi-readiness">')
                    if found:
                        parts.append(f'<span class="kpi-found">Found: {", ".join(_esc(c) for c in found)}</span>')
                    if missing:
                        parts.append(f'<span class="kpi-missing">Missing: {", ".join(_esc(c) for c in missing)}</span>')
                    parts.append("</div>")
            parts.append("</div>")
    else:
        parts.append("<p>No KPIs recommended for this dataset context.</p>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 8: Interesting Columns ───────────────────────────────────────────

def _render_s8_interesting_columns(data: Dict[str, Any]) -> str:
    ic = data.get("interesting_columns", {})
    parts = [_section_open(8, "Interesting Columns Detection", "s8")]

    columns = ic.get("interesting_columns", [])
    correlations = ic.get("correlations", {})

    if columns:
        parts.append(f"<p>{len(columns)} column(s) flagged as analytically interesting.</p>")
        parts.append('<div class="table-container"><table class="data-table">')
        parts.append("<thead><tr><th>Table</th><th>Column</th><th>Interest Score</th><th>Reason(s)</th></tr></thead><tbody>")
        for col in columns[:25]:
            reasons_text = "; ".join(r.get("description", "") for r in col.get("reasons", []))
            score = col.get("interest_score", 0)
            parts.append(
                f"<tr><td>{_esc(col.get('table', ''))}</td>"
                f"<td><strong>{_esc(col.get('column', ''))}</strong></td>"
                f"<td>{_badge(f'{score:.0%}', 'warning' if score > 0.5 else 'neutral')}</td>"
                f"<td>{_esc(reasons_text)}</td></tr>"
            )
        parts.append("</tbody></table></div>")
    else:
        parts.append("<p>No columns flagged as particularly interesting.</p>")

    # Correlations
    if correlations:
        parts.append("<h3>Notable Correlations</h3>")
        for tbl, corr_list in correlations.items():
            if not corr_list:
                continue
            parts.append(f"<h4>{_esc(tbl)}</h4>")
            parts.append('<table class="data-table compact"><thead><tr><th>Column A</th><th>Column B</th><th>Pearson r</th><th>Strength</th><th>Revenue-Related</th></tr></thead><tbody>')
            for c in corr_list[:10]:
                r = c.get("pearson_r", 0)
                strength = c.get("strength", "moderate")
                rev = "Yes" if c.get("involves_revenue") else "—"
                parts.append(
                    f"<tr><td>{_esc(c['col_a'])}</td><td>{_esc(c['col_b'])}</td>"
                    f"<td>{r:.3f}</td>"
                    f"<td>{_badge(strength.upper(), 'warning' if strength == 'very_strong' else 'neutral')}</td>"
                    f"<td>{rev}</td></tr>"
                )
            parts.append("</tbody></table>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 9: Missing Data Strategy ─────────────────────────────────────────

def _render_s9_missing_strategy(data: Dict[str, Any]) -> str:
    ms = data.get("missing_strategy", {})
    parts = [_section_open(9, "Missing Data Strategy Recommendation", "s9")]

    strategies = ms.get("strategies", [])
    summary = ms.get("summary", {})

    if summary:
        parts.append('<div class="metric-grid">')
        parts.append(_metric_card("Columns with Nulls", str(summary.get("total_columns_with_nulls", 0))))
        parts.append(_metric_card("Drop Recommended", str(summary.get("drop_recommended", 0))))
        parts.append(_metric_card("Impute Recommended", str(summary.get("impute_recommended", 0))))
        parts.append(_metric_card("Transform Recommended", str(summary.get("transform_recommended", 0))))
        parts.append("</div>")

    if strategies:
        parts.append('<div class="table-container"><table class="data-table">')
        parts.append(
            "<thead><tr><th>Table</th><th>Column</th><th>Null %</th>"
            "<th>Issue</th><th>Recommended Treatment</th><th>Priority</th></tr></thead><tbody>"
        )
        for s in strategies:
            pri = s.get("priority", "low")
            pri_v = {"high": "error", "medium": "warning", "low": "neutral"}.get(pri, "neutral")
            parts.append(
                f"<tr><td>{_esc(s.get('table', ''))}</td>"
                f"<td><strong>{_esc(s.get('column', ''))}</strong></td>"
                f"<td>{s.get('null_percent', 0):.1f}%</td>"
                f"<td>{_esc(s.get('issue', ''))}</td>"
                f"<td>{_esc(s.get('treatment', ''))}</td>"
                f"<td>{_badge(pri.upper(), pri_v)}</td></tr>"
            )
        parts.append("</tbody></table></div>")
    else:
        parts.append('<p class="no-issues">No missing data — all columns are complete.</p>')

    parts.append(_section_close())
    return "\n".join(parts)


# ── Section 10: Data Readiness Score ─────────────────────────────────────────

def _render_s10_readiness_score(data: Dict[str, Any]) -> str:
    rs = data.get("readiness_score", {})
    parts = [_section_open(10, "Data Readiness Score", "s10")]

    if not rs:
        parts.append("<p>Readiness score not computed.</p>")
        parts.append(_section_close())
        return "\n".join(parts)

    total = rs.get("total_score", 0)
    grade = rs.get("grade", "red")
    label = rs.get("label", "")
    grade_v = {"green": "success", "yellow": "warning", "red": "error"}.get(grade, "neutral")

    parts.append(f'<div class="readiness-hero">')
    parts.append(f'<div class="readiness-score-big {grade}">{total}<span class="readiness-max">/100</span></div>')
    parts.append(f'<div class="readiness-label">{_badge(label, grade_v)}</div>')
    parts.append("</div>")

    # Readiness breakdown chart
    parts.append('<div class="chart-container" style="max-height:220px"><canvas id="chart-readiness-breakdown"></canvas></div>')

    # Components
    components = rs.get("components", {})
    if components:
        parts.append("<h3>Score Breakdown</h3>")
        parts.append('<div class="readiness-grid">')
        for comp_key, comp in components.items():
            c_score = comp.get("score", 0)
            c_max = comp.get("max", 20)
            c_label = comp.get("label", comp_key)
            c_variant = _score_variant(c_score / max(c_max, 1) * 100)

            deductions = comp.get("deductions", [])
            boosts = comp.get("boosts", [])

            parts.append(f'<div class="readiness-component">')
            parts.append(f'<div class="rc-header">')
            parts.append(f'<span class="rc-label">{_esc(c_label)}</span>')
            parts.append(f'<span class="rc-score badge badge-{c_variant}">{c_score}/{c_max}</span>')
            parts.append(f'</div>')

            if deductions:
                parts.append('<ul class="rc-deductions">')
                for d in deductions:
                    parts.append(f'<li class="rc-minus">&minus; {_esc(d)}</li>')
                parts.append("</ul>")
            if boosts:
                parts.append('<ul class="rc-boosts">')
                for b in boosts:
                    parts.append(f'<li class="rc-plus">+ {_esc(b)}</li>')
                parts.append("</ul>")

            parts.append("</div>")
        parts.append("</div>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Appendix ─────────────────────────────────────────────────────────────────

def _render_appendix(data: Dict[str, Any]) -> str:
    profiles = data.get("profiles", data.get("profiling", {}))
    parts = [_section_open("A", "Appendix — Full Column Statistics", "appendix")]

    for table_name, profile in profiles.items():
        cols = profile.get("columns", {})
        if not cols:
            continue

        parts.append(f"<h3>{_esc(table_name)}</h3>")
        for col_name, cp in cols.items():
            stats = cp.get("stats", {})
            top_values = cp.get("top_values", [])
            llm_type = cp.get("llm_type_analysis", {})

            parts.append(f'<div class="appendix-col">')
            parts.append(f'<h4>{_esc(col_name)}</h4>')
            parts.append('<table class="data-table compact"><tbody>')

            for key, label in [
                ("dtype", "Raw Dtype"),
                ("type_category", "Category"),
                ("null_count", "Null Count"),
                ("null_percent", "Null %"),
                ("unique_count", "Unique Values"),
                ("unique_percent", "Unique %"),
            ]:
                val = cp.get(key)
                if val is not None:
                    parts.append(f"<tr><td><strong>{label}</strong></td><td>{_fmt_num(val) if isinstance(val, (int, float)) else _esc(val)}</td></tr>")

            for key, label in [
                ("mean", "Mean"), ("median", "Median"), ("std", "Std Dev"),
                ("min", "Min"), ("max", "Max"), ("p25", "P25"), ("p75", "P75"),
                ("skewness", "Skewness"), ("kurtosis", "Kurtosis"),
            ]:
                val = stats.get(key)
                if val is not None:
                    parts.append(f"<tr><td><strong>{label}</strong></td><td>{_fmt_num(val, 3)}</td></tr>")

            outlier_pct = cp.get("outlier_percent") or stats.get("outlier_percent")
            if outlier_pct is not None:
                parts.append(f"<tr><td><strong>Outlier %</strong></td><td>{_fmt_num(outlier_pct)}%</td></tr>")

            if llm_type:
                parts.append(f'<tr><td><strong>LLM Detected Type</strong></td><td>{_esc(llm_type.get("detected_type", ""))}</td></tr>')
                parts.append(f'<tr><td><strong>LLM Recommendation</strong></td><td>{_esc(llm_type.get("recommendation", ""))}</td></tr>')

            parts.append("</tbody></table>")

            if top_values:
                parts.append(f'<details><summary>Top {len(top_values)} values</summary>')
                parts.append('<table class="data-table compact"><thead><tr><th>Value</th><th>Count</th></tr></thead><tbody>')
                for v in top_values:
                    parts.append(f"<tr><td>{_esc(v.get('value', ''))}</td><td>{v.get('count', 0):,}</td></tr>")
                parts.append("</tbody></table></details>")

            parts.append("</div>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Chart data preparation ────────────────────────────────────────────────────

def _prepare_chart_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract chart-relevant data for embedded Chart.js visualizations."""
    charts: Dict[str, Any] = {}

    # 1. Data type distribution
    so = data.get("structural_overview", {})
    charts["dtype_dist"] = so.get("dtype_distribution", {})

    # 2. Null % per column (sorted, top 25)
    null_data = []
    for table_name, profile in data.get("profiles", data.get("profiling", {})).items():
        for col_name, cp in profile.get("columns", {}).items():
            null_pct = cp.get("null_percent", 0)
            if null_pct > 0:
                null_data.append({"col": col_name, "pct": round(null_pct, 1)})
    null_data.sort(key=lambda x: -x["pct"])
    charts["null_pct"] = null_data[:25]

    # 3. Quality components
    quality = data.get("quality", {})
    charts["quality_components"] = quality.get("components", {})
    charts["quality_overall"] = quality.get("overall_score", 0)

    # 4. Correlations (top 15)
    ic = data.get("interesting_columns", {})
    corr_data = []
    for _table, corr_list in ic.get("correlations", {}).items():
        for c in (corr_list or [])[:15]:
            corr_data.append({
                "pair": f"{c.get('col_a', '?')} \u2194 {c.get('col_b', '?')}",
                "r": round(c.get("pearson_r", 0), 3),
                "strength": c.get("strength", "moderate"),
            })
    charts["correlations"] = corr_data[:15]

    # 5. Readiness components
    rs = data.get("readiness_score", {})
    readiness_comps = []
    for _comp_key, comp in rs.get("components", {}).items():
        readiness_comps.append({
            "label": comp.get("label", _comp_key),
            "score": comp.get("score", 0),
            "max": comp.get("max", 20),
        })
    charts["readiness"] = {
        "total": rs.get("total_score", 0),
        "grade": rs.get("grade", "red"),
        "components": readiness_comps,
    }

    # 6. Numeric column statistics (box plot data)
    numeric_stats = []
    for table_name, profile in data.get("profiles", data.get("profiling", {})).items():
        for col_name, cp in profile.get("columns", {}).items():
            type_cat = cp.get("type_category", cp.get("dtype", "")).lower()
            if type_cat in ("numeric", "integer", "float") or "int" in type_cat or "float" in type_cat:
                stats = cp.get("statistics", cp.get("stats", {}))
                if stats.get("min") is not None and stats.get("max") is not None:
                    numeric_stats.append({
                        "col": col_name,
                        "min": stats.get("min", 0),
                        "q25": stats.get("q25", stats.get("p25", 0)),
                        "median": stats.get("median", 0),
                        "q75": stats.get("q75", stats.get("p75", 0)),
                        "max": stats.get("max", 0),
                        "mean": stats.get("mean", 0),
                    })
    charts["numeric_stats"] = numeric_stats[:20]

    # 7. Categorical distributions (top 6 columns, top 6 values each)
    cat_data = []
    for table_name, profile in data.get("profiles", data.get("profiling", {})).items():
        for col_name, cp in profile.get("columns", {}).items():
            type_cat = cp.get("type_category", cp.get("dtype", "")).lower()
            if type_cat in ("categorical", "object", "string", "bool"):
                top = cp.get("top_values", [])[:6]
                if top:
                    cat_data.append({
                        "col": col_name,
                        "values": [
                            {"value": str(v.get("value", "?"))[:20], "count": v.get("count", 0)}
                            for v in top
                        ],
                    })
    charts["categorical"] = cat_data[:6]

    return charts


# ── Visualizations Section ───────────────────────────────────────────────────

def _render_visualizations(data: Dict[str, Any], chart_data: Dict[str, Any]) -> str:
    """Render the Data Visualizations section with Chart.js canvases."""
    parts = [_section_open(11, "Data Visualizations", "s11")]

    # Null % chart
    if chart_data.get("null_pct"):
        parts.append("<h3>Null Percentage by Column</h3>")
        parts.append('<div class="chart-container" style="max-height:300px">'
                     '<canvas id="chart-null-pct"></canvas></div>')

    # Correlation strength chart
    if chart_data.get("correlations"):
        parts.append("<h3>Correlation Strength</h3>")
        parts.append(
            '<p>Top column pairs ranked by absolute Pearson correlation.</p>'
        )
        parts.append('<div class="chart-container" style="max-height:300px">'
                     '<canvas id="chart-correlations"></canvas></div>')

    # Categorical distributions
    if chart_data.get("categorical"):
        parts.append("<h3>Categorical Value Distributions</h3>")
        parts.append('<div class="chart-grid">')
        for i, cat in enumerate(chart_data["categorical"]):
            parts.append(
                f'<div class="chart-item">'
                f'<h4>{_esc(cat["col"])}</h4>'
                f'<div class="chart-container" style="max-height:180px">'
                f'<canvas id="chart-cat-{i}"></canvas></div></div>'
            )
        parts.append("</div>")

    # Numeric box plot approximation
    if chart_data.get("numeric_stats"):
        parts.append("<h3>Numeric Column Ranges</h3>")
        parts.append(
            '<p>Range bars showing min–max span with IQR (Q25–Q75) overlay for each numeric column.</p>'
        )
        parts.append('<div class="chart-container" style="max-height:350px">'
                     '<canvas id="chart-numeric-range"></canvas></div>')

    if not any(chart_data.get(k) for k in ("null_pct", "correlations", "categorical", "numeric_stats")):
        parts.append("<p>No additional visualizations available for this dataset.</p>")

    parts.append(_section_close())
    return "\n".join(parts)


# ── Chart.js initialization script ───────────────────────────────────────────

_CHART_INIT_SCRIPT = """<script>
document.addEventListener('DOMContentLoaded', function() {
  var D = window.__CHART_DATA;
  if (!D) return;

  var P = ['#aea1ff','#ffde58','#3debb6','#fdd1f1','#f5fe8b','#bec2c7','#ff9966','#66ccff','#ff6b6b','#69f0ae'];

  /* ── Data Type Distribution ───────────────────────────────── */
  if (D.dtype_dist && Object.keys(D.dtype_dist).length > 0) {
    var ctx = document.getElementById('chart-dtype-dist');
    if (ctx) {
      var lbl = Object.keys(D.dtype_dist);
      new Chart(ctx, {
        type:'bar', data:{ labels:lbl,
          datasets:[{ data:Object.values(D.dtype_dist),
            backgroundColor:lbl.map(function(_,i){return P[i%P.length]}), borderRadius:4 }]
        },
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{legend:{display:false}},
          scales:{ x:{title:{display:true,text:'Columns'},grid:{color:'#eee'}}, y:{grid:{display:false}} }
        }
      });
    }
  }

  /* ── Null % by Column ─────────────────────────────────────── */
  if (D.null_pct && D.null_pct.length > 0) {
    var ctx2 = document.getElementById('chart-null-pct');
    if (ctx2) {
      new Chart(ctx2, {
        type:'bar', data:{ labels:D.null_pct.map(function(i){return i.col}),
          datasets:[{ data:D.null_pct.map(function(i){return i.pct}),
            backgroundColor:D.null_pct.map(function(i){return i.pct>50?'#f87171':i.pct>20?'#ffde58':'#3debb6'}),
            borderRadius:3 }]
        },
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{legend:{display:false}},
          scales:{ x:{max:100,title:{display:true,text:'Null %'},grid:{color:'#eee'}}, y:{grid:{display:false}} }
        }
      });
    }
  }

  /* ── Quality Components ───────────────────────────────────── */
  if (D.quality_components && Object.keys(D.quality_components).length > 0) {
    var ctx3 = document.getElementById('chart-quality-components');
    if (ctx3) {
      var ql = Object.keys(D.quality_components).map(function(k){return k.replace(/_/g,' ')});
      var qv = Object.values(D.quality_components);
      new Chart(ctx3, {
        type:'bar', data:{ labels:ql,
          datasets:[{ data:qv,
            backgroundColor:qv.map(function(v){return v>=80?'#3debb6':v>=60?'#ffde58':'#fdd1f1'}),
            borderRadius:4 }]
        },
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{legend:{display:false}},
          scales:{ x:{max:100,title:{display:true,text:'Score'},grid:{color:'#eee'}}, y:{grid:{display:false}} }
        }
      });
    }
  }

  /* ── Correlation Bars ─────────────────────────────────────── */
  if (D.correlations && D.correlations.length > 0) {
    var ctx4 = document.getElementById('chart-correlations');
    if (ctx4) {
      var items = D.correlations;
      new Chart(ctx4, {
        type:'bar', data:{ labels:items.map(function(i){return i.pair}),
          datasets:[{ data:items.map(function(i){return Math.abs(i.r)}),
            backgroundColor:items.map(function(i){var a=Math.abs(i.r);return a>0.8?'#aea1ff':a>0.5?'#ffde58':'#bec2c7'}),
            borderRadius:3 }]
        },
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{legend:{display:false}},
          scales:{ x:{max:1,title:{display:true,text:'|Pearson r|'},grid:{color:'#eee'}}, y:{grid:{display:false}} }
        }
      });
    }
  }

  /* ── Categorical Distributions ─────────────────────────────── */
  if (D.categorical && D.categorical.length > 0) {
    D.categorical.forEach(function(cat, idx) {
      var ctx5 = document.getElementById('chart-cat-' + idx);
      if (!ctx5) return;
      new Chart(ctx5, {
        type:'bar', data:{
          labels:cat.values.map(function(v){return v.value.length>15?v.value.slice(0,13)+'…':v.value}),
          datasets:[{ data:cat.values.map(function(v){return v.count}),
            backgroundColor:P[idx%P.length]+'cc', borderColor:P[idx%P.length],
            borderWidth:1, borderRadius:3 }]
        },
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{legend:{display:false}},
          scales:{ x:{title:{display:true,text:'Count'},grid:{color:'#eee'}}, y:{grid:{display:false}} }
        }
      });
    });
  }

  /* ── Readiness Breakdown ───────────────────────────────────── */
  if (D.readiness && D.readiness.components && D.readiness.components.length > 0) {
    var ctx6 = document.getElementById('chart-readiness-breakdown');
    if (ctx6) {
      var rc = D.readiness.components;
      new Chart(ctx6, {
        type:'bar', data:{ labels:rc.map(function(c){return c.label}),
          datasets:[
            { label:'Score', data:rc.map(function(c){return c.score}),
              backgroundColor:rc.map(function(c){return c.score/c.max>=0.8?'#3debb6':c.score/c.max>=0.6?'#ffde58':'#fdd1f1'}),
              borderRadius:4 },
            { label:'Remaining', data:rc.map(function(c){return c.max-c.score}),
              backgroundColor:'#ececec33', borderRadius:4 }
          ]},
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{legend:{position:'top'}},
          scales:{ x:{stacked:true,title:{display:true,text:'Points'},grid:{color:'#eee'}},
                   y:{stacked:true,grid:{display:false}} }
        }
      });
    }
  }

  /* ── Numeric Range Bars ────────────────────────────────────── */
  if (D.numeric_stats && D.numeric_stats.length > 0) {
    var ctx7 = document.getElementById('chart-numeric-range');
    if (ctx7) {
      var ns = D.numeric_stats;
      /* Normalise each column to 0-100 for comparability */
      var normData = ns.map(function(c) {
        var range = c.max - c.min;
        if (range === 0) return {col:c.col, iqrLo:0, iqrHi:100, medPct:50};
        return {
          col: c.col,
          iqrLo: ((c.q25 - c.min) / range) * 100,
          iqrHi: ((c.q75 - c.min) / range) * 100,
          medPct: ((c.median - c.min) / range) * 100
        };
      });
      new Chart(ctx7, {
        type:'bar', data:{
          labels: ns.map(function(c){return c.col}),
          datasets:[
            { label:'IQR (Q25–Q75)', data:normData.map(function(d){return [d.iqrLo, d.iqrHi]}),
              backgroundColor:'#aea1ff88', borderColor:'#aea1ff', borderWidth:1, borderRadius:2,
              borderSkipped:false, barPercentage:0.7 },
            { label:'Full Range', data:normData.map(function(d){return [0, 100]}),
              backgroundColor:'#dedede33', borderColor:'#dedede66', borderWidth:1, borderRadius:1,
              borderSkipped:false, barPercentage:0.3 }
          ]},
        options:{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{position:'top'},
            tooltip:{ callbacks:{
              label: function(ctx) {
                var i = ctx.dataIndex;
                var c = ns[i];
                return ctx.dataset.label + ': ' + c.min.toFixed(1) + ' – ' + c.max.toFixed(1) +
                  ' (Q25=' + c.q25.toFixed(1) + ', Med=' + c.median.toFixed(1) + ', Q75=' + c.q75.toFixed(1) + ')';
              }
            }}
          },
          scales:{ x:{min:0,max:100,title:{display:true,text:'Normalised Range (%)'},grid:{color:'#eee'}},
                   y:{grid:{display:false}} }
        }
      });
    }
  }
});
</script>"""


# ── Page wrapper (CSS + structure) ───────────────────────────────────────────

def _wrap_page(data: Dict[str, Any], body: str, chart_data: Optional[Dict] = None) -> str:
    client_name = data.get("client", {}).get("name", "Client")
    gen_at = data.get("generated_at", datetime.now().isoformat())

    # Prepare chart embed
    chart_embed = ""
    if chart_data:
        chart_json = json.dumps(chart_data, default=str)
        chart_embed = f"<script>window.__CHART_DATA={chart_json}</script>\n{_CHART_INIT_SCRIPT}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Onboarding Report — {_esc(client_name)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&family=Roboto+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
/* ── Design tokens ──────────────────────────────────────────────────────── */
:root{{
  --c-text:#232323;
  --c-text-muted:#5c5c5c;
  --c-bg:#ffffff;
  --c-surface:#f9f9f9;
  --c-border:#dedede;
  --c-border-light:#ececec;
  --c-yellow:#ffde58;
  --c-lime:#f5fe8b;
  --c-purple:#aea1ff;
  --c-pink:#fdd1f1;
  --c-mint:#3debb6;
  --c-pale-yellow:#fffa99;
  --c-stroke:#bec2c7;
  --font-heading:'JetBrains Mono',monospace;
  --font-body:'Roboto Mono',monospace;
}}

/* ── Base ───────────────────────────────────────────────────────────────── */
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:var(--font-body);font-size:13px;
  line-height:1.7;color:var(--c-text);background:var(--c-surface);padding:0}}
.page-wrap{{max-width:1100px;margin:0 auto;background:var(--c-bg);
  border:1px solid var(--c-border);min-height:100vh}}

/* ── Cover ──────────────────────────────────────────────────────────────── */
.cover{{display:flex;min-height:520px;border-bottom:3px solid var(--c-text)}}
.cover-hero{{flex:1.2;background:var(--c-yellow);padding:60px 44px;display:flex;flex-direction:column;justify-content:flex-end}}
.cover-brand{{font-family:var(--font-heading);font-size:15px;font-weight:700;
  letter-spacing:6px;color:var(--c-text);margin-bottom:auto}}
.cover-title{{font-family:var(--font-heading);font-size:48px;font-weight:800;
  line-height:1.05;color:var(--c-text)}}
.cover-meta{{flex:0.8;padding:60px 36px;display:flex;flex-direction:column;justify-content:flex-end;gap:4px}}
.cover-label{{font-family:var(--font-body);font-size:10px;font-weight:600;
  letter-spacing:2px;color:var(--c-text-muted);text-transform:uppercase;margin-top:20px}}
.cover-label:first-child{{margin-top:auto}}
.cover-client{{font-family:var(--font-heading);font-size:18px;font-weight:700;color:var(--c-text)}}
.cover-date{{font-family:var(--font-body);font-size:13px;color:var(--c-text)}}
.cover-source{{font-family:var(--font-body);font-size:13px;color:var(--c-text-muted)}}

/* ── Sections ───────────────────────────────────────────────────────────── */
.report-section{{padding:36px 44px 40px;border-bottom:1px solid var(--c-border)}}
.section-num{{display:inline-block;background:var(--c-lime);font-family:var(--font-heading);
  font-size:14px;font-weight:700;padding:4px 14px;border-radius:3px;margin-bottom:12px;color:var(--c-text)}}
.report-section h2{{font-family:var(--font-heading);font-size:24px;font-weight:700;
  color:var(--c-text);margin-bottom:18px;border:none;padding:0}}
.report-section h3{{font-family:var(--font-heading);font-size:15px;font-weight:700;
  color:var(--c-text);margin:24px 0 10px;letter-spacing:.5px}}
.report-section h4{{font-family:var(--font-heading);font-size:13px;font-weight:600;
  color:var(--c-text-muted);margin:18px 0 8px}}

/* ── Metric cards ───────────────────────────────────────────────────────── */
.metric-grid{{display:flex;flex-wrap:wrap;gap:12px;margin:18px 0}}
.metric{{background:var(--c-bg);border:1.5px solid var(--c-border);border-radius:4px;
  padding:16px 20px;min-width:155px;flex:1}}
.metric-label{{font-family:var(--font-body);font-size:10px;font-weight:600;
  color:var(--c-text-muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}}
.metric-value{{font-family:var(--font-heading);font-size:22px;font-weight:800;color:var(--c-text)}}
.metric-success{{border-left:4px solid var(--c-mint)}}
.metric-success .metric-value{{color:#0e9a6d}}
.metric-warning{{border-left:4px solid var(--c-yellow)}}
.metric-warning .metric-value{{color:#b8860b}}
.metric-error{{border-left:4px solid #f87171}}
.metric-error .metric-value{{color:#c0392b}}

/* ── Tables ─────────────────────────────────────────────────────────────── */
.table-container{{overflow-x:auto;margin:18px 0}}
.data-table{{width:100%;border-collapse:collapse;font-family:var(--font-body);font-size:12px}}
.data-table th,.data-table td{{padding:10px 14px;text-align:left;border-bottom:1px solid var(--c-border-light)}}
.data-table th{{background:var(--c-purple);color:var(--c-text);font-weight:700;
  font-size:11px;text-transform:uppercase;letter-spacing:.5px;position:sticky;top:0}}
.data-table tbody tr:nth-child(even){{background:var(--c-surface)}}
.data-table tbody tr:hover{{background:#f0eeff}}
.data-table.compact{{font-size:11px}}
.data-table.compact th,.data-table.compact td{{padding:7px 10px}}
.example-cell{{font-size:11px;color:var(--c-text-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}

/* ── Badges ─────────────────────────────────────────────────────────────── */
.badge{{display:inline-block;padding:3px 12px;border-radius:3px;
  font-family:var(--font-body);font-size:10px;font-weight:700;letter-spacing:.5px;text-transform:uppercase}}
.badge-success{{background:var(--c-mint);color:#064e3b}}
.badge-warning{{background:var(--c-yellow);color:#78350f}}
.badge-error{{background:var(--c-pink);color:#831843}}
.badge-neutral{{background:var(--c-border-light);color:var(--c-text-muted)}}

/* ── Percentage bars ────────────────────────────────────────────────────── */
.pct-bar-wrap{{position:relative;background:var(--c-surface);border:1px solid var(--c-border-light);
  border-radius:3px;height:20px;min-width:120px}}
.pct-bar{{position:absolute;top:0;left:0;height:100%;border-radius:2px}}
.pct-bar-success{{background:var(--c-mint);opacity:.55}}
.pct-bar-warning{{background:var(--c-yellow);opacity:.65}}
.pct-bar-error{{background:var(--c-pink);opacity:.65}}
.pct-bar-label{{position:relative;z-index:1;font-size:10px;font-weight:600;
  line-height:20px;padding-left:8px;color:var(--c-text)}}

/* ── KPI cards ──────────────────────────────────────────────────────────── */
.kpi-card{{background:var(--c-bg);border:1.5px solid var(--c-border);border-radius:4px;
  padding:18px 22px;margin:14px 0;transition:border-color .15s}}
.kpi-card:hover{{border-color:var(--c-purple)}}
.kpi-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}}
.kpi-title{{font-family:var(--font-heading);font-size:14px;font-weight:700;color:var(--c-text)}}
.kpi-category{{font-family:var(--font-body);font-size:10px;font-weight:600;
  color:var(--c-text-muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px}}
.kpi-readiness{{font-size:11px;margin-top:10px;padding-top:10px;border-top:1px solid var(--c-border-light)}}
.kpi-found{{color:#0e9a6d;margin-right:16px}}
.kpi-missing{{color:#c0392b}}

/* ── Insight & rec cards ────────────────────────────────────────────────── */
.insight-card{{background:var(--c-bg);border:1.5px solid var(--c-border);border-radius:4px;
  padding:16px 20px;margin:12px 0}}
.insight-card:hover{{border-color:var(--c-purple)}}
.rec-card{{background:var(--c-bg);border:1.5px solid var(--c-border);border-left:4px solid var(--c-purple);
  border-radius:4px;padding:16px 20px;margin:12px 0}}
.rec-header{{margin-bottom:6px}}
.insight-category{{margin:18px 0}}
.cat-desc{{color:var(--c-text-muted);font-size:12px;margin-bottom:8px}}
.cat-detail{{margin:14px 0}}

/* ── Readiness score ────────────────────────────────────────────────────── */
.readiness-hero{{text-align:center;padding:32px 0;background:var(--c-surface);border:1.5px solid var(--c-border);
  border-radius:6px;margin:18px 0}}
.readiness-score-big{{font-family:var(--font-heading);font-size:72px;font-weight:800}}
.readiness-score-big.green{{color:#0e9a6d}}
.readiness-score-big.yellow{{color:#b8860b}}
.readiness-score-big.red{{color:#c0392b}}
.readiness-max{{font-size:.3em;color:var(--c-stroke);font-weight:600}}
.readiness-label{{margin-top:10px}}
.readiness-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px;margin:18px 0}}
.readiness-component{{background:var(--c-bg);border:1.5px solid var(--c-border);border-radius:4px;padding:18px}}
.rc-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}}
.rc-label{{font-family:var(--font-heading);font-size:12px;font-weight:700;color:var(--c-text)}}
.rc-deductions,.rc-boosts{{list-style:none;font-size:11px;padding:0;margin:4px 0}}
.rc-minus{{color:#c0392b;margin-bottom:2px}}
.rc-plus{{color:#0e9a6d;margin-bottom:2px}}

/* ── Appendix ───────────────────────────────────────────────────────────── */
.appendix-col{{background:var(--c-surface);border:1px solid var(--c-border-light);
  border-radius:4px;padding:14px 18px;margin:14px 0}}
.appendix-col h4{{margin-top:0;font-family:var(--font-heading);font-size:13px}}
details{{margin:8px 0}}
details summary{{cursor:pointer;font-family:var(--font-heading);font-size:12px;
  font-weight:600;color:var(--c-purple)}}
details summary:hover{{text-decoration:underline}}

/* ── Narrative ──────────────────────────────────────────────────────────── */
.exec-narrative{{background:var(--c-pale-yellow);border-left:4px solid var(--c-yellow);
  padding:18px 22px;margin:18px 0;border-radius:0 4px 4px 0;font-size:12px;line-height:1.8}}
.no-issues{{color:#0e9a6d;font-weight:600;font-family:var(--font-heading);font-size:13px}}

/* ── Callout pair (areas for improvement style) ─────────────────────────── */
.callout-pair{{display:flex;gap:14px;margin:18px 0}}
.callout-pair .callout{{flex:1;padding:22px 24px;border-radius:4px;font-size:12px;line-height:1.7}}
.callout-mint{{background:var(--c-mint)}}
.callout-pink{{background:var(--c-pink)}}
.callout-purple{{background:var(--c-purple)}}
.callout-yellow{{background:var(--c-yellow)}}

/* ── Footer ─────────────────────────────────────────────────────────────── */
.report-footer{{padding:28px 44px;text-align:center;color:var(--c-text-muted);
  font-size:11px;border-top:3px solid var(--c-text)}}
.report-footer p{{margin:3px 0}}

/* ── Charts ─────────────────────────────────────────────────────────────── */
.chart-container{{position:relative;margin:18px 0}}
.chart-container canvas{{max-height:320px;width:100%!important}}
.chart-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:18px 0}}
.chart-item{{background:var(--c-surface);border:1px solid var(--c-border-light);border-radius:4px;padding:14px}}
.chart-item h4{{font-family:var(--font-heading);font-size:12px;font-weight:600;color:var(--c-text-muted);margin-bottom:8px}}

/* ── Info tooltips ────────────────────────────────────────────────────────── */
.info-icon{{display:inline-flex;align-items:center;justify-content:center;width:15px;height:15px;border-radius:50%;background:var(--c-border);color:var(--c-text-muted);font-size:9px;font-weight:700;font-style:normal;cursor:help;position:relative;vertical-align:middle;margin-left:4px;flex-shrink:0}}
.info-icon:hover{{background:var(--c-purple);color:var(--c-text)}}
.info-icon .info-tip{{display:none;position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:var(--c-text);color:#fff;border-radius:6px;padding:8px 12px;font-size:10px;font-weight:400;font-style:normal;line-height:1.5;width:210px;white-space:normal;z-index:100;pointer-events:none;box-shadow:0 4px 12px rgba(0,0,0,.15)}}
.info-icon .info-tip::after{{content:'';position:absolute;top:100%;left:50%;transform:translateX(-50%);border:5px solid transparent;border-top-color:var(--c-text)}}
.info-icon:hover .info-tip{{display:block}}

/* ── Navigation bar ─────────────────────────────────────────────────────── */
.report-nav{{display:flex;justify-content:space-between;align-items:center;padding:12px 44px;background:var(--c-bg);border-bottom:1px solid var(--c-border);position:sticky;top:0;z-index:50}}
.nav-link{{font-family:var(--font-heading);font-size:12px;font-weight:600;color:var(--c-purple);text-decoration:none;transition:color .15s}}
.nav-link:hover{{color:var(--c-text);text-decoration:underline}}
.nav-actions{{display:flex;gap:8px;align-items:center}}
.nav-btn{{display:inline-flex;align-items:center;gap:5px;padding:7px 14px;border-radius:4px;font-family:var(--font-body);font-size:11px;font-weight:600;text-decoration:none;transition:all .15s;border:1px solid var(--c-border)}}
.nav-btn-download{{background:var(--c-yellow);color:var(--c-text);border-color:var(--c-yellow)}}
.nav-btn-download:hover{{background:var(--c-lime);border-color:var(--c-lime)}}
.nav-btn-json{{background:var(--c-bg);color:var(--c-text-muted)}}
.nav-btn-json:hover{{background:var(--c-surface);border-color:var(--c-purple);color:var(--c-text)}}
@media print{{.report-nav{{display:none}}}}

/* ── Print ──────────────────────────────────────────────────────────────── */
@media print{{
  body{{background:#fff;padding:0}}
  .page-wrap{{box-shadow:none;border:none}}
  .report-section{{break-inside:avoid}}
  .cover-hero{{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
  .section-num,.badge,.data-table th{{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
}}
</style>
</head>
<body>
<div class="page-wrap">
{body}
<footer class="report-footer">
  <p>Generated by Data Onboarding System &middot; {gen_at[:10]}</p>
  <p>This report contains automated analysis. Findings should be reviewed by domain experts before production use.</p>
</footer>
</div>
{chart_embed}
</body>
</html>"""
