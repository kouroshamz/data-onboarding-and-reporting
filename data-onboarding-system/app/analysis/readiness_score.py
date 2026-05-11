"""Data Readiness Score — Section 10 of the onboarding report.

5-component scoring system (each 0–20 points, total 0–100):

  1. Structure Quality  — duplicates, constant cols, dtype consistency
  2. Null Health         — overall null rate, fully null columns, null clustering
  3. Type Consistency    — types mismatches, dates-as-text, booleans-as-string
  4. Compliance Risk     — PII/GDPR findings, masking needs
  5. Business Value      — KPI-capable columns, business classifications, relationships

Final score → Green (≥ 70) / Yellow (40–69) / Red (< 40)
"""

from __future__ import annotations

from typing import Any, Dict, List


def compute_readiness_score(
    profile_results: Dict[str, Any],
    structural_overview: Dict[str, Any],
    quality_results: Dict[str, Any],
    pii_results: Dict[str, Any],
    gdpr_assessment: Dict[str, Any],
    kpi_results: Dict[str, Any],
    column_classifications: Dict[str, Any],
    relationship_data: Dict[str, Any] | None = None,
    llm_type_findings: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Compute the 5-component Data Readiness Score.

    Each component is 0–20 points. Total is 0–100.
    """
    c1 = _score_structure_quality(profile_results, structural_overview)
    c2 = _score_null_health(profile_results)
    c3 = _score_type_consistency(profile_results, llm_type_findings)
    c4 = _score_compliance_risk(pii_results, gdpr_assessment)
    c5 = _score_business_value(kpi_results, column_classifications, relationship_data)

    total = c1["score"] + c2["score"] + c3["score"] + c4["score"] + c5["score"]

    if total >= 70:
        grade = "green"
        label = "Ready for onboarding"
    elif total >= 40:
        grade = "yellow"
        label = "Needs remediation"
    else:
        grade = "red"
        label = "Significant issues"

    return {
        "total_score": total,
        "grade": grade,
        "label": label,
        "components": {
            "structure_quality": c1,
            "null_health": c2,
            "type_consistency": c3,
            "compliance_risk": c4,
            "business_value": c5,
        },
    }


# ── Component 1: Structure Quality (0–20) ────────────────────────────────────

def _score_structure_quality(
    profile_results: Dict[str, Any],
    structural_overview: Dict[str, Any],
) -> Dict[str, Any]:
    score = 20.0
    deductions: List[str] = []

    # Duplicates
    dup_pct = structural_overview.get("duplicate_pct", 0)
    if dup_pct > 20:
        score -= 6
        deductions.append(f"High duplicate rate ({dup_pct:.1f}%)")
    elif dup_pct > 5:
        score -= 3
        deductions.append(f"Moderate duplicate rate ({dup_pct:.1f}%)")
    elif dup_pct > 0:
        score -= 1
        deductions.append(f"Some duplicates ({dup_pct:.1f}%)")

    # Constant columns
    n_constant = len(structural_overview.get("constant_columns", []))
    if n_constant > 3:
        score -= 4
        deductions.append(f"{n_constant} constant columns (zero information)")
    elif n_constant > 0:
        score -= 2
        deductions.append(f"{n_constant} constant column(s)")

    # Fully null columns
    n_fully_null = structural_overview.get("columns_fully_null", 0)
    if n_fully_null > 2:
        score -= 4
        deductions.append(f"{n_fully_null} fully null columns")
    elif n_fully_null > 0:
        score -= 2
        deductions.append(f"{n_fully_null} fully null column(s)")

    # Suspicious ID columns (not inherently bad, small deduction if many)
    n_suspicious = len(structural_overview.get("suspicious_id_columns", []))
    if n_suspicious > 5:
        score -= 2
        deductions.append(f"{n_suspicious} potential ID columns — review for PII")

    score = max(score, 0)

    return {
        "score": round(score),
        "max": 20,
        "label": "Structure Quality",
        "deductions": deductions,
    }


# ── Component 2: Null Health (0–20) ──────────────────────────────────────────

def _score_null_health(profile_results: Dict[str, Any]) -> Dict[str, Any]:
    score = 20.0
    deductions: List[str] = []

    all_null_pcts = []
    for table_name, profile in profile_results.items():
        for col_name, cp in profile.get("columns", {}).items():
            all_null_pcts.append(cp.get("null_percent", 0))

    if not all_null_pcts:
        return {"score": 20, "max": 20, "label": "Null Health", "deductions": []}

    avg_null = sum(all_null_pcts) / len(all_null_pcts)
    max_null = max(all_null_pcts)
    high_null_cols = sum(1 for p in all_null_pcts if p > 50)

    # Average null rate
    if avg_null > 30:
        score -= 8
        deductions.append(f"Average null rate is {avg_null:.1f}%")
    elif avg_null > 15:
        score -= 5
        deductions.append(f"Average null rate is {avg_null:.1f}%")
    elif avg_null > 5:
        score -= 2
        deductions.append(f"Average null rate is {avg_null:.1f}%")

    # Columns over 50% null
    if high_null_cols > 5:
        score -= 6
        deductions.append(f"{high_null_cols} columns have > 50% nulls")
    elif high_null_cols > 2:
        score -= 3
        deductions.append(f"{high_null_cols} columns have > 50% nulls")
    elif high_null_cols > 0:
        score -= 1
        deductions.append(f"{high_null_cols} column(s) have > 50% nulls")

    # Max null rate
    if max_null >= 99:
        score -= 3
        deductions.append(f"At least one column is {max_null:.0f}% null")

    score = max(score, 0)

    return {
        "score": round(score),
        "max": 20,
        "label": "Null Health",
        "deductions": deductions,
    }


# ── Component 3: Type Consistency (0–20) ─────────────────────────────────────

def _score_type_consistency(
    profile_results: Dict[str, Any],
    llm_type_findings: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    score = 20.0
    deductions: List[str] = []

    # LLM type findings (from L1)
    if llm_type_findings:
        n_findings = len(llm_type_findings)
        critical = sum(1 for f in llm_type_findings if f.get("severity") == "critical")
        warnings = sum(1 for f in llm_type_findings if f.get("severity") == "warning")

        if critical > 0:
            score -= min(critical * 3, 10)
            deductions.append(f"{critical} critical type mismatch(es) found by LLM")
        if warnings > 0:
            score -= min(warnings * 1.5, 6)
            deductions.append(f"{warnings} type warning(s) found by LLM")
    else:
        # Heuristic without LLM: count string columns that look numeric
        mixed_count = 0
        for table_name, profile in profile_results.items():
            for col_name, cp in profile.get("columns", {}).items():
                type_cat = cp.get("type_category", "").lower()
                if type_cat in ("object", "string"):
                    # If unique count is very low and name suggests boolean
                    unique = cp.get("unique_count", 0)
                    if unique == 2:
                        top = cp.get("top_values", [])
                        if top:
                            vals = {str(v.get("value", "")).lower() for v in top}
                            if vals & {"true", "false", "yes", "no", "1", "0", "y", "n"}:
                                mixed_count += 1
        if mixed_count > 3:
            score -= 4
            deductions.append(f"{mixed_count} columns may have incorrect types")
        elif mixed_count > 0:
            score -= 2
            deductions.append(f"{mixed_count} column(s) may have incorrect types")

    score = max(score, 0)

    return {
        "score": round(score),
        "max": 20,
        "label": "Type Consistency",
        "deductions": deductions,
    }


# ── Component 4: Compliance Risk (0–20) ──────────────────────────────────────

def _score_compliance_risk(
    pii_results: Dict[str, Any],
    gdpr_assessment: Dict[str, Any],
) -> Dict[str, Any]:
    """Higher score = lower risk (good). Deduct for PII/GDPR issues."""
    score = 20.0
    deductions: List[str] = []

    overall_risk = gdpr_assessment.get("overall_risk", "low")
    n_pii = gdpr_assessment.get("total_pii_findings", 0)
    has_special = gdpr_assessment.get("has_special_category_data", False)

    if has_special:
        score -= 8
        deductions.append("Special category data (Art. 9) detected — DPIA required")
    elif overall_risk == "critical":
        score -= 6
        deductions.append("Critical PII detected (national ID / financial)")
    elif overall_risk == "high":
        score -= 4
        deductions.append("High-risk personal data detected")
    elif overall_risk == "medium":
        score -= 2
        deductions.append("Medium-risk personal data detected")

    # Volume of PII columns
    if n_pii > 10:
        score -= 4
        deductions.append(f"{n_pii} PII columns — data minimisation review needed")
    elif n_pii > 5:
        score -= 2
        deductions.append(f"{n_pii} PII columns detected")
    elif n_pii > 0:
        score -= 1
        deductions.append(f"{n_pii} PII column(s) detected")

    # GDPR categories count
    n_cats = len(gdpr_assessment.get("gdpr_categories", {}))
    if n_cats > 4:
        score -= 3
        deductions.append(f"PII spans {n_cats} GDPR categories")

    score = max(score, 0)

    return {
        "score": round(score),
        "max": 20,
        "label": "Compliance Risk",
        "deductions": deductions,
    }


# ── Component 5: Business Value Potential (0–20) ─────────────────────────────

def _score_business_value(
    kpi_results: Dict[str, Any],
    column_classifications: Dict[str, Any],
    relationship_data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Higher score = more business value potential."""
    score = 5.0  # start midway — build up
    boosts: List[str] = []

    # KPIs detected
    kpi_list = kpi_results if isinstance(kpi_results, list) else kpi_results.get("kpis", [])
    n_kpis = len(kpi_list) if isinstance(kpi_list, list) else 0
    if n_kpis >= 5:
        score += 5
        boosts.append(f"{n_kpis} KPIs identified")
    elif n_kpis >= 2:
        score += 3
        boosts.append(f"{n_kpis} KPIs identified")
    elif n_kpis > 0:
        score += 1
        boosts.append(f"{n_kpis} KPI(s) identified")

    # Business column classifications
    summary = column_classifications.get("summary", {})
    cats = summary.get("categories_found", [])
    n_classified = summary.get("total_classified", 0)

    if "revenue" in cats:
        score += 3
        boosts.append("Revenue columns detected")
    if "timestamp" in cats:
        score += 2
        boosts.append("Timestamp columns available (time-series possible)")
    if "customer_identifier" in cats:
        score += 2
        boosts.append("Customer identifiers found")
    if len(cats) >= 4:
        score += 2
        boosts.append(f"Rich schema — {len(cats)} business categories")

    # Relationships
    if relationship_data:
        rels = relationship_data if isinstance(relationship_data, list) else relationship_data.get("relationships", [])
        n_rels = len(rels) if isinstance(rels, list) else 0
        if n_rels > 0:
            score += 1
            boosts.append(f"{n_rels} table relationship(s) detected")

    score = min(score, 20)

    return {
        "score": round(score),
        "max": 20,
        "label": "Business Value Potential",
        "boosts": boosts,
    }
