"""GDPR & Sensitive Data Assessment — Section 5 of the onboarding report.

Extends PII scan results with GDPR-specific assessment:
  - Classification of PII types (email, phone, IP, name, postcode, national ID, financial, health)
  - GDPR readiness signals: lawful basis needed, data minimisation, retention, processor/controller
  - Risk severity per finding
"""

from __future__ import annotations

from typing import Any, Dict, List


# Mapping from PII detection types to GDPR categories
PII_TO_GDPR_CATEGORY = {
    "email": "contact_data",
    "phone": "contact_data",
    "ip_address": "online_identifier",
    "ip": "online_identifier",
    "name": "personal_identifier",
    "first_name": "personal_identifier",
    "last_name": "personal_identifier",
    "full_name": "personal_identifier",
    "postcode": "location_data",
    "postal_code": "location_data",
    "zip_code": "location_data",
    "address": "location_data",
    "ssn": "national_id",
    "national_id": "national_id",
    "passport": "national_id",
    "credit_card": "financial_data",
    "iban": "financial_data",
    "bank_account": "financial_data",
    "health": "special_category",
    "medical": "special_category",
    "diagnosis": "special_category",
    "religion": "special_category",
    "ethnicity": "special_category",
    "political": "special_category",
    "biometric": "special_category",
    "date_of_birth": "personal_identifier",
    "dob": "personal_identifier",
    "age": "personal_identifier",
    "gender": "personal_identifier",
    "sex": "personal_identifier",
}

GDPR_CATEGORY_LABELS = {
    "contact_data": "Contact Data",
    "online_identifier": "Online Identifiers",
    "personal_identifier": "Personal Identifiers",
    "location_data": "Location Data",
    "national_id": "National / Government IDs",
    "financial_data": "Financial Data",
    "special_category": "Special Category Data (Art. 9)",
}

# Risk levels per GDPR category
GDPR_RISK_MAP = {
    "contact_data": "medium",
    "online_identifier": "medium",
    "personal_identifier": "high",
    "location_data": "medium",
    "national_id": "critical",
    "financial_data": "critical",
    "special_category": "critical",
}


def compute_gdpr_assessment(
    pii_results: Dict[str, Any],
    profile_results: Dict[str, Any],
) -> Dict[str, Any]:
    """Build a GDPR assessment from PII scan results.

    pii_results is keyed by table name, each value has 'findings' list.
    Each finding has: column, pii_type, confidence, sample_matches.
    """

    all_findings: List[Dict[str, Any]] = []
    categories_found: Dict[str, List[str]] = {}
    highest_risk = "low"
    risk_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}

    for table_name, table_pii in pii_results.items():
        findings = table_pii if isinstance(table_pii, list) else table_pii.get("findings", [])

        for f in findings:
            pii_type = f.get("pii_type", f.get("type", "unknown")).lower()
            column = f.get("column", "unknown")
            confidence = f.get("confidence", f.get("score", 0))

            # Map to GDPR category
            gdpr_cat = PII_TO_GDPR_CATEGORY.get(pii_type, _guess_category(pii_type, column))

            risk = GDPR_RISK_MAP.get(gdpr_cat, "medium")
            if risk_order.get(risk, 0) > risk_order.get(highest_risk, 0):
                highest_risk = risk

            qualified_col = f"{table_name}.{column}"
            categories_found.setdefault(gdpr_cat, []).append(qualified_col)

            all_findings.append({
                "table": table_name,
                "column": column,
                "pii_type": pii_type,
                "gdpr_category": gdpr_cat,
                "gdpr_category_label": GDPR_CATEGORY_LABELS.get(gdpr_cat, gdpr_cat.replace("_", " ").title()),
                "risk": risk,
                "confidence": confidence,
            })

    # GDPR readiness recommendations
    recommendations = _build_gdpr_recommendations(categories_found, all_findings)

    # Summary
    has_special = "special_category" in categories_found
    has_national = "national_id" in categories_found
    has_financial = "financial_data" in categories_found

    return {
        "total_pii_findings": len(all_findings),
        "overall_risk": highest_risk,
        "has_special_category_data": has_special,
        "gdpr_categories": {
            cat: {
                "label": GDPR_CATEGORY_LABELS.get(cat, cat.replace("_", " ").title()),
                "columns": cols,
                "count": len(cols),
                "risk": GDPR_RISK_MAP.get(cat, "medium"),
            }
            for cat, cols in categories_found.items()
        },
        "findings": all_findings,
        "recommendations": recommendations,
    }


def _guess_category(pii_type: str, column: str) -> str:
    """Best-effort mapping for unrecognized PII types."""
    combined = f"{pii_type} {column}".lower()

    if any(t in combined for t in ["email", "mail"]):
        return "contact_data"
    if any(t in combined for t in ["phone", "tel", "mobile"]):
        return "contact_data"
    if any(t in combined for t in ["ip", "mac_addr"]):
        return "online_identifier"
    if any(t in combined for t in ["name", "person", "user"]):
        return "personal_identifier"
    if any(t in combined for t in ["address", "city", "zip", "post", "country", "lat", "lon", "geo"]):
        return "location_data"
    if any(t in combined for t in ["ssn", "passport", "national", "license", "licence"]):
        return "national_id"
    if any(t in combined for t in ["card", "iban", "bank", "account", "payment"]):
        return "financial_data"
    if any(t in combined for t in ["health", "medical", "diagnosis", "religion", "ethnic", "biometric"]):
        return "special_category"

    return "personal_identifier"  # conservative default


def _build_gdpr_recommendations(
    categories_found: Dict[str, List[str]],
    all_findings: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Generate actionable GDPR recommendations."""
    recs: List[Dict[str, str]] = []

    if not all_findings:
        recs.append({
            "area": "General",
            "priority": "low",
            "recommendation": "No personal data detected. Confirm with a domain expert before classifying as anonymous.",
        })
        return recs

    # Lawful basis
    recs.append({
        "area": "Lawful Basis (Art. 6)",
        "priority": "high",
        "recommendation": (
            "Identify and document the lawful basis for processing each category of personal data. "
            f"Categories detected: {', '.join(GDPR_CATEGORY_LABELS.get(c, c) for c in categories_found)}."
        ),
    })

    # Data minimisation
    total_pii_cols = sum(len(v) for v in categories_found.values())
    recs.append({
        "area": "Data Minimisation (Art. 5(1)(c))",
        "priority": "medium",
        "recommendation": (
            f"{total_pii_cols} column(s) contain personal data. "
            "Review whether all are strictly necessary for the stated processing purpose."
        ),
    })

    # Special category
    if "special_category" in categories_found:
        recs.append({
            "area": "Special Category Data (Art. 9)",
            "priority": "critical",
            "recommendation": (
                "Special category data detected. Processing requires an explicit exemption under Art. 9(2). "
                "Conduct a Data Protection Impact Assessment (DPIA) before onboarding."
            ),
        })

    # National ID / Financial
    for cat, label, advice in [
        ("national_id", "National IDs", "Apply pseudonymisation or tokenisation before any analytics."),
        ("financial_data", "Financial Data", "Ensure PCI-DSS compliance if credit card data is present. Apply masking."),
    ]:
        if cat in categories_found:
            recs.append({
                "area": label,
                "priority": "critical",
                "recommendation": advice,
            })

    # Retention
    recs.append({
        "area": "Retention Policy (Art. 5(1)(e))",
        "priority": "medium",
        "recommendation": (
            "Define and document a retention schedule for this dataset. "
            "Ensure personal data is not kept longer than necessary for the processing purpose."
        ),
    })

    # Controller / Processor
    recs.append({
        "area": "Controller vs Processor (Art. 26/28)",
        "priority": "medium",
        "recommendation": (
            "Clarify whether the data operator acts as data controller or processor for this dataset. "
            "If processor, ensure a Data Processing Agreement (DPA) is in place."
        ),
    })

    return recs
