"""PII (Personally Identifiable Information) detection."""

from typing import Dict, Any, List, Set
import pandas as pd
import re
from loguru import logger


class PIIScanner:
    """Conservative PII detection based on column names and patterns."""
    
    # PII patterns in column names
    PII_KEYWORDS = {
        "high_sensitivity": [
            "ssn", "social_security", "tax_id", "passport", "driver_license",
            "credit_card", "card_number", "cvv", "account_number", "routing",
            "password", "secret", "token", "api_key"
        ],
        "medium_sensitivity": [
            "email", "phone", "mobile", "telephone", "address", "street",
            "zip", "postal", "dob", "birth", "age", "salary", "income",
            "firstname", "lastname", "first_name", "last_name", "full_name"
        ],
        "low_sensitivity": [
            "name", "user", "customer", "client", "gender", "sex",
            "city", "state", "country", "ip_address"
        ]
    }
    
    def __init__(self):
        self.patterns = self._compile_patterns()
    
    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """Compile regex patterns for PII detection."""
        return {
            "email": re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            "phone": re.compile(r'^\+?1?\d{10,15}$|^\(\d{3}\)\s?\d{3}-?\d{4}$'),
            "ssn": re.compile(r'^\d{3}-?\d{2}-?\d{4}$'),
            "credit_card": re.compile(r'^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$'),
            "ip_address": re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
        }
    
    def scan_table(
        self,
        table: str,
        columns: List[Dict[str, Any]],
        sample_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Scan table for PII columns.
        
        Args:
            table: Table name
            columns: Column metadata from schema
            sample_data: Sample data for pattern matching
            
        Returns:
            PII scan report with flagged columns
        """
        logger.info(f"Scanning {table} for PII")
        
        pii_columns = []
        
        for col_meta in columns:
            col_name = col_meta["column_name"]
            
            # Check column name against keywords
            sensitivity = self._check_column_name(col_name)
            
            # If column is in sample data, check patterns
            if col_name in sample_data.columns and sensitivity:
                pattern_match = self._check_patterns(sample_data[col_name])
                
                pii_columns.append({
                    "column": col_name,
                    "sensitivity": sensitivity,
                    "detection_method": "column_name",
                    "pattern_match": pattern_match,
                    "recommendation": self._get_recommendation(sensitivity)
                })
            
            elif sensitivity:
                pii_columns.append({
                    "column": col_name,
                    "sensitivity": sensitivity,
                    "detection_method": "column_name",
                    "recommendation": self._get_recommendation(sensitivity)
                })
        
        # Calculate PII risk score
        risk_score = self._calculate_risk_score(pii_columns)
        
        return {
            "table_name": table,
            "has_pii": len(pii_columns) > 0,
            "pii_column_count": len(pii_columns),
            "risk_score": risk_score,
            "pii_columns": pii_columns,
            "compliance_notes": self._get_compliance_notes(pii_columns)
        }
    
    def _check_column_name(self, col_name: str) -> str:
        """Check if column name matches PII keywords."""
        col_lower = col_name.lower()
        
        # Check high sensitivity first
        for keyword in self.PII_KEYWORDS["high_sensitivity"]:
            if keyword in col_lower:
                return "high"
        
        # Then medium
        for keyword in self.PII_KEYWORDS["medium_sensitivity"]:
            if keyword in col_lower:
                return "medium"
        
        # Then low
        for keyword in self.PII_KEYWORDS["low_sensitivity"]:
            if keyword in col_lower:
                return "low"
        
        return None
    
    def _check_patterns(self, data: pd.Series) -> Dict[str, bool]:
        """Check if data matches PII patterns (sample only, no value extraction)."""
        sample = data.dropna().astype(str).head(100)
        
        if len(sample) == 0:
            return {}
        
        matches = {}
        for pattern_name, pattern in self.patterns.items():
            # Check if any values match the pattern
            match_count = sample.apply(lambda x: bool(pattern.match(str(x)))).sum()
            # Cast explicitly to Python bool for JSON serialization.
            matches[pattern_name] = bool(match_count > len(sample) * 0.5)  # >50% match
        
        return {k: v for k, v in matches.items() if v}
    
    def _calculate_risk_score(self, pii_columns: List[Dict[str, Any]]) -> str:
        """Calculate overall PII risk level."""
        if not pii_columns:
            return "none"
        
        high_count = sum(1 for col in pii_columns if col["sensitivity"] == "high")
        medium_count = sum(1 for col in pii_columns if col["sensitivity"] == "medium")
        
        if high_count > 0:
            return "high"
        elif medium_count > 3:
            return "high"
        elif medium_count > 0:
            return "medium"
        else:
            return "low"
    
    def _get_recommendation(self, sensitivity: str) -> str:
        """Get handling recommendation based on sensitivity."""
        recommendations = {
            "high": "CRITICAL: Encrypt at rest, mask in reports, restrict access, implement audit logging",
            "medium": "Important: Consider encryption, implement access controls, log access",
            "low": "Standard: Apply basic access controls, consider anonymization for analytics"
        }
        return recommendations.get(sensitivity, "Review data handling practices")
    
    def _get_compliance_notes(self, pii_columns: List[Dict[str, Any]]) -> List[str]:
        """Generate compliance notes based on detected PII."""
        notes = []
        
        if not pii_columns:
            notes.append("No PII detected - standard data protection practices apply")
            return notes
        
        high_pii = [c for c in pii_columns if c["sensitivity"] == "high"]
        if high_pii:
            notes.append("⚠️  GDPR Article 9: Special category data detected - requires explicit consent")
            notes.append("⚠️  Implement data minimization and purpose limitation")
            notes.append("⚠️  Consider pseudonymization or anonymization for analytics")
        
        if len(pii_columns) > 5:
            notes.append("Multiple PII fields detected - conduct Data Protection Impact Assessment (DPIA)")
        
        notes.append("Ensure 'Right to Erasure' (GDPR Article 17) can be honored")
        notes.append("Implement access controls and audit logging for PII access")
        
        return notes
    
    def generate_pii_report(self, scan_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary PII report across all tables."""
        total_tables = len(scan_results)
        tables_with_pii = sum(1 for r in scan_results if r["has_pii"])
        
        all_pii_columns = []
        for result in scan_results:
            for col in result.get("pii_columns", []):
                all_pii_columns.append({
                    "table": result["table_name"],
                    **col
                })
        
        # Group by sensitivity
        by_sensitivity = {
            "high": [c for c in all_pii_columns if c["sensitivity"] == "high"],
            "medium": [c for c in all_pii_columns if c["sensitivity"] == "medium"],
            "low": [c for c in all_pii_columns if c["sensitivity"] == "low"],
        }
        
        return {
            "summary": {
                "total_tables_scanned": total_tables,
                "tables_with_pii": tables_with_pii,
                "total_pii_columns": len(all_pii_columns),
                "high_sensitivity_count": len(by_sensitivity["high"]),
                "medium_sensitivity_count": len(by_sensitivity["medium"]),
                "low_sensitivity_count": len(by_sensitivity["low"]),
            },
            "by_sensitivity": by_sensitivity,
            "compliance_actions": self._generate_compliance_actions(all_pii_columns)
        }
    
    def _generate_compliance_actions(self, pii_columns: List[Dict[str, Any]]) -> List[str]:
        """Generate actionable compliance recommendations."""
        actions = []
        
        if not pii_columns:
            return ["✅ No PII detected - standard security practices sufficient"]
        
        actions.append("1. Document legal basis for processing (GDPR Article 6)")
        actions.append("2. Update privacy policy to reflect data types collected")
        actions.append("3. Implement data retention and deletion policies")
        actions.append("4. Set up access controls and role-based permissions")
        actions.append("5. Enable audit logging for all PII access")
        
        high_pii = [c for c in pii_columns if c["sensitivity"] == "high"]
        if high_pii:
            actions.append("6. ⚠️  URGENT: Encrypt high-sensitivity fields at rest and in transit")
            actions.append("7. ⚠️  Implement data masking for non-production environments")
        
        return actions
