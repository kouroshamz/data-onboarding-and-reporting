"""Integration tests requiring database."""

import pytest
import os
from pathlib import Path
from app.config import Config
from app.connectors.postgres import PostgreSQLConnector
from app.ingestion.schema_extract import SchemaExtractor
from app.analysis.profiling import DataProfiler
from app.analysis.quality_checks import QualityChecker
from app.analysis.pii_scan import PIIScanner


@pytest.fixture
def test_config():
    """Create test configuration."""
    return Config(
        client={
            "id": "test_client",
            "name": "Test Client",
            "industry": "ecommerce"
        },
        connection={
            "type": "postgresql",
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5433")),
            "database": os.getenv("DB_NAME", "testdb"),
            "username": os.getenv("DB_USER", "testuser"),
            "password": os.getenv("DB_PASSWORD", "testpass"),
            "read_only": False  # Test DB doesn't have read-only user
        }
    )


@pytest.mark.integration
def test_connection(test_config):
    """Test database connection."""
    connector = PostgreSQLConnector(test_config.connection)
    try:
        connector.connect()
        tables = connector.get_table_list()
        assert len(tables) > 0
        assert "customers" in tables
    finally:
        connector.close()


@pytest.mark.integration
def test_schema_extraction(test_config):
    """Test schema extraction."""
    connector = PostgreSQLConnector(test_config.connection)
    try:
        connector.connect()
        extractor = SchemaExtractor(connector)
        schema_data = extractor.extract()
        
        assert schema_data["table_count"] >= 4
        assert "customers" in schema_data["tables"]
        assert "orders" in schema_data["tables"]
        
        customers = schema_data["tables"]["customers"]
        assert customers["row_count"] > 0
        assert len(customers["columns"]) > 0
        assert customers["has_primary_key"] == True
    finally:
        connector.close()


@pytest.mark.integration
def test_profiling(test_config):
    """Test data profiling."""
    connector = PostgreSQLConnector(test_config.connection)
    try:
        connector.connect()
        profiler = DataProfiler(connector, test_config.analysis)
        
        # Get sample data
        sample_data = connector.execute_query("SELECT * FROM customers")
        
        # Profile
        profile = profiler.profile_table("customers", sample_data)
        
        assert "columns" in profile
        assert "email" in profile["columns"]
        assert profile["columns"]["email"]["null_percent"] >= 0
        assert profile["completeness_score"] > 0
    finally:
        connector.close()


@pytest.mark.integration
def test_quality_checks(test_config):
    """Test quality checking."""
    connector = PostgreSQLConnector(test_config.connection)
    try:
        connector.connect()
        profiler = DataProfiler(connector, test_config.analysis)
        quality_checker = QualityChecker(connector, test_config.analysis)
        
        # Get sample and profile
        sample_data = connector.execute_query("SELECT * FROM orders")
        profile = profiler.profile_table("orders", sample_data)
        
        # Run quality checks
        quality = quality_checker.check_table_quality("orders", profile, sample_data)
        
        assert "quality_score" in quality
        assert quality["quality_score"] >= 0
        assert quality["quality_score"] <= 100
        assert len(quality["checks"]) > 0
    finally:
        connector.close()


@pytest.mark.integration
def test_pii_detection(test_config):
    """Test PII detection."""
    connector = PostgreSQLConnector(test_config.connection)
    try:
        connector.connect()
        pii_scanner = PIIScanner()
        
        # Get schema and sample
        columns = connector.get_column_info("customers").to_dict("records")
        sample_data = connector.execute_query("SELECT * FROM customers")
        
        # Scan for PII
        pii_result = pii_scanner.scan_table("customers", columns, sample_data)
        
        assert pii_result["has_pii"] == True  # Should detect email
        assert pii_result["pii_column_count"] > 0
        
        # Check that email was detected
        pii_columns = [col["column"] for col in pii_result["pii_columns"]]
        assert "email" in pii_columns
    finally:
        connector.close()
