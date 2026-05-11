"""Test configuration loading."""

from app.config import Config


def test_config_defaults():
    """Test default configuration values."""
    config_data = {
        "client": {
            "id": "test_001",
            "name": "Test Client"
        },
        "connection": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "test",
            "password": "test"
        }
    }
    
    config = Config(**config_data)
    
    assert config.client.id == "test_001"
    assert config.sampling.enabled == True
    assert config.analysis.schema_discovery == True


def test_connection_env_placeholders_resolve(monkeypatch):
    """Connection fields should resolve ${VAR} placeholders."""
    monkeypatch.setenv("TEST_DB_HOST", "db.internal")
    monkeypatch.setenv("TEST_DB_PORT", "6543")

    config_data = {
        "client": {"id": "test_002", "name": "Env Client"},
        "connection": {
            "type": "postgresql",
            "host": "${TEST_DB_HOST}",
            "port": "${TEST_DB_PORT}",
            "database": "testdb",
            "username": "user",
            "password": "pass",
        },
    }

    config = Config(**config_data)

    assert config.connection.host == "db.internal"
    assert config.connection.port == 6543


def test_connection_env_placeholders_support_default_value():
    """Connection fields should support ${VAR:-default}."""
    config_data = {
        "client": {"id": "test_003", "name": "Default Env Client"},
        "connection": {
            "type": "postgresql",
            "host": "${MISSING_HOST:-localhost}",
            "port": "${MISSING_PORT:-5433}",
            "database": "${MISSING_DB:-testdb}",
            "username": "${MISSING_USER:-testuser}",
            "password": "${MISSING_PASS:-testpass}",
        },
    }

    config = Config(**config_data)

    assert config.connection.host == "localhost"
    assert config.connection.port == 5433
    assert config.connection.database == "testdb"
    assert config.connection.username == "testuser"
    assert config.connection.password == "testpass"
