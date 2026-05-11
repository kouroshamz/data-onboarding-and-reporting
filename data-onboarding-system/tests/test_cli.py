"""Test CLI functionality."""

import pytest
import yaml
from click.testing import CliRunner
from app.cli import cli


def test_cli_help():
    """Test CLI help command."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Data Onboarding System' in result.output


def test_cli_run_without_config():
    """Test CLI run without config fails gracefully."""
    runner = CliRunner()
    result = runner.invoke(cli, ['run'])
    assert result.exit_code != 0
    assert 'config' in result.output.lower()


def test_cli_rejects_unsupported_connection_type(tmp_path):
    """CLI should fail fast for unsupported DB types."""
    cfg = {
        "client": {"id": "x", "name": "Unsupported DB"},
        "connection": {
            "type": "mssql",
            "host": "localhost",
            "port": 1433,
            "database": "db",
            "username": "u",
            "password": "p",
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--config", str(config_path)])

    assert result.exit_code != 0
    assert isinstance(result.exception, NotImplementedError)
    assert "not supported" in str(result.exception)
