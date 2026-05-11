"""End-to-end CLI tests — run the full pipeline via Click CliRunner."""

import json
import yaml
from pathlib import Path

import pytest
from click.testing import CliRunner
from app.cli import cli


TITANIC_DIR = Path(__file__).parent / "fixtures" / "datasets" / "01_titanic"


def _make_config(tmp_path, **overrides):
    """Create a minimal CSV config YAML and return its path."""
    cfg = {
        "spec_version": "1.0.0",
        "client": {"id": "e2e_test", "name": "E2E Test"},
        "connection": {
            "type": "csv",
            "host": str(TITANIC_DIR),
        },
        "output": {"directory": str(tmp_path / "output")},
        "logging": {"level": "WARNING", "file": str(tmp_path / "log.log")},
        "pipeline": {"fail_on_partial": False},
    }
    cfg.update(overrides)
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return str(p)


# ---------------------------------------------------------------------------
# Full pipeline run
# ---------------------------------------------------------------------------

class TestE2EFullPipeline:
    """Verify the whole CLI pipeline end-to-end with the Titanic CSV."""

    @pytest.fixture
    def result_dir(self, tmp_path):
        """Run the pipeline once and return the output directory."""
        cfg_path = _make_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", cfg_path], catch_exceptions=False)
        assert result.exit_code == 0, f"CLI failed:\n{result.output}"
        return tmp_path / "output" / "e2e_test"

    def test_pipeline_produces_artifacts(self, result_dir):
        """All contract-mandated JSON deliverables exist."""
        for name in [
            "source_connection_status.json",
            "schema.json",
            "assets_inventory.json",
            "sampling_manifest.json",
            "profile.json",
            "kpi_candidates.json",
            "report_data.json",
        ]:
            assert (result_dir / name).exists(), f"Missing artifact: {name}"

    def test_html_report_generated(self, result_dir):
        html = result_dir / "report.html"
        assert html.exists()
        content = html.read_text()
        assert "DATA ONBOARDING" in content
        assert "E2E Test" in content

    def test_text_report_generated(self, result_dir):
        txt = result_dir / "report.txt"
        assert txt.exists()
        assert "DATA ONBOARDING REPORT" in txt.read_text()

    def test_connection_status_ok(self, result_dir):
        data = json.loads((result_dir / "source_connection_status.json").read_text())
        assert data["ok"] is True
        assert data["source_type"] == "csv"

    def test_schema_has_tables(self, result_dir):
        data = json.loads((result_dir / "schema.json").read_text())
        assert data["table_count"] >= 1
        assert "data" in data["tables"] or len(data["tables"]) >= 1

    def test_profile_has_columns(self, result_dir):
        data = json.loads((result_dir / "profile.json").read_text())
        assert len(data) >= 1
        first_table = next(iter(data.values()))
        assert "columns" in first_table
        assert len(first_table["columns"]) >= 1

    def test_quality_scores_calculated(self, result_dir):
        report = json.loads((result_dir / "report_data.json").read_text())
        quality = report["quality"]
        assert "overall_score" in quality
        assert quality["overall_score"] > 0

    def test_kpi_candidates_generated(self, result_dir):
        data = json.loads((result_dir / "kpi_candidates.json").read_text())
        assert data["total_candidates"] >= 0

    def test_readiness_score_present(self, result_dir):
        report = json.loads((result_dir / "report_data.json").read_text())
        rs = report.get("readiness_score", {})
        assert "total_score" in rs
        assert rs["total_score"] > 0
        assert rs["grade"] in ("green", "amber", "red")


# ---------------------------------------------------------------------------
# Quick command
# ---------------------------------------------------------------------------

class TestQuickCommand:
    """Verify the `quick` shortcut command works."""

    def test_quick_runs_csv(self, tmp_path):
        csv_path = TITANIC_DIR / "data.csv"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "quick", str(csv_path),
            "--output", str(tmp_path / "quick_out"),
        ], catch_exceptions=False)
        assert result.exit_code == 0, f"quick failed:\n{result.output}"
        out_dir = tmp_path / "quick_out" / "data"
        assert (out_dir / "report.html").exists()
        assert (out_dir / "report_data.json").exists()


# ---------------------------------------------------------------------------
# Validate command
# ---------------------------------------------------------------------------

class TestValidateCommand:

    def test_validate_good_config(self, tmp_path):
        cfg_path = _make_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", cfg_path])
        assert result.exit_code == 0
        assert "Configuration valid" in result.output

    def test_validate_test_connection_csv(self, tmp_path):
        cfg_path = _make_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", cfg_path, "--test-connection"])
        assert result.exit_code == 0
        assert "Connection OK" in result.output
        assert "Assets found" in result.output

    def test_validate_bad_config(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("client: {id: x}\n")  # missing required fields
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", str(p)])
        assert result.exit_code != 0

    def test_validate_test_connection_bad_path(self, tmp_path):
        cfg = {
            "spec_version": "1.0.0",
            "client": {"id": "x", "name": "X"},
            "connection": {"type": "csv", "host": "/nonexistent/path"},
            "output": {"directory": str(tmp_path / "out")},
            "logging": {"level": "WARNING", "file": str(tmp_path / "l.log")},
        }
        p = tmp_path / "config.yaml"
        p.write_text(yaml.safe_dump(cfg))
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", str(p), "--test-connection"])
        # Should fail (no such directory) or succeed with 0 assets
        # Connection test for CSV should at least not crash
        assert result.exit_code in (0, 2)


# ---------------------------------------------------------------------------
# Schema command
# ---------------------------------------------------------------------------

class TestSchemaCommand:

    def test_schema_extracts_to_file(self, tmp_path):
        cfg_path = _make_config(tmp_path)
        out_file = str(tmp_path / "schema_out.json")
        runner = CliRunner()
        result = runner.invoke(cli, ["schema", "--config", cfg_path, "-o", out_file])
        assert result.exit_code == 0
        data = json.loads(Path(out_file).read_text())
        assert "table_count" in data
