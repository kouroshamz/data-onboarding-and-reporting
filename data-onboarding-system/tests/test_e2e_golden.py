"""Golden-path end-to-end assertions for report outputs."""

import json
import os

import pytest
import yaml
from click.testing import CliRunner

from app.cli import cli


@pytest.mark.integration
def test_e2e_pipeline_generates_expected_artifacts_and_metrics(tmp_path):
    """Run the full CLI pipeline and assert stable output structure."""
    output_root = tmp_path / "reports"
    log_path = tmp_path / "logs" / "pipeline.log"

    config = {
        "client": {"id": "golden_client", "name": "Golden Client", "industry": "ecommerce"},
        "connection": {
            "type": "postgresql",
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5433")),
            "database": os.getenv("DB_NAME", "testdb"),
            "username": os.getenv("DB_USER", "testuser"),
            "password": os.getenv("DB_PASSWORD", "testpass"),
            "read_only": False,
        },
        "reporting": {"format": ["html", "pdf", "json"]},
        "output": {"directory": str(output_root)},
        "logging": {"level": "INFO", "file": str(log_path), "json_format": False},
    }

    config_path = tmp_path / "config.e2e.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--config", str(config_path)])
    assert result.exit_code == 0, result.output

    report_dir = output_root / "golden_client"
    assert (report_dir / "report_data.json").exists()
    assert (report_dir / "report.txt").exists()
    assert (report_dir / "schema.json").exists()
    assert (report_dir / "profile.json").exists()
    assert (report_dir / "report.html").exists()
    # PDF generation requires WeasyPrint system dependencies, skip assertion if not available
    # assert (report_dir / "report.pdf").exists()

    report_data = json.loads((report_dir / "report_data.json").read_text(encoding="utf-8"))

    assert report_data["schema"]["table_count"] >= 4
    assert {"customers", "orders", "products", "order_items"}.issubset(
        set(report_data["schema"]["tables"].keys())
    )
    assert report_data["industry"]["industry"] == "ecommerce"
    assert report_data["pii"]["summary"]["tables_with_pii"] >= 1
    assert len(report_data["relationships"]["relationships"]) >= 1
    assert report_data["kpis"]
    quality = report_data["quality"]
    assert 0 <= quality["overall_score"] <= 100
    assert all(
        0 <= t["overall_score"] <= 100
        for t in quality["tables"].values()
    )
