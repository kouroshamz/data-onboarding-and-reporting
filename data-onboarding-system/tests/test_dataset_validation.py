"""Parametrized pipeline test – runs 10 real public datasets end-to-end.

For each dataset:
  1. Writes a config YAML pointing the CSV connector at the dataset dir
  2. Invokes the CLI ``run`` command
  3. Asserts: no crash, all deliverable files created, quality score plausible,
     report_data.json has required contract keys
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml
from click.testing import CliRunner

from app.cli import cli

# ---------------------------------------------------------------------------
# Dataset registry
# ---------------------------------------------------------------------------

DATASETS_DIR = Path(__file__).parent / "fixtures" / "datasets"

# (directory_name, human_label, expected_industry_hint, expected_min_tables)
DATASETS = [
    ("01_titanic",             "Titanic passengers",          "auto",       1),
    ("02_diamonds",            "Diamonds (54K rows)",         "auto",       1),
    ("03_penguins",            "Palmer penguins",             "auto",       1),
    ("04_tips",                "Restaurant tips",             "auto",       1),
    ("05_flights",             "Airline flights",             "auto",       1),
    ("06_california_housing",  "California housing",          "auto",       1),
    ("07_breast_cancer",       "Breast cancer Wisconsin",     "auto",       1),
    ("08_earthquakes",         "USGS earthquakes",            "auto",       1),
    ("09_wine_quality",        "Wine quality (UCI)",          "auto",       1),
    ("10_world_indicators",    "World dev indicators",        "auto",       1),
]

# Contract-required keys in report_data.json
REPORT_CONTRACT_KEYS = {
    "schema_version", "run_id", "client", "generated_at",
    "schema", "profiles", "quality", "pii", "relationships", "kpis",
}

# Expected deliverable files
DELIVERABLE_FILES = [
    "source_connection_status.json",
    "assets_inventory.json",
    "schema.json",
    "profile.json",
    "sampling_manifest.json",
    "kpi_candidates.json",
    "report_data.json",
    "report.txt",
    "report.html",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(dataset_dir: Path, output_dir: Path, client_id: str) -> Dict[str, Any]:
    """Build a pipeline config dict for a CSV dataset."""
    return {
        "spec_version": "1.0.0",
        "client": {
            "id": client_id,
            "name": client_id.replace("_", " ").title(),
            "industry": "auto",
        },
        "connection": {
            "type": "csv",
            "host": str(dataset_dir),
        },
        "sampling": {
            "enabled": True,
            "small_table_threshold": 100000,
            "medium_sample_rate": 0.1,
            "large_sample_rate": 0.05,
            "max_sample_size": 10000,
        },
        "analysis": {
            "schema_discovery": True,
            "data_profiling": True,
            "quality_checks": True,
            "pii_detection": True,
            "relationship_inference": True,
            "kpi_suggestions": True,
        },
        "kpi": {
            "auto_detect_industry": True,
            "confidence_threshold": 0.3,
            "generate_sql_examples": True,
            "max_recommendations": 10,
        },
        "reporting": {
            "format": ["html", "json", "txt"],
        },
        "output": {
            "directory": str(output_dir),
        },
        "pipeline": {
            "fail_on_partial": False,
        },
        "logging": {
            "level": "WARNING",
            "file": str(output_dir / "pipeline.log"),
        },
    }


# ---------------------------------------------------------------------------
# Parametrized test
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "dir_name, label, industry, min_tables",
    DATASETS,
    ids=[d[0] for d in DATASETS],
)
def test_dataset_pipeline(dir_name, label, industry, min_tables, tmp_path):
    """Full pipeline run on a real public dataset."""
    dataset_dir = DATASETS_DIR / dir_name
    assert dataset_dir.exists(), f"Dataset not found: {dataset_dir}"

    client_id = dir_name
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Write config
    cfg = _make_config(dataset_dir, output_dir, client_id)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg, default_flow_style=False))

    # Run pipeline
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--config", str(config_path)])

    # ── Assert: no crash ──
    assert result.exit_code == 0, (
        f"Pipeline failed for {label}.\n"
        f"Exit code: {result.exit_code}\n"
        f"Exception: {result.exception}\n"
        f"Output: {result.output[:2000]}"
    )

    # ── Assert: deliverable files exist ──
    client_dir = output_dir / client_id
    assert client_dir.exists(), f"Output directory not created for {label}"

    for fname in DELIVERABLE_FILES:
        fpath = client_dir / fname
        assert fpath.exists(), f"Missing deliverable: {fname} for {label}"
        assert fpath.stat().st_size > 10, f"Empty deliverable: {fname} for {label}"

    # ── Assert: report_data.json contract compliance ──
    report_data = json.loads((client_dir / "report_data.json").read_text())
    missing = REPORT_CONTRACT_KEYS - set(report_data.keys())
    assert not missing, f"report_data.json missing keys: {missing} for {label}"

    # Client info
    assert report_data["client"]["id"] == client_id

    # Schema
    schema = report_data["schema"]
    assert schema["table_count"] >= min_tables, (
        f"Expected ≥{min_tables} tables, got {schema['table_count']} for {label}"
    )

    # Quality aggregated
    quality = report_data["quality"]
    assert "overall_score" in quality, f"Missing overall_score in quality for {label}"
    assert 0 <= quality["overall_score"] <= 100, (
        f"Quality score out of range: {quality['overall_score']} for {label}"
    )

    # PII payload
    pii = report_data["pii"]
    assert "summary" in pii, f"Missing PII summary for {label}"

    # KPIs list
    assert isinstance(report_data["kpis"], list)

    # ── Assert: HTML report is non-trivial ──
    html = (client_dir / "report.html").read_text()
    assert len(html) > 500, f"HTML report suspiciously short for {label}"
    assert "<html" in html.lower() or "<!doctype" in html.lower(), (
        f"HTML report doesn't look like HTML for {label}"
    )

    # ── Assert: text report has key sections ──
    txt = (client_dir / "report.txt").read_text()
    assert "DATA QUALITY" in txt.upper(), f"Text report missing quality section for {label}"

    # ── Assert: source_connection_status.json is valid ──
    conn_status = json.loads((client_dir / "source_connection_status.json").read_text())
    assert conn_status["ok"] is True, f"Connection status not ok for {label}"

    # ── Assert: assets_inventory.json ──
    inventory = json.loads((client_dir / "assets_inventory.json").read_text())
    assert inventory["total_assets"] >= min_tables, (
        f"Asset inventory too small for {label}"
    )

    # ── Assert: sampling_manifest.json ──
    manifest = json.loads((client_dir / "sampling_manifest.json").read_text())
    assert manifest["tables_sampled"] >= min_tables, (
        f"Sampling manifest too small for {label}"
    )
