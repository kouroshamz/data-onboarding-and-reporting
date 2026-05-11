"""Test IoT KPI template detection."""

import pytest
from pathlib import Path
import yaml

def test_iot_template_exists():
    """Verify IoT template file exists."""
    template_path = Path("app/kpi/templates/iot.yaml")
    assert template_path.exists(), "IoT template should exist"

def test_iot_template_structure():
    """Verify IoT template has correct structure."""
    template_path = Path("app/kpi/templates/iot.yaml")
    
    with open(template_path) as f:
        template = yaml.safe_load(f)
    
    # Check required fields
    assert 'industry' in template
    assert template['industry'] == 'iot'
    assert 'detection_signals' in template
    assert 'kpis' in template
    
    # Check detection signals
    signals = template['detection_signals']
    assert 'tables' in signals
    assert 'columns' in signals
    assert len(signals['tables']) > 0
    assert len(signals['columns']) > 0
    
    # Check KPIs
    kpis = template['kpis']
    assert len(kpis) >= 5, "Should have at least 5 KPIs"
    
    for kpi in kpis:
        assert 'name' in kpi
        assert 'category' in kpi
        assert 'description' in kpi
        assert 'priority' in kpi

def test_iot_kpi_names():
    """Verify expected IoT KPIs are present."""
    template_path = Path("app/kpi/templates/iot.yaml")
    
    with open(template_path) as f:
        template = yaml.safe_load(f)
    
    kpi_names = [kpi['name'] for kpi in template['kpis']]
    
    # Check for essential IoT KPIs
    expected_kpis = [
        "Device Health Score",
        "Battery Performance Index",
        "Device Uptime & Connectivity"
    ]
    
    for expected in expected_kpis:
        assert expected in kpi_names, f"Should include KPI: {expected}"
