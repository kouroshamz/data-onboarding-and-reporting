"""Test HTML report generation."""

import pytest
from pathlib import Path
import tempfile
from app.reporting.renderer_html import generate_html_report

def test_html_report_generation():
    """Test basic HTML report generation."""
    
    # Sample data
    test_data = {
        'client': {
            'name': 'Test Client',
            'id': 'test_001'
        },
        'schema': {
            'table_count': 2,
            'tables': {
                'users': {
                    'row_count': 100,
                    'columns': ['id', 'name', 'email']
                },
                'orders': {
                    'row_count': 500,
                    'columns': ['id', 'user_id', 'total']
                }
            }
        },
        'quality': {
            'users': {
                'quality_score': 95.0,
                'completeness_score': 99.5,
                'checks': []
            },
            'orders': {
                'quality_score': 87.5,
                'completeness_score': 98.0,
                'checks': [{'status': 'warning', 'message': 'Stale data'}]
            }
        },
        'pii': {
            'has_pii': True,
            'risk_score': 'medium',
            'pii_column_count': 2,
            'pii_columns': [
                {
                    'column': 'email',
                    'sensitivity': 'medium',
                    'recommendation': 'Consider encryption'
                }
            ]
        },
        'kpi_recommendations': {
            'detected_industry': 'ecommerce',
            'confidence': 0.85,
            'recommended_kpis': [
                {
                    'name': 'Customer Lifetime Value',
                    'category': 'Revenue',
                    'description': 'Total customer value',
                    'status': 'ready'
                }
            ]
        },
        'relationships': {
            'relationships': [
                {
                    'from_table': 'orders',
                    'to_table': 'users',
                    'relationship_type': 'many-to-one',
                    'confidence': 0.9
                }
            ]
        }
    }
    
    # Generate HTML
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        output_path = Path(f.name)
    
    try:
        generate_html_report(test_data, output_path)
        
        # Verify file was created
        assert output_path.exists()
        
        # Verify HTML content
        html_content = output_path.read_text()
        assert 'Test Client' in html_content
        assert 'Data Onboarding Report' in html_content
        assert 'Executive Summary' in html_content
        assert 'Data Quality Flags' in html_content
        assert 'PII' in html_content or 'GDPR' in html_content
        assert 'Column-Level Profiling' in html_content
        
    finally:
        if output_path.exists():
            output_path.unlink()
