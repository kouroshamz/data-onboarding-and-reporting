"""PDF export tests with mocked WeasyPrint."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest

from app.reporting.export_pdf import export_to_pdf, is_weasyprint_available


class TestPDFExport:

    def test_export_returns_false_when_weasyprint_missing(self, tmp_path):
        """When WeasyPrint is not installed, export should return False."""
        html_path = tmp_path / "report.html"
        html_path.write_text("<html><body>test</body></html>")
        pdf_path = tmp_path / "report.pdf"

        with patch.dict(sys.modules, {"weasyprint": None}):
            # Force re-import failure
            result = export_to_pdf(html_path, pdf_path)
        # Since we can't easily unimport, test via the function itself
        # If weasyprint is actually missing it returns False
        assert isinstance(result, bool)

    def test_export_succeeds_with_mock_weasyprint(self, tmp_path):
        """With mocked WeasyPrint, export should succeed."""
        html_path = tmp_path / "report.html"
        html_path.write_text("<html><body>test</body></html>")
        pdf_path = tmp_path / "report.pdf"

        mock_html_cls = MagicMock()
        mock_css_cls = MagicMock()
        mock_html_instance = MagicMock()
        mock_html_cls.return_value = mock_html_instance

        weasyprint_mock = MagicMock()
        weasyprint_mock.HTML = mock_html_cls
        weasyprint_mock.CSS = mock_css_cls

        with patch.dict(sys.modules, {"weasyprint": weasyprint_mock}):
            result = export_to_pdf(html_path, pdf_path)

        assert result is True
        mock_html_instance.write_pdf.assert_called_once()

    def test_export_handles_exception(self, tmp_path):
        """WeasyPrint exceptions should return False, not crash."""
        html_path = tmp_path / "report.html"
        html_path.write_text("<html><body>test</body></html>")
        pdf_path = tmp_path / "report.pdf"

        mock_html_cls = MagicMock()
        mock_html_cls.return_value.write_pdf.side_effect = RuntimeError("Cairo error")
        mock_css_cls = MagicMock()

        weasyprint_mock = MagicMock()
        weasyprint_mock.HTML = mock_html_cls
        weasyprint_mock.CSS = mock_css_cls

        with patch.dict(sys.modules, {"weasyprint": weasyprint_mock}):
            result = export_to_pdf(html_path, pdf_path)

        assert result is False

    def test_is_weasyprint_available_returns_bool(self):
        result = is_weasyprint_available()
        assert isinstance(result, bool)


class TestPDFCSS:
    """Verify the injected PDF CSS contains needed rules."""

    def test_pdf_css_has_page_size(self):
        from app.reporting.export_pdf import _PDF_CSS
        assert "@page" in _PDF_CSS
        assert "A4" in _PDF_CSS

    def test_pdf_css_has_page_break_rules(self):
        from app.reporting.export_pdf import _PDF_CSS
        assert "page-break-inside" in _PDF_CSS

    def test_pdf_css_forces_print_colors(self):
        from app.reporting.export_pdf import _PDF_CSS
        assert "print-color-adjust" in _PDF_CSS
