"""HTML and PDF report rendering."""

from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader, select_autoescape
from loguru import logger


class ReportRenderer:
    """Render onboarding reports into HTML and PDF formats."""

    def __init__(self):
        template_dir = Path(__file__).parent / "templates"
        self._env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
        )

    def render_html(self, report_data: Dict[str, Any]) -> str:
        """Render report data to an HTML string."""
        template = self._env.get_template("report.html.j2")
        return template.render(report=report_data)

    def write_html(self, report_data: Dict[str, Any], output_path: Path) -> Path:
        """Render and persist HTML report."""
        html = self.render_html(report_data)
        output_path.write_text(html, encoding="utf-8")
        return output_path

    def write_pdf(self, html_path: Path, output_path: Path) -> Path:
        """
        Convert HTML report to PDF.

        Raises:
            RuntimeError: if WeasyPrint is unavailable or PDF generation fails.
        """
        try:
            from weasyprint import HTML
        except Exception as exc:  # pragma: no cover - import path depends on env
            raise RuntimeError("WeasyPrint is not available for PDF export") from exc

        try:
            HTML(filename=str(html_path)).write_pdf(str(output_path))
        except Exception as exc:
            raise RuntimeError(f"Failed to generate PDF report: {exc}") from exc

        logger.info(f"PDF report generated: {output_path}")
        return output_path
