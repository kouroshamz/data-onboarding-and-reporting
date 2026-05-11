"""PDF export functionality.

Primary engine: WeasyPrint (requires system-level Cairo/Pango libs).
Fallback: None — HTML report remains the canonical output.

On macOS:  brew install cairo pango gdk-pixbuf libffi
On Ubuntu: apt-get install libcairo2 libpango-1.0-0 libgdk-pixbuf2.0-0
Then:      pip install weasyprint
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from loguru import logger


# Extra CSS injected for print-quality PDF rendering
_PDF_CSS = """
@page {
    size: A4;
    margin: 18mm 16mm 20mm 16mm;
}
/* Force background colours into print */
* {
    -webkit-print-color-adjust: exact !important;
    print-color-adjust: exact !important;
}
/* Page-break control */
.report-section { page-break-inside: avoid; }
.cover          { page-break-after: always; }
.data-table     { page-break-inside: auto; }
.data-table tr  { page-break-inside: avoid; page-break-after: auto; }
.metric-grid    { page-break-inside: avoid; }
.kpi-card       { page-break-inside: avoid; }
.readiness-hero { page-break-inside: avoid; }
/* Hide interactive-only elements */
details summary { cursor: default; }
/* Ensure page-wrap fills width */
.page-wrap { border: none; box-shadow: none; max-width: 100%; }
body { background: #fff; padding: 0; }
"""


def export_to_pdf(html_path: Path, pdf_path: Path, *, timeout: int = 120) -> bool:
    """Export an HTML report to PDF.

    Returns True on success, False if WeasyPrint is unavailable or export fails.
    The pipeline treats PDF as optional — HTML is always the primary deliverable.
    """
    try:
        from weasyprint import HTML, CSS  # type: ignore[import-untyped]
    except ImportError:
        logger.warning(
            "WeasyPrint not installed — PDF export skipped.  "
            "Install system deps then `pip install weasyprint`."
        )
        return False

    try:
        html_doc = HTML(filename=str(html_path))
        extra_css = CSS(string=_PDF_CSS)
        html_doc.write_pdf(str(pdf_path), stylesheets=[extra_css])
        logger.info("PDF report written to {}", pdf_path)
        return True
    except Exception as exc:
        logger.error("PDF export failed: {}", exc)
        return False


def is_weasyprint_available() -> bool:
    """Check whether WeasyPrint can be imported."""
    try:
        import weasyprint  # noqa: F401
        return True
    except ImportError:
        return False
