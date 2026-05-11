#!/usr/bin/env python3
"""Data Onboarding Dashboard — Interactive report comparison dashboard.

Serves the comparison dashboard with live API endpoints for:
  - Listing existing reports
  - Uploading & analyzing new datasets
  - Removing reports from the dashboard

Usage:
    python -m app.dashboard_server [--port 8787]
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import unquote, urlparse

REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
PROJECT_DIR = Path(__file__).resolve().parent.parent


class DashboardHandler(SimpleHTTPRequestHandler):
    """HTTP handler that serves static reports + JSON API."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(REPORTS_DIR), **kwargs)

    # ── Routing ───────────────────────────────────────────────────────

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/reports":
            return self._list_reports()
        return super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/analyze":
            return self._analyze()
        self.send_error(404)

    def do_DELETE(self):
        path = urlparse(self.path).path
        if path.startswith("/api/reports/"):
            report_id = unquote(path[len("/api/reports/"):])
            return self._delete_report(report_id)
        self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    # ── API: list reports ─────────────────────────────────────────────

    def _list_reports(self):
        reports = []
        for rj in sorted(REPORTS_DIR.rglob("report_data.json")):
            try:
                data = json.loads(rj.read_text())
                rel = rj.parent.relative_to(REPORTS_DIR)
                so = data.get("structural_overview", {})
                quality = data.get("quality", {})
                rs = data.get("readiness_score", {})
                pii = data.get("pii", {}).get("summary", {})
                llm = data.get("llm_usage", {})

                # Derive quality components for radar comparison
                components = quality.get("components", {})

                reports.append({
                    "id": str(rel),
                    "path": str(rel),
                    "label": data.get("client", {}).get("name", str(rel)),
                    "rows": so.get("total_rows", 0),
                    "columns": so.get("total_columns", 0),
                    "quality": round(quality.get("overall_score", 0), 1),
                    "readiness": rs.get("total_score", 0),
                    "grade": rs.get("grade", "?"),
                    "pii_columns": pii.get("total_pii_columns", 0),
                    "pii_risk": pii.get("risk_score", "none"),
                    "duplicates": so.get("total_duplicate_rows", 0),
                    "duplicate_pct": round(so.get("duplicate_pct", 0), 1),
                    "kpis": len(data.get("kpis", [])),
                    "industry": data.get("industry", {}).get("industry", "?"),
                    "llm_calls": llm.get("total_calls", 0),
                    "has_report": (rj.parent / "report.html").exists(),
                    "quality_components": {
                        k: round(v, 1) for k, v in components.items()
                    },
                    "dtype_distribution": so.get("dtype_distribution", {}),
                    "columns_with_nulls": so.get("columns_with_nulls", 0),
                    "columns_fully_null": so.get("columns_fully_null", 0),
                })
            except Exception:
                pass
        self._send_json(reports)

    # ── API: analyze uploaded file ────────────────────────────────────

    def _analyze(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            payload = json.loads(body)
        except Exception as e:
            return self._send_json({"ok": False, "error": f"Bad request: {e}"}, 400)

        filename = payload.get("filename", "upload.csv")
        content = payload.get("content", "")
        client_name = payload.get("client_name", "").strip()
        if not client_name:
            client_name = filename.rsplit(".", 1)[0]

        # Slug for report directory
        slug = client_name.lower().replace(" ", "_").replace("/", "_")
        slug = "".join(c for c in slug if c.isalnum() or c == "_")
        if not slug:
            slug = "upload"

        # Write temp file
        suffix = ".tsv" if filename.lower().endswith(".tsv") else ".csv"
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=suffix, delete=False, prefix=f"{slug}_",
        )
        tmp.write(content)
        tmp.close()

        output_dir = REPORTS_DIR / slug

        try:
            python = sys.executable
            cmd = [
                python, "-m", "app.cli", "quick",
                tmp.name,
                "--output", str(output_dir),
                "--client-name", client_name,
            ]
            result = subprocess.run(
                cmd, cwd=str(PROJECT_DIR),
                capture_output=True, text=True, timeout=180,
            )
            os.unlink(tmp.name)

            if result.returncode != 0:
                err = result.stderr[-800:] if result.stderr else "Pipeline failed"
                return self._send_json({"ok": False, "error": err}, 500)

            # Find the generated report
            rj_files = list(output_dir.rglob("report_data.json"))
            if rj_files:
                rel = rj_files[0].parent.relative_to(REPORTS_DIR)
                return self._send_json({"ok": True, "id": str(rel), "path": str(rel)})
            return self._send_json({"ok": True, "id": slug, "path": slug})

        except subprocess.TimeoutExpired:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)
            return self._send_json(
                {"ok": False, "error": "Analysis timed out (>3 min)"}, 500
            )
        except Exception as e:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)
            return self._send_json({"ok": False, "error": str(e)}, 500)

    # ── API: delete report ────────────────────────────────────────────

    def _delete_report(self, report_id):
        # Sanitise path
        safe = Path(report_id)
        if ".." in safe.parts:
            return self._send_json({"ok": False, "error": "Invalid ID"}, 400)

        target = REPORTS_DIR / safe
        if not target.exists() and safe.parts:
            # Try top-level parent (e.g. "diamonds/data" → delete "diamonds")
            target = REPORTS_DIR / safe.parts[0]

        if target.exists() and target.is_dir():
            shutil.rmtree(target)
            return self._send_json({"ok": True})

        return self._send_json({"ok": False, "error": "Report not found"}, 404)

    # ── Helpers ───────────────────────────────────────────────────────

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def log_message(self, fmt, *args):
        msg = args[0] if args else ""
        if "/api/" in str(msg) or "404" in str(msg):
            super().log_message(fmt, *args)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Data Onboarding Dashboard")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    # Ensure reports directory exists
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\033[1;33m{'=' * 56}\033[0m")
    print(f"\033[1;33m  DATA ONBOARDING DASHBOARD\033[0m")
    print(f"\033[1;33m{'=' * 56}\033[0m")
    print(f"  Dashboard : http://{args.host}:{args.port}/compare.html")
    print(f"  Reports   : {REPORTS_DIR}")
    print()
    print(f"  API endpoints:")
    print(f"    GET    /api/reports         List all reports")
    print(f"    POST   /api/analyze         Upload & analyze CSV")
    print(f"    DELETE /api/reports/<id>     Remove a report")
    print(f"\033[1;33m{'=' * 56}\033[0m")
    print()

    server = HTTPServer((args.host, args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()
