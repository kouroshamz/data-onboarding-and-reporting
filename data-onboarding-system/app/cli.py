"""Command-line interface for data onboarding system.

Supports all connector types (postgresql, mysql, csv, s3) and integrates
orchestration, security masking, artifact storage, and Jinja reporting.

Pipeline deliverables per PROJECT_SPEC_V1.md §4:
  assets_inventory.json, source_connection_status.json, sampling_manifest.json,
  schema.json, profile.json, kpi_candidates.json,
  report_data.json, report.txt, report.html, report.pdf
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import click
from loguru import logger

from app.config import Config
from app.connectors import create_connector
from app.connectors.base import ConnectionStatus
from app.ingestion.schema_extract import SchemaExtractor
from app.ingestion.sampling import SamplingStrategy
from app.analysis.profiling import DataProfiler
from app.analysis.quality_checks import QualityChecker
from app.analysis.pii_scan import PIIScanner
from app.analysis.relationships import RelationshipInferencer
from app.analysis.structural_overview import compute_structural_overview
from app.analysis.gdpr_assessment import compute_gdpr_assessment
from app.analysis.column_classifier import classify_columns
from app.analysis.interesting_columns import detect_interesting_columns
from app.analysis.missing_strategy import compute_missing_strategy
from app.analysis.readiness_score import compute_readiness_score
from app.kpi.detector import KPIDetector
from app.reporting.renderer_html import generate_html_report
from app.reporting.export_pdf import export_to_pdf
from app.orchestration.engine import PipelineEngine, PipelineStatus
from app.security.masking import DataMasker
from app.security.audit import AuditLogger
from app.storage.local import LocalStorage
from app.llm.service import LLMService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _save_json(path: Path, data: Any) -> Path:
    """Write JSON with default=str serialisation."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    return path


def _aggregate_quality(quality_results: Dict[str, Dict]) -> Dict[str, Any]:
    """Produce top-level aggregated quality payload (contract §6)."""
    from app.analysis.quality_checks import DEFAULT_WEIGHTS

    if not quality_results:
        return {
            "overall_score": 0,
            "weights": dict(DEFAULT_WEIGHTS),
            "components": {k: 0 for k in DEFAULT_WEIGHTS},
            "severity_counts": {},
            "tables": quality_results,
        }

    n = len(quality_results)
    agg_components: Dict[str, float] = {k: 0.0 for k in DEFAULT_WEIGHTS}
    agg_severity: Dict[str, int] = {}

    for tq in quality_results.values():
        for comp, val in tq.get("components", {}).items():
            agg_components[comp] = agg_components.get(comp, 0) + val
        for sev, cnt in tq.get("severity_counts", {}).items():
            agg_severity[sev] = agg_severity.get(sev, 0) + cnt

    agg_components = {k: round(v / n, 1) for k, v in agg_components.items()}
    overall = sum(
        agg_components.get(k, 0) * w / 100 for k, w in DEFAULT_WEIGHTS.items()
    )

    return {
        "overall_score": round(overall, 1),
        "weights": dict(DEFAULT_WEIGHTS),
        "components": agg_components,
        "severity_counts": agg_severity,
        "tables": quality_results,
    }


# ---------------------------------------------------------------------------
# CLI root
# ---------------------------------------------------------------------------

@click.group()
def cli():
    """Data Onboarding System – Analyze client databases in 24 hours."""
    pass


# ---------------------------------------------------------------------------
# Quick-run command (one-liner for CSV files)
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("data", type=click.Path(exists=True))
@click.option("--output", "-o", default=None, help="Output directory (default: ./reports/quick)")
@click.option("--client-name", default="Quick Analysis", help="Client display name")
@click.option("--industry", default="auto", help="Industry hint (auto-detect by default)")
@click.option("--llm/--no-llm", default=False, help="Enable LLM analysis (requires API key)")
def quick(data, output, client_name, industry, llm):
    """Run instant analysis on a CSV/TSV file — no config needed.

    \b
    Usage:
        onboard quick data.csv
        onboard quick data.tsv --llm --client-name "Acme Corp"
    """
    import tempfile, yaml  # noqa: E401
    from pathlib import Path as P

    data_path = P(data).resolve()
    client_id = data_path.stem.replace(" ", "_").lower()
    out_dir = output or f"./reports/{client_id}"

    # Build an in-memory config dict
    cfg_dict = {
        "spec_version": "1.0.0",
        "client": {"id": client_id, "name": client_name, "industry": industry},
        "connection": {"type": "csv", "host": str(data_path.parent), "database": str(data_path)},
        "output": {"directory": out_dir},
        "logging": {"level": "INFO", "file": f"./logs/{client_id}.log"},
    }

    if llm:
        cfg_dict["llm"] = {
            "enabled": True,
            "provider": "openai",
            "model": "gpt-4o-mini",
            "api_key_env": "OPENAI_API_KEY",
            "cache": {"enabled": True, "directory": ".llm_cache"},
        }

    # Write a temporary YAML and delegate to the `run` command
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        yaml.dump(cfg_dict, tmp, default_flow_style=False)
        tmp_path = tmp.name

    click.echo(f"⚡ Quick analysis: {data_path.name}  →  {out_dir}")
    try:
        ctx = click.Context(run)
        ctx.invoke(run, config=tmp_path)
    finally:
        P(tmp_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Main pipeline command
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to configuration YAML file")
def run(config):
    """Run full data onboarding pipeline."""

    cfg = Config.from_yaml(config)
    run_id = uuid.uuid4().hex[:12]

    # Logging
    log_path = Path(cfg.logging.file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(cfg.logging.file, level=cfg.logging.level, rotation="100 MB")
    logger.info("Pipeline {} starting for client: {}", run_id, cfg.client.name)

    # Output + services
    output_dir = Path(cfg.output.directory) / cfg.client.id
    output_dir.mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(root=output_dir)
    audit = AuditLogger(log_dir=log_path.parent)
    masker = DataMasker()
    llm_service = LLMService(cfg.llm)

    connector = None
    try:
        # ================================================================
        # Stage 1 – Connect & Inventory
        # ================================================================
        logger.info("Stage 1: Connecting to data source")
        connector = create_connector(cfg.connection)

        # Legacy connectors may have explicit .connect()
        if hasattr(connector, "connect"):
            connector.connect()

        # -- Deliverable: source_connection_status.json --
        try:
            conn_status = connector.test_connection()
        except Exception:
            conn_status = ConnectionStatus(ok=True)  # connected fine if we got here
        status_payload = {
            "ok": conn_status.ok,
            "error": conn_status.error,
            "latency_ms": conn_status.latency_ms,
            "auth_type": conn_status.auth_type,
            "source_type": cfg.connection.type,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_json(output_dir / "source_connection_status.json", status_payload)
        logger.success("Connected to {} data source", cfg.connection.type)
        audit.log_data_access(cfg.connection.type, 0, actor=cfg.client.id)

        # ================================================================
        # Stage 2 – Schema & Asset Inventory
        # ================================================================
        logger.info("Stage 2: Extracting schema")
        extractor = SchemaExtractor(connector)
        schema_data = extractor.extract()
        logger.success("Extracted schema: {} tables", schema_data["table_count"])

        _save_json(output_dir / "schema.json", schema_data)

        # -- Deliverable: assets_inventory.json --
        assets_inventory: List[Dict[str, Any]] = []
        for tbl_name, tbl_meta in schema_data.get("tables", {}).items():
            assets_inventory.append({
                "source_id": cfg.connection.type,
                "asset_type": "table",
                "name": tbl_name,
                "namespace": "public",
                "row_count": tbl_meta.get("row_count"),
                "column_count": len(tbl_meta.get("columns", [])),
            })
        _save_json(output_dir / "assets_inventory.json", {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_assets": len(assets_inventory),
            "assets": assets_inventory,
        })

        # ================================================================
        # Stage 3 – Profile, Quality, PII (per-table with isolation)
        # ================================================================
        logger.info("Stage 3: Profiling data")
        sampler = SamplingStrategy(connector, cfg.sampling)
        profiler = DataProfiler(connector, cfg.analysis)
        quality_checker = QualityChecker(connector, cfg.analysis)
        pii_scanner = PIIScanner()

        profile_results: Dict[str, Any] = {}
        quality_results: Dict[str, Any] = {}
        pii_results: Dict[str, Any] = {}
        sample_frames: Dict[str, Any] = {}  # collect DataFrames for new analyzers
        sampling_manifest: List[Dict[str, Any]] = []
        failed_tables: List[str] = []

        for table_name, table_meta in schema_data["tables"].items():
            try:
                logger.info("  Analyzing table: {}", table_name)

                # Sampling strategy
                strategy = sampler.determine_strategy(
                    table_name, table_meta.get("row_count", 0)
                )
                sampling_manifest.append({
                    "table": table_name,
                    "row_count": table_meta.get("row_count", 0),
                    **strategy,
                })

                sample_data = sampler.extract_sample(table_name, strategy)
                sample_frames[table_name] = sample_data  # keep for later stages
                audit.log_data_access(
                    table_name,
                    len(sample_data) if hasattr(sample_data, "__len__") else 0,
                )

                # Profile
                profile = profiler.profile_table(table_name, sample_data)
                profile_results[table_name] = profile

                # Quality checks
                quality = quality_checker.check_table_quality(
                    table_name, profile, sample_data
                )
                quality["completeness_score"] = profile.get("completeness_score", 0)
                quality_results[table_name] = quality

                # PII scan
                columns = table_meta.get("columns", [])
                pii = pii_scanner.scan_table(table_name, columns, sample_data)
                pii_results[table_name] = pii

                if pii.get("has_pii"):
                    pii_cols = [c.get("column", "?") for c in pii.get("pii_columns", [])]
                    audit.log_pii_detected(table_name, pii_cols)

                score_key = "overall_score" if "overall_score" in quality else "quality_score"
                logger.success(
                    "  ✓ {}: Quality {:.1f}/100, Completeness {:.1f}%",
                    table_name,
                    float(quality.get(score_key, 0)),
                    float(profile.get("completeness_score", 0)),
                )

            except Exception as table_exc:
                failed_tables.append(table_name)
                logger.error("  ✗ {} failed: {}", table_name, table_exc)
                audit.log_error(f"Table {table_name}: {table_exc}")
                if cfg.pipeline.fail_on_partial:
                    raise

        # -- Deliverable: sampling_manifest.json --
        _save_json(output_dir / "sampling_manifest.json", {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tables_sampled": len(sampling_manifest),
            "tables_failed": failed_tables,
            "manifest": sampling_manifest,
        })

        # Save profiles
        _save_json(output_dir / "profile.json", profile_results)

        # ================================================================
        # Stage 3b – LLM Type Inspector (L1)
        # ================================================================
        type_findings: Dict[str, Any] = {}
        if llm_service.enabled:
            logger.info("Stage 3b: LLM Type Inspector")
            for table_name_l1, table_meta_l1 in schema_data["tables"].items():
                if table_name_l1 in failed_tables:
                    continue
                prof = profile_results.get(table_name_l1, {})
                sample = sampler.extract_sample(table_name_l1, {"method": "full", "size": 200, "reason": "LLM type inspection sample"})
                result = llm_service.inspect_types(table_name_l1, prof, sample)
                if not result.skipped and result.findings:
                    type_findings[table_name_l1] = result.model_dump() if hasattr(result, 'model_dump') else result.dict()
                    # Enrich profile with LLM findings
                    for finding in result.findings:
                        col = finding.column
                        if col in profile_results.get(table_name_l1, {}).get("columns", {}):
                            profile_results[table_name_l1]["columns"][col]["llm_type_analysis"] = {
                                "detected_type": finding.detected_type,
                                "confidence": finding.confidence,
                                "severity": finding.severity,
                                "recommendation": finding.recommendation,
                                "action": finding.action,
                            }
            if type_findings:
                _save_json(output_dir / "type_inspector_results.json", type_findings)
                logger.success("LLM Type Inspector: findings in {} tables", len(type_findings))
                # Re-save enriched profiles
                _save_json(output_dir / "profile.json", profile_results)

        # ================================================================
        # Stage 4 – Relationships
        # ================================================================
        logger.info("Stage 4: Inferring relationships")
        rel_inferencer = RelationshipInferencer(connector, cfg.analysis)
        relationships = rel_inferencer.infer_relationships(schema_data, profile_results)
        logger.success("Found {} relationships", len(relationships.get("relationships", [])))

        # ================================================================
        # Stage 5 – KPI Recommendations
        # ================================================================
        logger.info("Stage 5: Recommending KPIs")
        kpi_detector = KPIDetector(cfg.kpi)
        industry_detection = kpi_detector.detect_industry(
            schema_data, cfg.client.industry
        )
        recommendations = kpi_detector.recommend_kpis(
            industry_detection.get("industry", "general"),
            schema_data,
            profile_results,
        )
        recommended_kpis: List[Dict[str, Any]] = []
        for kpi in recommendations:
            readiness = kpi.get("readiness", {})
            status = "ready" if readiness.get("is_ready") else "partial"
            recommended_kpis.append({**kpi, "status": status})

        # -- Deliverable: kpi_candidates.json --
        _save_json(output_dir / "kpi_candidates.json", {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "detected_industry": industry_detection.get("industry", "general"),
            "detection_confidence": industry_detection.get("confidence", 0),
            "detection_method": industry_detection.get("method", ""),
            "total_candidates": len(recommended_kpis),
            "candidates": recommended_kpis,
        })

        logger.success(
            "Recommended {} KPIs (Industry: {})",
            len(recommended_kpis),
            industry_detection.get("industry", "unknown"),
        )

        # ================================================================
        # Stage 5b – LLM Insight Detector (L2)
        # ================================================================
        llm_insights: Dict[str, Any] = {}
        industry_info = {
            "industry": industry_detection.get("industry", "general"),
            "confidence": industry_detection.get("confidence", 0),
            "method": industry_detection.get("method", ""),
        }
        if llm_service.enabled:
            logger.info("Stage 5b: LLM Insight Detector")
            agg_qual_for_llm = _aggregate_quality(quality_results)
            agg_pii_for_llm = {
                "summary": {
                    "has_pii": any(p.get("has_pii", False) for p in pii_results.values()),
                    "tables_with_pii": sum(1 for p in pii_results.values() if p.get("has_pii", False)),
                    "total_pii_columns": sum(p.get("pii_column_count", 0) for p in pii_results.values()),
                    "risk_score": max(
                        (p.get("risk_score", "none") for p in pii_results.values()),
                        key=lambda x: {"none": 0, "low": 1, "medium": 2, "high": 3}.get(x, 0),
                    ) if pii_results else "none",
                },
                "by_table": pii_results,
            }
            insight_result = llm_service.detect_insights(
                schema_data, profile_results, agg_qual_for_llm,
                agg_pii_for_llm, relationships, recommended_kpis, industry_info,
            )
            if not insight_result.skipped:
                llm_insights = insight_result.model_dump() if hasattr(insight_result, 'model_dump') else insight_result.dict()
                _save_json(output_dir / "insights.json", llm_insights)
                logger.success(
                    "LLM Insights: {} findings, {} good-to-know",
                    len(insight_result.insights), len(insight_result.good_to_know),
                )

        # ================================================================
        # Stage 5c – Advanced Analysis (Section 2,5,6,8,9,10)
        # ================================================================
        logger.info("Stage 5c: Advanced analysis (structural, GDPR, insights)")

        # Source file path for file-size calculation
        source_file_paths = []
        if hasattr(cfg.connection, "path") and cfg.connection.path:
            source_file_paths.append(cfg.connection.path)

        structural_overview = compute_structural_overview(
            schema_data, profile_results, sample_frames, source_file_paths,
        )
        logger.success("Structural overview: {} rows, {} cols, {} dupes",
                        structural_overview["total_rows"],
                        structural_overview["total_columns"],
                        structural_overview["total_duplicate_rows"])

        # GDPR assessment (extends PII scan)
        gdpr_assessment = compute_gdpr_assessment(pii_results, profile_results)
        logger.success("GDPR assessment: {} findings, risk={}",
                        gdpr_assessment["total_pii_findings"],
                        gdpr_assessment["overall_risk"])

        # Business column classification
        column_classifications = classify_columns(profile_results, schema_data)
        logger.success("Column classifier: {} classified, categories={}",
                        column_classifications["summary"]["total_classified"],
                        column_classifications["summary"]["categories_found"])

        # Interesting columns detection
        interesting_columns = detect_interesting_columns(
            profile_results, sample_frames, column_classifications,
        )
        logger.success("Interesting columns: {} flagged", interesting_columns["count"])

        # Missing data strategy
        missing_strategy = compute_missing_strategy(profile_results, column_classifications)
        logger.success("Missing strategy: {} columns need treatment",
                        missing_strategy["summary"]["total_columns_with_nulls"])

        # ================================================================
        # Stage 5d – Security Masking (redact PII in report artefacts)
        # ================================================================
        logger.info("Stage 5d: Applying security masking")
        masked_col_count = 0
        for tbl_name, tbl_pii in pii_results.items():
            if not tbl_pii.get("has_pii"):
                continue
            pii_col_names = [c.get("column", "") for c in tbl_pii.get("pii_columns", [])]
            if not pii_col_names:
                continue
            # Mask top_values in profile results for PII columns
            tbl_profile = profile_results.get(tbl_name, {})
            for col_name in pii_col_names:
                col_profile = tbl_profile.get("columns", {}).get(col_name, {})
                if col_profile.get("top_values"):
                    for tv in col_profile["top_values"]:
                        tv["value"] = "***MASKED***"
                    masked_col_count += 1
        if masked_col_count:
            logger.success("Masked top-value samples for {} PII columns in profile output", masked_col_count)
        else:
            logger.info("No PII columns required masking in report output")

        # ================================================================
        # Stage 6 – Report Assembly
        # ================================================================
        logger.info("Stage 6: Generating reports")

        # Aggregate PII
        aggregated_pii = {
            "has_pii": any(p.get("has_pii", False) for p in pii_results.values()),
            "tables_with_pii": sum(
                1 for p in pii_results.values() if p.get("has_pii", False)
            ),
            "total_pii_columns": sum(
                p.get("pii_column_count", 0) for p in pii_results.values()
            ),
            "risk_score": max(
                (p.get("risk_score", "none") for p in pii_results.values()),
                key=lambda x: {"none": 0, "low": 1, "medium": 2, "high": 3}.get(x, 0),
            ) if pii_results else "none",
            "by_table": pii_results,
        }

        # Aggregate quality (contract §6)
        aggregated_quality = _aggregate_quality(quality_results)

        # ================================================================
        # Stage 6b – LLM Report Advisor (L3)
        # ================================================================
        llm_report_layout: Dict[str, Any] = {}
        if llm_service.enabled:
            logger.info("Stage 6b: LLM Report Advisor")
            advisor_result = llm_service.advise_report(
                schema_data, aggregated_quality,
                {"summary": aggregated_pii, "by_table": pii_results},
                relationships, recommended_kpis, industry_info,
                llm_insights if llm_insights else None,
                type_findings if type_findings else None,
            )
            if not advisor_result.skipped:
                llm_report_layout = advisor_result.model_dump() if hasattr(advisor_result, 'model_dump') else advisor_result.dict()
                _save_json(output_dir / "report_layout.json", llm_report_layout)
                logger.success("LLM Report Advisor: layout generated")

        # -- Readiness Score (needs aggregated quality + gdpr + kpis) --
        # Flatten LLM type findings for readiness scorer
        flat_type_findings = []
        for _tf_table, _tf_data in type_findings.items():
            flat_type_findings.extend(_tf_data.get("findings", []))

        readiness_score = compute_readiness_score(
            profile_results, structural_overview, aggregated_quality,
            pii_results, gdpr_assessment, recommended_kpis,
            column_classifications, relationships, flat_type_findings or None,
        )
        logger.success("Readiness score: {}/100 ({})",
                        readiness_score["total_score"], readiness_score["grade"])

        # Determine source type for cover page
        source_type = cfg.connection.type

        report_data: Dict[str, Any] = {
            "schema_version": cfg.spec_version,
            "run_id": run_id,
            "client": {
                "id": cfg.client.id,
                "name": cfg.client.name,
                "industry": cfg.client.industry,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_type": source_type,
            "profiling": profile_results,
            "schema": schema_data,
            "profiles": profile_results,
            "quality": aggregated_quality,
            "pii": {"summary": aggregated_pii, "by_table": pii_results},
            "relationships": relationships,
            "industry": industry_info,
            "kpis": recommended_kpis,
            # New analysis sections
            "structural_overview": structural_overview,
            "gdpr_assessment": gdpr_assessment,
            "column_classifications": column_classifications,
            "interesting_columns": interesting_columns,
            "missing_strategy": missing_strategy,
            "readiness_score": readiness_score,
            # LLM
            "llm_insights": llm_insights,
            "llm_type_findings": type_findings,
            "llm_report_layout": llm_report_layout,
            "llm_usage": llm_service.usage_dict(),
            # Security
            "masking_applied": masked_col_count > 0,
            "masked_columns_count": masked_col_count,
        }

        _save_json(output_dir / "report_data.json", report_data)
        logger.success("Saved JSON report")

        # Text summary
        _generate_text_report(report_data, output_dir / "report.txt")
        logger.success("Saved text report")

        # HTML report
        html_path = output_dir / "report.html"
        generate_html_report(report_data, html_path)
        logger.success("Saved HTML report")

        # PDF (optional)
        pdf_path = output_dir / "report.pdf"
        if export_to_pdf(html_path, pdf_path):
            logger.success("Saved PDF report")
        else:
            logger.warning("PDF export skipped (WeasyPrint not available)")

        # Save LLM usage
        if llm_service.enabled:
            llm_service.save_usage(output_dir / "llm_usage.json")

        # Pipeline summary
        audit.log_pipeline_run(run_id, "completed", 6)
        logger.success(
            "✅ Pipeline {} complete! {} tables processed, {} failed. Reports → {}",
            run_id,
            len(profile_results),
            len(failed_tables),
            output_dir,
        )

    except Exception as e:
        logger.error("Pipeline {} failed: {}", run_id, e)
        audit.log_error(str(e))
        raise
    finally:
        if connector is not None:
            connector.close()


# ---------------------------------------------------------------------------
# Validate-only command
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--test-connection", "test_conn", is_flag=True, default=False,
              help="Test actual connectivity to the data source")
def validate(config, test_conn):
    """Validate configuration file and optionally test the connection."""
    try:
        cfg = Config.from_yaml(config)
        click.echo(f"✅ Configuration valid  (spec {cfg.spec_version})")
        click.echo(f"   Client : {cfg.client.name} [{cfg.client.id}]")
        click.echo(f"   Source : {cfg.connection.type}")
        click.echo(f"   Output : {cfg.output.directory}")
    except Exception as e:
        click.echo(f"❌ Configuration error: {e}", err=True)
        raise SystemExit(1)

    if test_conn:
        click.echo("\n🔌 Testing connection …")
        try:
            connector = create_connector(cfg.connection)
            if hasattr(connector, "connect"):
                connector.connect()
            status = connector.test_connection()
            if status.ok:
                click.echo(f"✅ Connection OK  (latency: {status.latency_ms}ms, auth: {status.auth_type})")
                # List assets
                assets = connector.list_assets()
                click.echo(f"   Assets found: {len(assets)}")
                for a in assets[:10]:
                    click.echo(f"     • {a.name} ({a.asset_type})")
                if len(assets) > 10:
                    click.echo(f"     … and {len(assets) - 10} more")
            else:
                click.echo(f"❌ Connection failed: {status.error}", err=True)
                raise SystemExit(2)
            connector.close()
        except SystemExit:
            raise
        except Exception as e:
            click.echo(f"❌ Connection test error: {e}", err=True)
            raise SystemExit(2)


# ---------------------------------------------------------------------------
# Schema-only command
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--output", "-o", default=None, help="Output JSON path")
def schema(config, output):
    """Extract schema without running full pipeline."""
    cfg = Config.from_yaml(config)
    connector = create_connector(cfg.connection)
    if hasattr(connector, "connect"):
        connector.connect()
    try:
        extractor = SchemaExtractor(connector)
        schema_data = extractor.extract()
        out = output or f"./reports/{cfg.client.id}/schema.json"
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        _save_json(Path(out), schema_data)
        click.echo(f"✅ Schema extracted → {out}  ({schema_data['table_count']} tables)")
    finally:
        connector.close()


# ---------------------------------------------------------------------------
# Text report helper
# ---------------------------------------------------------------------------

def _generate_text_report(data: dict, output_path: Path):
    """Generate human-readable text report."""
    with open(output_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write(f"  DATA ONBOARDING REPORT - {data['client']['name']}\n")
        f.write("=" * 80 + "\n\n")

        schema = data["schema"]
        f.write("SCHEMA SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Tables: {schema['table_count']}\n")
        f.write(f"Generated: {data['generated_at']}\n\n")

        # Quality overview – use aggregated quality
        quality = data["quality"]
        f.write("DATA QUALITY OVERVIEW\n")
        f.write("-" * 80 + "\n")
        f.write(f"Overall Score: {quality.get('overall_score', 'N/A')}/100\n")
        for table, tq in quality.get("tables", {}).items():
            score = tq.get("overall_score", tq.get("quality_score", 0))
            f.write(f"  {table}: {float(score):.1f}/100\n")
        f.write("\n")

        pii = data["pii"]
        f.write("PII FINDINGS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Tables with PII: {pii['summary']['tables_with_pii']}/{schema['table_count']}\n")
        f.write(f"Total PII columns: {pii['summary']['total_pii_columns']}\n")
        f.write(f"Risk Level: {pii['summary']['risk_score'].upper()}\n\n")

        relationships = data["relationships"]
        f.write("RELATIONSHIPS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Found: {len(relationships.get('relationships', []))} relationships\n\n")

        industry = data["industry"]
        kpis = data["kpis"]
        f.write("RECOMMENDED KPIs\n")
        f.write("-" * 80 + "\n")
        f.write(f"Detected Industry: {industry.get('industry', 'unknown')} ")
        f.write(f"(confidence: {industry.get('confidence', 0):.0%})\n\n")

        for i, rec in enumerate(kpis, 1):
            symbol = "✓" if rec["status"] == "ready" else "⚠" if rec["status"] == "partial" else "✗"
            f.write(f"{i}. {symbol} {rec['name']} - {rec['description']}\n")


if __name__ == "__main__":
    cli()
