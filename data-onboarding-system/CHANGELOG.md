# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-02-04

### Added
- Full automated CI pipeline in `.github/workflows/ci.yml`.
- Docker-based clean test image build.
- PostgreSQL integration test service.
- Full pytest suite execution.
- E2E pipeline smoke run.
- Artifact upload (`report.txt`, `report_data.json`, `report.html`, `report.pdf`, `e2e_summary.json`).
- Containerized testing workflow with `docker/Dockerfile.test`.
- Integration and E2E test coverage with fixture-backed PostgreSQL test data.

### Changed
- `config.test.yaml` now supports environment-driven DB connection values using placeholders with defaults (`${VAR:-default}`), removing the need for in-place config rewriting in CI.

### Fixed
- Added runtime ignore for local virtual environments (`.venv/`) in `.gitignore`.
- Missing `numpy` import in quality checks.
- Connector lifecycle safety and null-close guard.
- Unsupported connection type validation (clear `NotImplementedError`).
- Logging directory creation before logger initialization.
- Pydantic v2 validator compatibility updates.

### Notes
- PDF export depends on WeasyPrint system libraries in non-Docker environments.
- For reproducible execution, prefer Docker-based test and E2E flows from `docs/TESTING.md`.
