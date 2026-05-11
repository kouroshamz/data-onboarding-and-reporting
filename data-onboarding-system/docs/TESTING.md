# Testing Guide

This project supports three test layers:

- Unit tests (logic-level)
- Integration tests (live PostgreSQL)
- End-to-end pipeline smoke (CLI output + report artifacts)

## 1) Local Unit Tests

```bash
cd data-onboarding-system
pytest tests/ -v -m "not integration"
```

## Local Runtime Note (PDF)

`report.pdf` generation depends on WeasyPrint system libraries.

- On Debian/Ubuntu install: `libcairo2 libpango-1.0-0 libpangoft2-1.0-0 libgdk-pixbuf-2.0-0 libharfbuzz-subset0 fonts-dejavu-core`
- If you want zero host setup, run tests through `docker/Dockerfile.test` (recommended below)

## 2) Integration Tests with Docker DB

```bash
cd data-onboarding-system
docker compose -f docker-compose.test.yml up -d test-postgres
pytest tests/ -v
docker compose -f docker-compose.test.yml down
```

## 3) Clean-Environment Tests (Recommended)

Use the reusable test image to avoid host dependency drift:

```bash
cd data-onboarding-system
docker build -f docker/Dockerfile.test -t data-onboarding-test:latest .
docker compose -f docker-compose.test.yml up -d test-postgres

docker run --rm \
  --network data-onboarding-system_default \
  -e DB_HOST=test-postgres \
  -e DB_PORT=5432 \
  -e DB_NAME=testdb \
  -e DB_USER=testuser \
  -e DB_PASSWORD=testpass \
  -e PYTHONPATH=/app \
  -v "$PWD:/app" \
  -w /app \
  data-onboarding-test:latest \
  pytest tests/ -v
```

## 4) E2E Pipeline Smoke

```bash
docker run --rm \
  --network data-onboarding-system_default \
  -e DB_HOST=test-postgres \
  -e DB_PORT=5432 \
  -e DB_NAME=testdb \
  -e DB_USER=testuser \
  -e DB_PASSWORD=testpass \
  -e PYTHONPATH=/app \
  -v "$PWD:/app" \
  -w /app \
  data-onboarding-test:latest \
  python -m app.cli run --config config.test.yaml
```

Expected outputs under `test_reports/test_001/`:

- `report_data.json`
- `report.txt`
- `report.html`
- `report.pdf`
- `schema.json`
- `profile.json`

## CI

GitHub Actions workflow: `../../.github/workflows/ci.yml`

It performs:

1. Builds cached test image (`docker/Dockerfile.test`)
2. Starts PostgreSQL test service
3. Runs full pytest suite
4. Runs E2E smoke pipeline
5. Uploads artifacts:
   - `report.txt`
   - `report_data.json`
   - `report.html`
   - `report.pdf`
   - `e2e_summary.json`
