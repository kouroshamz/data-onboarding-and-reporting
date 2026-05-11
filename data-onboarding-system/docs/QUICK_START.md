# Quick Start Guide

## Installation

```bash
# Clone the repository
cd data-onboarding-system

# Install dependencies
pip install -r requirements.txt

# Or use Docker
docker build -f docker/Dockerfile -t data-onboarding:latest .
```

## Configuration

1. Copy example config:
```bash
cp config.example.yaml config.yaml
```

2. Edit `config.yaml` with your database credentials:
```yaml
client:
  id: "client_001"
  name: "Your Client Name"
  industry: "ecommerce"  # or "saas", "marketing", "auto"

connection:
  type: "postgresql"
  host: "your-db-host.com"
  port: 5432
  database: "production"
  username: "readonly_user"
  password: "${DB_PASSWORD}"  # Set DB_PASSWORD in .env
```

3. Set environment variables:
```bash
cp .env.example .env
# Edit .env with your secrets
```

## Running Analysis

### Local
```bash
python -m app.cli run --config config.yaml
```

### Docker
```bash
docker run -v $(pwd)/config.yaml:/app/config.yaml \
           -v $(pwd)/reports:/app/reports \
           -v $(pwd)/.env:/app/.env \
           data-onboarding:latest
```

## Output

Reports are generated in `./reports/{client_id}/`:
- `report_data.json` - Complete analysis data
- `report.txt` - Human-readable summary
- `report.html` - Formatted report for browser review
- `report.pdf` - Portable print/share output (if enabled)
- `schema.json` - Database schema
- `profile.json` - Data profiling results

## What's Next?

Use the canonical handover documentation:
- `../../handover/INDEX.md`
- `../../handover/PROJECT_SPEC_V1.md`
- `../../handover/OPERATIONS_RUNBOOK_V1.md`
