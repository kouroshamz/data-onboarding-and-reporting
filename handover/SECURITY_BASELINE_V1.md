# Security Baseline v1

## 1. Data Access Policy
1. Request read-only credentials by default
2. Least privilege for schema and table scopes
3. Log source type and scope, not raw secrets
4. Never embed plaintext credentials in reports or artifacts

## 2. Secret Management
1. Use environment variables for local development
2. Do not commit `.env` or `config.yaml` with secrets
3. Mask secret-like fields in application logs
4. Rotate credentials after onboarding cycle completion

## 3. PII Handling
1. Detect PII fields by regex and name heuristics
2. Report only field metadata and risk level
3. Do not include raw PII values in outputs
4. Use redacted samples when manual debugging is unavoidable

## 4. Logging Requirements
1. Never log row-level sensitive values
2. Include run ID in every major log line
3. Keep error messages actionable but sanitized
4. Retain logs per policy and remove stale logs on schedule

## 5. Artifact and Retention Controls
1. Store curated sample artifacts only in `examples/diagnostic_pack/`
2. Archive superseded artifacts under `handover/archive/`
3. Generated runtime outputs in `reports/` and `logs/` are non-canonical
4. Apply retention policy to generated outputs

## 6. Incident Baseline
Incident classes:
- `SEV-1`: suspected secret or PII leakage
- `SEV-2`: incorrect diagnostic delivered to client
- `SEV-3`: run failure without data exposure

Minimum response:
1. Freeze external sharing of affected artifacts
2. Capture run ID and impacted paths
3. Rotate secrets if exposure suspected
4. Perform root cause analysis and corrective action record

## 7. Security Verification Checklist
1. `.env` and `config.yaml` are ignored
2. No secrets in repo history for current branch
3. Reports inspected for sensitive value leakage
4. PII detection results present in report payload
5. Access mode documented as read-only per client source
