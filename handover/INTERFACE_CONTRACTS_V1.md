# Interface Contracts v1

## 1. Versioning Rules
- Spec version: `1.0.0`
- Every output payload must include a schema/version field where applicable
- Backward-incompatible changes require spec minor/major update and migration notes

## 2. Connector Contract (Required Methods)
Each connector implementation must provide:

```python
def test_connection() -> ConnectionStatus: ...
def list_assets() -> list[AssetRef]: ...
def get_schema(asset: AssetRef) -> SchemaInfo: ...
def sample(asset: AssetRef, n: int) -> "DataFrame": ...
def estimate_row_count(asset: AssetRef) -> int | None: ...
def get_freshness(asset: AssetRef) -> "datetime | None": ...
```

### Connector Types
`AssetRef`
- `source_id: str`
- `asset_type: str` (`table`, `file`, `object`)
- `name: str`
- `namespace: str | None`
- `identifier: str`

`SchemaInfo`
- `columns: list[ColumnInfo]`

`ColumnInfo`
- `name: str`
- `declared_type: str`
- `inferred_type: str`
- `nullable: bool`
- `notes: str | None`

`ConnectionStatus`
- `ok: bool`
- `error: str | None`
- `latency_ms: int | None`
- `auth_type: str`

## 3. Config Contract
Top-level config requires `spec_version` and sections:
- `spec_version`
- `client`
- `connection`
- `sampling`
- `analysis`
- `kpi`
- `reporting`
- `output`
- `pipeline`
- `logging`

Minimum constraints:
- `spec_version`: string, default `1.0.0`
- `client.id`: non-empty string
- `connection.type`: one of `postgresql`, `mysql`, `mssql`, `csv`, `s3`
- `sampling.max_sample_size`: integer > 0
- `analysis.max_null_percent`: number between 0 and 100
- `kpi.max_recommendations`: integer > 0
- `reporting.format`: subset of `html`, `pdf`, `txt`, `json`

## 4. Report Output Contract (`report_data.json`)
Must include:
- `schema_version`
- `client`
- `schema`
- `profiling`
- `quality`
- `pii`
- `relationships`
- `kpis`
- `generated_at`

Example shape:

```json
{
  "schema_version": "1.0.0",
  "client": {"id": "client_001", "name": "Example"},
  "schema": {"table_count": 0, "tables": []},
  "profiling": {},
  "quality": {"overall_score": 0, "tables": []},
  "pii": {"summary": {}, "findings": []},
  "relationships": {"relationships": []},
  "kpis": [],
  "generated_at": "2026-02-13T00:00:00Z"
}
```

## 5. KPI Candidate Contract
Every KPI candidate must include:
- `name: str`
- `description: str`
- `formula_sql: str`
- `required_fields: list[str]`
- `required_tables: list[str]`
- `grain: str`
- `dimensions: list[str]`
- `confidence: float` (0.0 to 1.0)
- `blocked_by: list[str]`

## 6. Quality Score Contract
`quality` payload must include:
- `overall_score` (0-100)
- `weights`
- `components`
- `severity_counts`

Required weight keys:
- `missingness`
- `validity`
- `uniqueness`
- `freshness`
- `integrity`

## 7. Reporting Template Contracts
- `handover/templates/brand.yaml`: company branding tokens
- `handover/templates/report_template.yaml`: section order and writing/visual rules
- `handover/templates/impact_rules.yaml`: metric-specific business impact phrasing and action mappings
