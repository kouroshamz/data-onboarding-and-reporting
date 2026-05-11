"""Configuration models using Pydantic."""

import re
from typing import Optional, List, Literal
try:
    from pydantic import BaseModel, Field, field_validator
except ImportError:  # Pydantic v1 compatibility
    from pydantic import BaseModel, Field, validator as _validator

    def field_validator(*fields, mode=None, **kwargs):
        pre = True if mode == "before" else kwargs.pop("pre", False)

        def decorator(func):
            # Unwrap @classmethod if present (v2 style)
            if isinstance(func, classmethod):
                func = func.__func__
            return _validator(*fields, pre=pre, **kwargs)(func)

        return decorator
import os


class ClientConfig(BaseModel):
    id: str
    name: str
    industry: Optional[str] = "auto"


class ConnectionConfig(BaseModel):
    type: Literal["postgresql", "mysql", "mssql", "csv", "s3"]
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    connection_string: Optional[str] = None
    read_only: bool = True
    timeout_seconds: int = 30
    pool_size: int = 5

    @field_validator(
        "host",
        "port",
        "database",
        "username",
        "password",
        "connection_string",
        mode="before",
    )
    @classmethod
    def resolve_env_vars(cls, v):
        """Replace ${VAR} or ${VAR:-default} with environment values."""
        if v and isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            expr = v[2:-1]
            if ":-" in expr:
                var_name, default_value = expr.split(":-", 1)
                return os.getenv(var_name, default_value)
            return os.getenv(expr, "")
        return v


class SamplingConfig(BaseModel):
    enabled: bool = True
    small_table_threshold: int = 100000
    medium_sample_rate: float = 0.10
    large_sample_rate: float = 0.05
    max_sample_size: int = 1000000
    stratify_by_date: bool = True


class AnalysisConfig(BaseModel):
    schema_discovery: bool = True
    data_profiling: bool = True
    quality_checks: bool = True
    pii_detection: bool = True
    relationship_inference: bool = True
    kpi_suggestions: bool = True
    
    top_values_limit: int = 10
    outlier_method: Literal["iqr", "zscore"] = "iqr"
    outlier_threshold: float = 3.0
    
    max_null_percent: float = 50
    min_freshness_days: int = 90
    
    max_tables_for_joins: int = 50
    min_overlap_percent: float = 80


class KPIConfig(BaseModel):
    auto_detect_industry: bool = True
    confidence_threshold: float = 0.7
    generate_sql_examples: bool = True
    max_recommendations: int = 7


class ReportingConfig(BaseModel):
    format: List[Literal["html", "pdf", "json", "txt"]] = ["html", "pdf"]
    include_charts: bool = True
    include_raw_stats: bool = True
    executive_summary_only: bool = False


class OutputConfig(BaseModel):
    directory: str = "./reports"
    retention_days: int = 90
    upload_to_s3: bool = False
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None


class PipelineConfig(BaseModel):
    require_human_qa: bool = True
    timeout_hours: int = 20
    fail_on_partial: bool = False


class LLMLayersConfig(BaseModel):
    type_inspector: bool = True
    insight_detector: bool = True
    report_advisor: bool = True


class LLMCacheConfig(BaseModel):
    enabled: bool = True
    directory: str = ".llm_cache"


class LLMConfig(BaseModel):
    enabled: bool = False
    provider: Literal["openai", "anthropic", "local", "ollama"] = "openai"
    model: str = "gpt-4o-mini"
    api_key: Optional[str] = None
    api_key_env: str = "OPENAI_API_KEY"
    base_url: Optional[str] = None
    temperature: float = 0.1
    max_tokens: int = 2000
    budget_limit_usd: float = 1.0
    layers: LLMLayersConfig = Field(default_factory=LLMLayersConfig)
    cache: LLMCacheConfig = Field(default_factory=LLMCacheConfig)


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    file: str = "./logs/pipeline.log"
    json_format: bool = False


class Config(BaseModel):
    """Complete system configuration."""
    spec_version: str = "1.0.0"
    client: ClientConfig
    connection: ConnectionConfig
    sampling: SamplingConfig = Field(default_factory=SamplingConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    kpi: KPIConfig = Field(default_factory=KPIConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)

    @field_validator("spec_version")
    @classmethod
    def validate_spec_version(cls, v: str) -> str:
        """Ensure spec version is explicit and semver-like."""
        if not re.match(r"^\d+\.\d+\.\d+$", v):
            raise ValueError("spec_version must use MAJOR.MINOR.PATCH format")
        return v

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        """Load configuration from YAML file."""
        import yaml
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
