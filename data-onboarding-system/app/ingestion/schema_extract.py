"""Schema extraction from databases."""

from typing import Dict, List, Any
from datetime import datetime, timezone
import pandas as pd
from loguru import logger

class SchemaExtractor:
    """Extract comprehensive schema metadata from databases."""
    
    def __init__(self, connector):
        self.connector = connector
        
    def _resolve_schema(self, schema: str | None) -> str:
        """Determine the correct default schema for the connector type."""
        if schema is not None:
            return schema
        cfg = getattr(self.connector, "config", None)
        ctype = getattr(cfg, "type", "").lower() if cfg else ""
        if ctype == "mysql":
            return getattr(cfg, "database", "") or ""
        return "public"  # PostgreSQL default

    def extract(self, schema: str | None = None) -> Dict[str, Any]:
        """
        Extract complete schema metadata.
        
        Args:
            schema: Schema/namespace to scan.  *None* auto-detects:
                    PostgreSQL → "public", MySQL → config.database.

        Returns:
            Dictionary with tables, columns, relationships, indexes
        """
        schema = self._resolve_schema(schema)
        logger.info(f"Extracting schema from {schema}")
        
        tables = self.connector.get_table_list(schema)
        logger.info(f"Found {len(tables)} tables")
        
        schema_data = {
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "schema_name": schema,
            "table_count": len(tables),
            "tables": {}
        }
        
        for table in tables:
            logger.debug(f"Extracting metadata for table: {table}")
            schema_data["tables"][table] = self._extract_table_metadata(table, schema)
        
        logger.info("Schema extraction complete")
        return schema_data
    
    def _extract_table_metadata(self, table: str, schema: str) -> Dict[str, Any]:
        """Extract metadata for a single table."""
        try:
            row_count = self.connector.get_table_row_count(table, schema)
            columns = self.connector.get_column_info(table, schema)
            primary_keys = self.connector.get_primary_keys(table, schema)
            foreign_keys = self.connector.get_foreign_keys(table, schema)
            indexes = self.connector.get_indexes(table, schema)
            
            # Estimate table size category
            if row_count < 100000:
                size_category = "small"
            elif row_count < 10000000:
                size_category = "medium"
            else:
                size_category = "large"
            
            return {
                "row_count": row_count,
                "size_category": size_category,
                "column_count": len(columns),
                "columns": columns.to_dict("records"),
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
                "has_primary_key": len(primary_keys) > 0,
                "has_foreign_keys": len(foreign_keys) > 0,
            }
        except Exception as e:
            logger.error(f"Failed to extract metadata for {table}: {e}")
            return {
                "error": str(e),
                "row_count": 0,
                "columns": []
            }
    
    def get_table_summary(self, schema_data: Dict[str, Any]) -> pd.DataFrame:
        """Generate summary DataFrame of all tables."""
        rows = []
        for table_name, table_data in schema_data["tables"].items():
            if "error" not in table_data:
                rows.append({
                    "table_name": table_name,
                    "row_count": table_data["row_count"],
                    "column_count": table_data["column_count"],
                    "size_category": table_data["size_category"],
                    "has_primary_key": table_data["has_primary_key"],
                    "has_foreign_keys": table_data["has_foreign_keys"],
                    "index_count": len(table_data["indexes"])
                })
        
        return pd.DataFrame(rows)
