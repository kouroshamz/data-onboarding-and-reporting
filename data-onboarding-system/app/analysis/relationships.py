"""Relationship inference - discover joins and foreign key patterns."""

from typing import Dict, Any, List, Set, Tuple
import pandas as pd
from loguru import logger

from app.connectors.postgres import PostgreSQLConnector
from app.config import AnalysisConfig


class RelationshipInferencer:
    """Infer table relationships and joinability."""
    
    def __init__(self, connector: PostgreSQLConnector, config: AnalysisConfig):
        self.connector = connector
        self.config = config
    
    def infer_relationships(
        self,
        schema_data: Dict[str, Any],
        profiles: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Infer relationships between tables.
        
        Returns:
            Relationship map with join paths and cardinality
        """
        logger.info("Inferring table relationships")
        
        tables = list(schema_data["tables"].keys())
        
        # Limit to top N tables by size for performance
        if len(tables) > self.config.max_tables_for_joins:
            logger.warning(f"Limiting relationship inference to {self.config.max_tables_for_joins} tables")
            # Sort by row count and take largest
            sorted_tables = sorted(
                tables,
                key=lambda t: schema_data["tables"][t].get("row_count", 0),
                reverse=True
            )
            tables = sorted_tables[:self.config.max_tables_for_joins]
        
        relationships = {
            "inferred_at": pd.Timestamp.now().isoformat(),
            "tables_analyzed": len(tables),
            "relationships": [],
            "candidate_primary_keys": {},
            "join_paths": []
        }
        
        # Find candidate primary keys
        for table in tables:
            relationships["candidate_primary_keys"][table] = self._find_candidate_keys(
                table, profiles.get(table, {})
            )
        
        # Find joinable column pairs
        for i, table1 in enumerate(tables):
            for table2 in tables[i+1:]:
                joins = self._find_potential_joins(
                    table1, table2,
                    schema_data["tables"][table1],
                    schema_data["tables"][table2],
                    profiles.get(table1, {}),
                    profiles.get(table2, {})
                )
                relationships["relationships"].extend(joins)
        
        # Build join paths for common patterns
        relationships["join_paths"] = self._build_join_paths(relationships["relationships"])
        
        logger.info(f"Found {len(relationships['relationships'])} potential relationships")
        return relationships
    
    def _find_candidate_keys(
        self,
        table: str,
        profile: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Find columns that could serve as primary keys."""
        candidates = []
        
        columns = profile.get("columns", {})
        
        for col_name, col_data in columns.items():
            # Check if column is unique and complete
            if (col_data.get("unique_percent", 0) == 100 and
                col_data.get("null_percent", 0) == 0):
                
                candidates.append({
                    "column": col_name,
                    "confidence": "high",
                    "reason": "100% unique, no nulls"
                })
            
            # Check for ID-like column names
            elif "id" in col_name.lower() and col_data.get("unique_percent", 0) > 95:
                candidates.append({
                    "column": col_name,
                    "confidence": "medium",
                    "reason": "ID column with high uniqueness"
                })
        
        return candidates
    
    def _find_potential_joins(
        self,
        table1: str,
        table2: str,
        table1_meta: Dict[str, Any],
        table2_meta: Dict[str, Any],
        profile1: Dict[str, Any],
        profile2: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Find potential join columns between two tables."""
        joins = []
        
        # Get columns from both tables
        cols1 = {c["column_name"]: c for c in table1_meta.get("columns", [])}
        cols2 = {c["column_name"]: c for c in table2_meta.get("columns", [])}
        
        # Look for matching column names
        common_names = set(cols1.keys()) & set(cols2.keys())
        
        for col_name in common_names:
            # Skip if different data types
            if cols1[col_name]["data_type"] != cols2[col_name]["data_type"]:
                continue
            
            # Check if column looks like a key
            if any(keyword in col_name.lower() for keyword in ["id", "key", "code"]):
                
                # Get uniqueness from profiles if available
                unique1 = profile1.get("columns", {}).get(col_name, {}).get("unique_percent", 0)
                unique2 = profile2.get("columns", {}).get(col_name, {}).get("unique_percent", 0)
                
                # Infer cardinality
                cardinality = self._infer_cardinality(unique1, unique2)
                
                joins.append({
                    "table1": table1,
                    "table2": table2,
                    "column": col_name,
                    "data_type": cols1[col_name]["data_type"],
                    "cardinality": cardinality,
                    "confidence": self._calculate_join_confidence(
                        col_name, unique1, unique2
                    ),
                    "join_type": "inner"  # Default recommendation
                })
        
        # Look for foreign key naming patterns (e.g., user_id -> users.id)
        for col1 in cols1:
            for col2 in cols2:
                if self._is_fk_pattern(col1, col2, table1, table2):
                    joins.append({
                        "table1": table1,
                        "table2": table2,
                        "column1": col1,
                        "column2": col2,
                        "cardinality": "many-to-one",
                        "confidence": "medium",
                        "join_type": "left",
                        "pattern": "foreign_key_naming"
                    })
        
        return joins
    
    def _is_fk_pattern(
        self,
        col1: str,
        col2: str,
        table1: str,
        table2: str
    ) -> bool:
        """Check if columns match foreign key naming pattern."""
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        table2_lower = table2.lower()
        
        # Pattern: user_id in table1 -> id in users table
        if (col1_lower == f"{table2_lower}_id" and col2_lower == "id"):
            return True
        
        # Pattern: customer_id -> customers.customer_id
        if (col1_lower.endswith("_id") and 
            col1_lower.replace("_id", "") + "s" == table2_lower and
            col2_lower == "id"):
            return True
        
        return False
    
    def _infer_cardinality(self, unique_pct1: float, unique_pct2: float) -> str:
        """Infer relationship cardinality from uniqueness."""
        if unique_pct1 > 95 and unique_pct2 > 95:
            return "one-to-one"
        elif unique_pct1 > 95:
            return "many-to-one"
        elif unique_pct2 > 95:
            return "one-to-many"
        else:
            return "many-to-many"
    
    def _calculate_join_confidence(
        self,
        col_name: str,
        unique1: float,
        unique2: float
    ) -> str:
        """Calculate confidence in join relationship."""
        # ID columns with high uniqueness = high confidence
        if "id" in col_name.lower() and (unique1 > 90 or unique2 > 90):
            return "high"
        
        # Matching names with some uniqueness = medium
        if unique1 > 50 or unique2 > 50:
            return "medium"
        
        return "low"
    
    def _build_join_paths(
        self,
        relationships: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Build multi-hop join paths between tables."""
        # Build adjacency list
        graph = {}
        for rel in relationships:
            table1 = rel["table1"]
            table2 = rel["table2"]
            
            if table1 not in graph:
                graph[table1] = []
            if table2 not in graph:
                graph[table2] = []
            
            graph[table1].append((table2, rel))
            graph[table2].append((table1, rel))
        
        # Find interesting 2-hop paths
        paths = []
        for start_table in graph:
            for mid_table, rel1 in graph.get(start_table, []):
                for end_table, rel2 in graph.get(mid_table, []):
                    if end_table != start_table and rel1.get("confidence") in ["high", "medium"]:
                        paths.append({
                            "start": start_table,
                            "middle": mid_table,
                            "end": end_table,
                            "hops": 2,
                            "confidence": min(rel1.get("confidence", "low"), rel2.get("confidence", "low"))
                        })
        
        # Return top paths by confidence
        return sorted(paths, key=lambda p: p["confidence"], reverse=True)[:20]
    
    def generate_erd_data(self, relationships: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data for ERD visualization."""
        nodes = set()
        edges = []
        
        for rel in relationships["relationships"]:
            nodes.add(rel["table1"])
            nodes.add(rel["table2"])
            
            edges.append({
                "from": rel["table1"],
                "to": rel["table2"],
                "label": rel.get("column", rel.get("column1", "")),
                "cardinality": rel.get("cardinality", "unknown"),
                "confidence": rel.get("confidence", "low")
            })
        
        return {
            "nodes": [{"id": node, "label": node} for node in nodes],
            "edges": edges
        }
