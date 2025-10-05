"""
Configuration management for NASA data pipelines.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union
from enum import Enum
import os


class DestinationType(Enum):
    """Supported destination types for data storage."""
    
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    PARQUET = "parquet"
    CSV = "csv"
    JSONL = "jsonl"


@dataclass
class PipelineConfig:
    """Configuration for NASA data pipeline."""
    
    pipeline_name: str
    destination_type: DestinationType
    destination_params: Dict[str, Any] = field(default_factory=dict)
    batch_size: int = 1000
    max_records: Optional[int] = None
    schema_name: Optional[str] = None
    table_prefix: str = "nasa_"
    
    # Query configuration
    default_columns: Optional[list] = None
    filters: Dict[str, Any] = field(default_factory=dict)
    
    # Runtime configuration
    parallel_load: bool = True
    retry_attempts: int = 3
    timeout_seconds: int = 300
    
    # Data processing options
    normalize_columns: bool = True
    handle_nulls: bool = True
    convert_types: bool = True
    
    @classmethod
    def from_env(cls, pipeline_name: str) -> 'PipelineConfig':
        """
        Create configuration from environment variables.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            PipelineConfig instance
        """
        dest_type = os.getenv("NASA_DESTINATION_TYPE", "duckdb")
        dest_params = {}
        
        if dest_type in ["postgres", "mysql"]:
            dest_params.update({
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432" if dest_type == "postgres" else "3306")),
                "database": os.getenv("DB_NAME", "nasa_data"),
                "username": os.getenv("DB_USER", ""),
                "password": os.getenv("DB_PASSWORD", ""),
            })
        elif dest_type == "duckdb":
            dest_params["database_path"] = os.getenv("DUCKDB_PATH", f"{pipeline_name}.duckdb")
        elif dest_type == "sqlite":
            dest_params["database_path"] = os.getenv("SQLITE_PATH", f"{pipeline_name}.sqlite")
        elif dest_type in ["parquet", "csv", "jsonl"]:
            dest_params["file_path"] = os.getenv("FILE_PATH", f"./data/{pipeline_name}")
            
        return cls(
            pipeline_name=pipeline_name,
            destination_type=DestinationType(dest_type),
            destination_params=dest_params,
            batch_size=int(os.getenv("BATCH_SIZE", "1000")),
            max_records=int(os.getenv("MAX_RECORDS")) if os.getenv("MAX_RECORDS") else None,
            schema_name=os.getenv("SCHEMA_NAME"),
            table_prefix=os.getenv("TABLE_PREFIX", "nasa_"),
        )
    
    @classmethod
    def for_local_development(cls, pipeline_name: str, data_dir: str = "./data") -> 'PipelineConfig':
        """
        Create a configuration suitable for local development.
        
        Args:
            pipeline_name: Name of the pipeline
            data_dir: Directory to store data files
            
        Returns:
            PipelineConfig for local development
        """
        return cls(
            pipeline_name=pipeline_name,
            destination_type=DestinationType.DUCKDB,
            destination_params={"database_path": f"{data_dir}/{pipeline_name}.duckdb"},
            batch_size=500,
            max_records=10000,  # Limit for development
            schema_name="dev",
        )
    
    @classmethod 
    def for_production(cls, pipeline_name: str, destination_type: str, **kwargs) -> 'PipelineConfig':
        """
        Create a production-ready configuration.
        
        Args:
            pipeline_name: Name of the pipeline
            destination_type: Type of destination (postgres, bigquery, etc.)
            **kwargs: Additional destination parameters
            
        Returns:
            PipelineConfig for production use
        """
        return cls(
            pipeline_name=pipeline_name,
            destination_type=DestinationType(destination_type),
            destination_params=kwargs,
            batch_size=5000,
            parallel_load=True,
            retry_attempts=5,
            timeout_seconds=600,
        )


@dataclass
class QueryConfig:
    """Configuration for specific queries."""
    
    table_name: str
    columns: Optional[list] = None
    filters: Dict[str, Any] = field(default_factory=dict)
    limit: Optional[int] = None
    order_by: Optional[str] = None
    
    def to_query_params(self) -> Dict[str, Any]:
        """Convert to parameters for QueryBuilder."""
        return {
            "table": self.table_name,
            "columns": self.columns or ['*'],
            "filters": self.filters,
            "limit": self.limit,
            "order_by": self.order_by
        }