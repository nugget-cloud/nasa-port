"""
Destination configurations for NASA data pipelines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional
import dlt
from dlt.destinations import duckdb, postgres, bigquery, filesystem


@dataclass
class DestinationConfig(ABC):
    """Abstract base class for destination configurations."""
    
    @abstractmethod
    def get_dlt_destination(self) -> Any:
        """Get the DLT destination object."""
        pass
    
    @abstractmethod
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for the destination."""
        pass


@dataclass
class DatabaseDestination(DestinationConfig):
    """Configuration for database destinations."""
    
    destination_type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    schema_name: Optional[str] = None
    
    def get_dlt_destination(self) -> Any:
        """Get DLT destination for databases."""
        if self.destination_type == "postgres":
            return dlt.destinations.postgres(
                credentials={
                    "host": self.host,
                    "port": self.port,
                    "database": self.database,
                    "username": self.username,
                    "password": self.password,
                }
            )
        elif self.destination_type == "duckdb":
            # For DuckDB, database is the file path
            return dlt.destinations.duckdb(credentials=self.database)
        else:
            raise ValueError(f"Unsupported database type: {self.destination_type}")
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
        }
    
    @classmethod
    def postgres(
        cls, 
        host: str, 
        database: str, 
        username: str, 
        password: str,
        port: int = 5432,
        schema_name: Optional[str] = None
    ) -> 'DatabaseDestination':
        """Create PostgreSQL destination configuration."""
        return cls(
            destination_type="postgres",
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema_name=schema_name
        )
    
    @classmethod
    def duckdb(cls, database_path: str) -> 'DatabaseDestination':
        """Create DuckDB destination configuration."""
        return cls(
            destination_type="duckdb",
            host="",  # Not used for DuckDB
            port=0,   # Not used for DuckDB
            database=database_path,
            username="",  # Not used for DuckDB
            password="",  # Not used for DuckDB
        )


@dataclass
class FileDestination(DestinationConfig):
    """Configuration for file-based destinations."""
    
    file_format: str  # parquet, csv, jsonl
    base_path: str
    compression: Optional[str] = None
    
    def get_dlt_destination(self) -> Any:
        """Get DLT filesystem destination."""
        return dlt.destinations.filesystem(
            bucket_url=self.base_path,
            file_format=self.file_format
        )
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters."""
        return {
            "base_path": self.base_path,
            "file_format": self.file_format,
            "compression": self.compression,
        }
    
    @classmethod
    def parquet(cls, base_path: str, compression: str = "snappy") -> 'FileDestination':
        """Create Parquet file destination."""
        return cls(
            file_format="parquet",
            base_path=base_path,
            compression=compression
        )
    
    @classmethod
    def csv(cls, base_path: str) -> 'FileDestination':
        """Create CSV file destination."""
        return cls(
            file_format="csv", 
            base_path=base_path
        )
    
    @classmethod
    def jsonl(cls, base_path: str, compression: Optional[str] = None) -> 'FileDestination':
        """Create JSONL file destination."""
        return cls(
            file_format="jsonl",
            base_path=base_path,
            compression=compression
        )


@dataclass  
class CloudDestination(DestinationConfig):
    """Configuration for cloud-based destinations."""
    
    provider: str  
    project_id: Optional[str] = None
    dataset_id: Optional[str] = None
    credentials_path: Optional[str] = None
    additional_params: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.additional_params is None:
            self.additional_params = {}
    
    def get_dlt_destination(self) -> Any:
        """Get DLT destination for cloud providers."""
        if self.provider == "bigquery":
            return dlt.destinations.bigquery(
                credentials=self.credentials_path,
                **self.additional_params
            )
        else:
            raise ValueError(f"Unsupported cloud provider: {self.provider}")
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters."""
        return {
            "provider": self.provider,
            "project_id": self.project_id,
            "dataset_id": self.dataset_id,
            "credentials_path": self.credentials_path,
            **self.additional_params
        }
    
    @classmethod
    def bigquery(
        cls,
        project_id: str,
        dataset_id: str,
        credentials_path: Optional[str] = None,
        **kwargs
    ) -> 'CloudDestination':
        """Create BigQuery destination configuration."""
        return cls(
            provider="bigquery",
            project_id=project_id,
            dataset_id=dataset_id,
            credentials_path=credentials_path,
            additional_params=kwargs
        )