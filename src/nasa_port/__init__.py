"""
NASA Exoplanet Archive Python SDK

A Python SDK for accessing NASA's Exoplanet Archive TAP (Table Access Protocol) service.
This package provides a fluent interface for building queries and retrieving astronomical data,
plus data pipeline functionality using DLT for ETL operations.
"""

from .builder.client import ExoplanetArchiveClient
from .builder.query_builder import QueryBuilder
from .builder.models import TableName, OutputFormat
from .builder.spatial import SpatialConstraints

# Import data_bindings for ETL functionality
try:
    from .data_bindings import (
        NASAPipeline,
        PipelineConfig,
        DestinationType,
        DataTransformer,
        NASADataTransformer,
        ExoplanetSource,
        PlanetarySystemSource,
        TESSSource,
        KeplerSource,
        MicrolensingSource,
        DatabaseDestination,
        FileDestination,
        CloudDestination
    )
    
    __all__ = [
        # Core query functionality
        "ExoplanetArchiveClient",
        "QueryBuilder", 
        "TableName",
        "OutputFormat",
        "SpatialConstraints",
        
        # Data pipeline functionality
        "NASAPipeline",
        "PipelineConfig", 
        "DestinationType",
        "DataTransformer",
        "NASADataTransformer",
        "ExoplanetSource",
        "PlanetarySystemSource", 
        "TESSSource",
        "KeplerSource",
        "MicrolensingSource",
        "DatabaseDestination",
        "FileDestination",
        "CloudDestination"
    ]
    
except ImportError as e:
    # If DLT or other dependencies are not available, only provide core functionality
    __all__ = [
        "ExoplanetArchiveClient",
        "QueryBuilder", 
        "TableName", 
        "OutputFormat",
        "SpatialConstraints",
    ]
    
    import warnings
    warnings.warn(
        f"Data bindings functionality not available due to missing dependencies: {e}. "
        "Install DLT and related packages to use data pipeline features.",
        ImportWarning
    )

__version__ = "0.1.0"
__author__ = "NASA Port SDK"
__email__ = ""