from typing import Any, Dict, List, Optional, Union
import dlt
from dlt.pipeline import Pipeline
from dlt.sources import DltSource

from .sources import (
    ExoplanetSource,
    PlanetarySystemSource, 
    TESSSource,
    KeplerSource,
    MicrolensingSource
)
from .destinations import DestinationConfig
from .config import PipelineConfig, DestinationType
from .transforms import DataTransformer, NASADataTransformer
from ..builder.client import ExoplanetArchiveClient


class NASAPipeline:
    """
    Main pipeline class for NASA Exoplanet Archive data loading.
    
    Orchestrates data extraction from NASA's APIs using the query builders,
    applies transformations, and loads data to user-specified destinations.
    """
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize the NASA data pipeline.
        
        Args:
            config: Pipeline configuration specifying destination and options
        """
        self.config = config
        self.client = ExoplanetArchiveClient(timeout=config.timeout_seconds)
        self.transformer = NASADataTransformer()
        self._pipeline: Optional[Pipeline] = None
    
    def _get_destination(self) -> Any:
        """Get DLT destination based on configuration."""
        dest_type = self.config.destination_type
        params = self.config.destination_params
        
        if dest_type == DestinationType.DUCKDB:
            return dlt.destinations.duckdb(params.get("database_path", "nasa_data.duckdb"))
        elif dest_type == DestinationType.POSTGRES:
            return dlt.destinations.postgres(
                credentials={
                    "host": params["host"],
                    "port": params["port"],
                    "database": params["database"],
                    "username": params["username"],
                    "password": params["password"],
                }
            )
        elif dest_type == DestinationType.PARQUET:
            return dlt.destinations.filesystem(
                bucket_url=params.get("file_path", "./data"),
                file_format="parquet"
            )
        elif dest_type == DestinationType.CSV:
            return dlt.destinations.filesystem(
                bucket_url=params.get("file_path", "./data"),
                file_format="csv"
            )
        elif dest_type == DestinationType.JSONL:
            return dlt.destinations.filesystem(
                bucket_url=params.get("file_path", "./data"),
                file_format="jsonl"
            )
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
    
    def _create_pipeline(self) -> Pipeline:
        """Create and configure the DLT pipeline."""
        if self._pipeline is None:
            self._pipeline = dlt.pipeline(
                pipeline_name=self.config.pipeline_name,
                destination=self._get_destination(),
                dataset_name=self.config.schema_name or "nasa_data"
            )
        return self._pipeline
    
    def load_exoplanets(
        self,
        confirmed_only: bool = True,
        include_candidates: bool = False,
        limit: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Load exoplanet data.
        
        Args:
            confirmed_only: Whether to load only confirmed planets
            include_candidates: Whether to also load candidate planets
            limit: Maximum number of records per dataset
            filters: Additional query filters
            
        Returns:
            DLT load info
        """
        pipeline = self._create_pipeline()
        
        source = ExoplanetSource(
            client=self.client,
            transformer=self.transformer,
            batch_size=self.config.batch_size
        )

        resources = []
        
        if confirmed_only or not include_candidates:
            # Manually create a DLT resource from the generator method
            confirmed_planets_resource = dlt.resource(
                source.confirmed_planets(
                    limit=limit or self.config.max_records,
                    additional_filters=filters
                ),
                name="confirmed_planets",
                write_disposition="replace"
            )
            resources.append(confirmed_planets_resource)
        
        if include_candidates:
            candidate_planets_resource = dlt.resource(
                source.candidate_planets(
                    limit=limit or self.config.max_records,
                    additional_filters=filters
                ),
                name="candidate_planets",
                write_disposition="replace"
            )
            resources.append(candidate_planets_resource)
        
        if not resources:
            return None
            
        return pipeline.run(resources)
    
    def load_planetary_systems(self, limit: Optional[int] = None) -> Any:
        """
        Load planetary system overview data.
        
        Args:
            limit: Maximum number of systems to load
            
        Returns:
            DLT load info
        """
        pipeline = self._create_pipeline()
        
        source = PlanetarySystemSource(
            client=self.client,
            transformer=self.transformer,
            batch_size=self.config.batch_size
        )
        
        systems_overview_resource = dlt.resource(
            source.systems_overview(limit=limit or self.config.max_records),
            name="systems_overview",
            write_disposition="replace"
        )
        
        return pipeline.run(systems_overview_resource)
    
    def load_tess_data(
        self,
        disposition: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Any:
        """
        Load TESS mission data.
        
        Args:
            disposition: TOI disposition filter (e.g., 'PC' for Planet Candidate)
            limit: Maximum number of records
            
        Returns:
            DLT load info
        """
        pipeline = self._create_pipeline()
        
        source = TESSSource(
            client=self.client,
            transformer=self.transformer,
            batch_size=self.config.batch_size
        )
        
        tess_candidates_resource = dlt.resource(
            source.tess_candidates(
                disposition=disposition,
                limit=limit or self.config.max_records
            ),
            name="tess_candidates",
            write_disposition="replace"
        )
        
        return pipeline.run(tess_candidates_resource)
    
    def load_kepler_data(
        self,
        disposition: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Any:
        """
        Load Kepler mission data.
        
        Args:
            disposition: KOI disposition filter
            limit: Maximum number of records
            
        Returns:
            DLT load info
        """
        pipeline = self._create_pipeline()
        
        source = KeplerSource(
            client=self.client,
            transformer=self.transformer,
            batch_size=self.config.batch_size
        )
        
        kepler_objects_resource = dlt.resource(
            source.kepler_objects(
                disposition=disposition,
                limit=limit or self.config.max_records
            ),
            name="kepler_objects",
            write_disposition="replace"
        )
        
        return pipeline.run(kepler_objects_resource)
    
    def load_microlensing_data(self, limit: Optional[int] = None) -> Any:
        """
        Load microlensing discovery data.
        
        Args:
            limit: Maximum number of records
            
        Returns:
            DLT load info
        """
        pipeline = self._create_pipeline()
        
        source = MicrolensingSource(
            client=self.client,
            transformer=self.transformer,
            batch_size=self.config.batch_size
        )
        
        microlensing_events_resource = dlt.resource(
            source.microlensing_events(limit=limit or self.config.max_records),
            name="microlensing_events",
            write_disposition="replace"
        )
        
        return pipeline.run(microlensing_events_resource)
    
    def load_custom_query(
        self,
        query: Union[str, Any],  # QueryBuilder
        resource_name: str = "custom_data",
        limit: Optional[int] = None
    ) -> Any:
        """
        Load data from a custom query.
        
        Args:
            query: ADQL query string or QueryBuilder instance
            resource_name: Name for the resulting data resource
            limit: Maximum number of records
            
        Returns:
            DLT load info
        """
        pipeline = self._create_pipeline()
        
        # Create a custom resource from the query
        @dlt.resource(name=resource_name, write_disposition="replace")
        def custom_data():
            # Execute the query
            response = self.client.query(query)
            
            if isinstance(response.data, list):
                records = response.data
                
                # Apply limit if specified
                if limit:
                    records = records[:limit]
                
                # Apply transformations
                if self.transformer:
                    records = self.transformer.transform_batch(records)
                
                # Yield in batches
                batch_size = self.config.batch_size
                for i in range(0, len(records), batch_size):
                    yield records[i:i + batch_size]
        
        return pipeline.run(custom_data())
    
    def load_all_datasets(
        self,
        include_candidates: bool = False,
        limit_per_dataset: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Load all available NASA datasets.
        
        Args:
            include_candidates: Whether to include candidate planets
            limit_per_dataset: Limit per individual dataset
            
        Returns:
            Dictionary of load results for each dataset
        """
        results = {}
        
        # Load confirmed exoplanets
        results["exoplanets"] = self.load_exoplanets(
            confirmed_only=True,
            include_candidates=include_candidates,
            limit=limit_per_dataset
        )
        
        # Load planetary systems
        results["planetary_systems"] = self.load_planetary_systems(
            limit=limit_per_dataset
        )
        
        # Load TESS data
        results["tess"] = self.load_tess_data(limit=limit_per_dataset)
        
        # Load Kepler data
        results["kepler"] = self.load_kepler_data(limit=limit_per_dataset)
        
        # Load microlensing data
        results["microlensing"] = self.load_microlensing_data(limit=limit_per_dataset)
        
        return results
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Get information about the current pipeline.
        
        Returns:
            Pipeline configuration and status information
        """
        pipeline = self._create_pipeline()
        
        return {
            "pipeline_name": self.config.pipeline_name,
            "destination_type": self.config.destination_type.value,
            "destination_params": self.config.destination_params,
            "schema_name": pipeline.dataset_name,
            "pipeline_state": pipeline.state,
            "last_trace": pipeline.last_trace,
        }
    
    def drop_pipeline(self) -> None:
        """Drop the entire pipeline and all its data."""
        if self._pipeline:
            self._pipeline.drop()
            self._pipeline = None
    
    @classmethod
    def create_simple(
        cls,
        pipeline_name: str,
        destination_type: str = "duckdb",
        **destination_params
    ) -> 'NASAPipeline':
        """
        Create a simple pipeline with minimal configuration.
        
        Args:
            pipeline_name: Name of the pipeline
            destination_type: Type of destination (duckdb, postgres, parquet, etc.)
            **destination_params: Additional destination parameters
            
        Returns:
            Configured NASAPipeline instance
        """
        config = PipelineConfig(
            pipeline_name=pipeline_name,
            destination_type=DestinationType(destination_type),
            destination_params=destination_params
        )
        
        return cls(config)
    
    @classmethod
    def for_local_development(cls, pipeline_name: str, data_dir: str = "./data") -> 'NASAPipeline':
        """
        Create a pipeline configured for local development.
        
        Args:
            pipeline_name: Name of the pipeline
            data_dir: Directory to store data
            
        Returns:
            Development-configured NASAPipeline instance
        """
        config = PipelineConfig.for_local_development(pipeline_name, data_dir)
        return cls(config)