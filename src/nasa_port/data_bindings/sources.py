"""
Data source definitions for NASA data pipelines.

This module defines DLT sources for various NASA datasets,
integrating with the existing query builders and client.
"""

from typing import Any, Dict, Iterator, List, Optional, Union
import dlt
from dlt.sources import DltResource

from ..builder.client import ExoplanetArchiveClient
from ..builder.query_builder import QueryBuilder
from ..builder.models import TableName, OutputFormat
from .transforms import DataTransformer, NASADataTransformer
from .config import QueryConfig


class BaseNASASource:
    """
    Base class for NASA data sources.
    
    Provides common functionality for querying NASA's Exoplanet Archive
    and yielding data in batches for DLT processing.
    """
    
    def __init__(
        self,
        client: Optional[ExoplanetArchiveClient] = None,
        transformer: Optional[DataTransformer] = None,
        batch_size: int = 1000
    ):
        """
        Initialize the base NASA source.
        
        Args:
            client: ExoplanetArchiveClient instance
            transformer: Data transformer for cleaning/normalizing data
            batch_size: Number of records per batch
        """
        self.client = client or ExoplanetArchiveClient()
        self.transformer = transformer or NASADataTransformer()
        self.batch_size = batch_size
    
    def _execute_query(self, query: Union[str, QueryBuilder]) -> List[Dict[str, Any]]:
        """
        Execute a query and return the results.
        
        Args:
            query: ADQL query string or QueryBuilder instance
            
        Returns:
            List of records from the query
        """
        response = self.client.query(query, OutputFormat.JSON)
        
        if isinstance(response.data, list):
            return response.data
        else:
            # Handle non-list responses (errors, etc.)
            print(f"Unexpected response format: {type(response.data)}")
            return []
    
    def _batch_records(self, records: List[Dict[str, Any]]) -> Iterator[List[Dict[str, Any]]]:
        """
        Yield records in batches.
        
        Args:
            records: List of all records
            
        Yields:
            Batches of records
        """
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            
            # Apply transformations if transformer is configured
            if self.transformer:
                batch = self.transformer.transform_batch(batch)
            
            yield batch


class ExoplanetSource(BaseNASASource):
    """
    DLT source for general exoplanet data from NASA's Exoplanet Archive.
    
    Provides access to the main planetary systems table with flexible
    querying capabilities.
    """
    
    def __init__(
        self,
        client: Optional[ExoplanetArchiveClient] = None,
        transformer: Optional[DataTransformer] = None,
        batch_size: int = 1000,
        default_columns: Optional[List[str]] = None
    ):
        """
        Initialize the exoplanet source.
        
        Args:
            client: ExoplanetArchiveClient instance
            transformer: Data transformer
            batch_size: Records per batch
            default_columns: Default columns to select
        """
        super().__init__(client, transformer, batch_size)
        
        self.default_columns = default_columns or [
            'pl_name', 'hostname', 'discoverymethod', 'disc_year',
            'pl_orbper', 'pl_orbsmax', 'pl_masse', 'pl_rade', 
            'pl_eqt', 'st_teff', 'st_rad', 'st_mass',
            'ra', 'dec', 'sy_dist', 'default_flag'
        ]
    
    def confirmed_planets(
        self, 
        limit: Optional[int] = None,
        additional_filters: Optional[Dict[str, Any]] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Get confirmed exoplanets.
        
        Args:
            limit: Maximum number of records to fetch
            additional_filters: Additional query filters
            
        Yields:
            Batches of confirmed planet records
        """
        query_builder = (self.client.create_query_builder()
                        .select(self.default_columns)
                        .from_table(TableName.PLANETARY_SYSTEMS)
                        .where_confirmed()
                        .where_default_flag())
        
        # Apply additional filters
        if additional_filters:
            for key, value in additional_filters.items():
                if isinstance(value, (list, tuple)):
                    query_builder = query_builder.where_in(key, value)
                else:
                    query_builder = query_builder.and_where(f"{key} = '{value}'")
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder)
        yield from self._batch_records(records)
    
    def candidate_planets(
        self,
        limit: Optional[int] = None,
        additional_filters: Optional[Dict[str, Any]] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Get candidate exoplanets.
        
        Args:
            limit: Maximum number of records to fetch
            additional_filters: Additional query filters
            
        Yields:
            Batches of candidate planet records
        """
        query_builder = (self.client.create_query_builder()
                        .select(self.default_columns)
                        .from_table(TableName.PLANETARY_SYSTEMS)
                        .where_candidates())
        
        # Apply additional filters
        if additional_filters:
            for key, value in additional_filters.items():
                if isinstance(value, (list, tuple)):
                    query_builder = query_builder.where_in(key, value)
                else:
                    query_builder = query_builder.and_where(f"{key} = '{value}'")
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder)
        yield from self._batch_records(records)
    
    def planets_with_mass(
        self,
        min_mass: float = 0.0,
        max_mass: Optional[float] = None,
        limit: Optional[int] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Get planets with mass measurements.
        
        Args:
            min_mass: Minimum planet mass in Earth masses
            max_mass: Maximum planet mass in Earth masses
            limit: Maximum number of records
            
        Yields:
            Batches of planet records with mass data
        """
        query_builder = (self.client.create_query_builder()
                        .select(self.default_columns)
                        .from_table(TableName.PLANETARY_SYSTEMS)
                        .where_has_mass()
                        .where_default_flag())
        
        if min_mass > 0:
            query_builder = query_builder.and_where(f"pl_masse >= {min_mass}")
        
        if max_mass:
            query_builder = query_builder.and_where(f"pl_masse <= {max_mass}")
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder)
        yield from self._batch_records(records)


class PlanetarySystemSource(BaseNASASource):
    """
    DLT source for planetary system data with system-level aggregations.
    """
    
    def systems_overview(self, limit: Optional[int] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Get overview of planetary systems.
        
        Args:
            limit: Maximum number of systems
            
        Yields:
            Batches of system overview records
        """
        columns = [
            'hostname', 'sy_snum', 'sy_pnum', 'sy_mnum',
            'st_teff', 'st_rad', 'st_mass', 'st_met',
            'sy_dist', 'ra', 'dec', 'default_flag'
        ]
        
        query_builder = (self.client.create_query_builder()
                        .select(columns)
                        .from_table(TableName.PLANETARY_SYSTEMS)
                        .where_default_flag()
                        .group_by(['hostname', 'sy_snum', 'sy_pnum', 'sy_mnum', 
                                  'st_teff', 'st_rad', 'st_mass', 'st_met',
                                  'sy_dist', 'ra', 'dec', 'default_flag']))
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder)
        yield from self._batch_records(records)


class TESSSource(BaseNASASource):
    """
    DLT source for TESS (Transiting Exoplanet Survey Satellite) data.
    """
    
    def tess_candidates(
        self,
        disposition: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Get TESS Objects of Interest (TOI).
        
        Args:
            disposition: TOI disposition filter (e.g., 'PC' for Planet Candidate)
            limit: Maximum number of records
            
        Yields:
            Batches of TESS candidate records
        """
        columns = [
            'toi', 'tic_id', 'toipfx', 'pl_name', 'hostname',
            'pl_orbper', 'pl_rade', 'pl_eqt', 'st_tmag',
            'ra', 'dec', 'tfopwg_disp'
        ]
        
        query_builder = (self.client.create_query_builder()
                        .select(columns)
                        .from_table(TableName.TESS_TOI))
        
        if disposition:
            query_builder = query_builder.where(f"tfopwg_disp = '{disposition}'")
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder) 
        yield from self._batch_records(records)


class KeplerSource(BaseNASASource):
    """
    DLT source for Kepler mission data.
    """
    
    def kepler_objects(
        self,
        disposition: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Get Kepler Objects of Interest (KOI).
        
        Args:
            disposition: KOI disposition (e.g., 'CONFIRMED', 'FALSE POSITIVE')
            limit: Maximum number of records
            
        Yields:
            Batches of Kepler object records
        """
        columns = [
            'kepid', 'kepoi_name', 'koi_disposition', 'koi_score',
            'koi_period', 'koi_prad', 'koi_teq', 'koi_slogg',
            'ra', 'dec'
        ]
        
        query_builder = (self.client.create_query_builder()
                        .select(columns)
                        .from_table(TableName.KOI_CUMULATIVE))
        
        if disposition:
            query_builder = query_builder.where(f"koi_disposition = '{disposition}'")
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder)
        yield from self._batch_records(records)


class MicrolensingSource(BaseNASASource):
    """
    DLT source for microlensing detection data.
    """
    
    def microlensing_events(self, limit: Optional[int] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Get microlensing planet discoveries.
        
        Args:
            limit: Maximum number of records
            
        Yields:
            Batches of microlensing event records
        """
        columns = [
            'pl_name', 'hostname', 'discoverymethod', 'disc_year',
            'pl_orbsmax', 'pl_masse', 'st_mass', 'sy_dist',
            'ra', 'dec'
        ]
        
        query_builder = (self.client.create_query_builder()
                        .select(columns)
                        .from_table(TableName.MICROLENSING))
        
        if limit:
            query_builder = query_builder.limit(limit)
        
        records = self._execute_query(query_builder)
        yield from self._batch_records(records)