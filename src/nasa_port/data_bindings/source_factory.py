"""
DLT source factory functions for NASA data bindings.

This module provides factory functions to create properly configured
DLT sources from the NASA source classes.
"""

from typing import Optional, List
import dlt
from dlt.sources import DltSource

from ..builder.client import ExoplanetArchiveClient
from .transforms import DataTransformer, NASADataTransformer
from .sources import (
    ExoplanetSource,
    PlanetarySystemSource,
    TESSSource,
    KeplerSource,
    MicrolensingSource
)


def exoplanets_source(
    client: Optional[ExoplanetArchiveClient] = None,
    transformer: Optional[DataTransformer] = None,
    batch_size: int = 1000,
    default_columns: Optional[List[str]] = None
) -> DltSource:
    """
    Create an exoplanets DLT source.
    
    Args:
        client: ExoplanetArchiveClient instance
        transformer: Data transformer
        batch_size: Records per batch
        default_columns: Default columns to select
        
    Returns:
        DLT source for exoplanet data
    """
    source_instance = ExoplanetSource(client, transformer, batch_size, default_columns)
    @dlt.source(name="exoplanets")
    def _exoplanets_source():
        yield source_instance.confirmed_planets
        yield source_instance.candidate_planets
        yield source_instance.planets_with_mass
    
    return _exoplanets_source()


def planetary_systems_source(
    client: Optional[ExoplanetArchiveClient] = None,
    transformer: Optional[DataTransformer] = None,
    batch_size: int = 1000
) -> DltSource:
    """
    Create a planetary systems DLT source.
    
    Args:
        client: ExoplanetArchiveClient instance
        transformer: Data transformer  
        batch_size: Records per batch
        
    Returns:
        DLT source for planetary systems data
    """
    source_instance = PlanetarySystemSource(client, transformer, batch_size)
    
    @dlt.source(name="planetary_systems")
    def _planetary_systems_source():
        yield source_instance.systems_overview
    
    return _planetary_systems_source()


def tess_source(
    client: Optional[ExoplanetArchiveClient] = None,
    transformer: Optional[DataTransformer] = None,
    batch_size: int = 1000
) -> DltSource:
    """
    Create a TESS DLT source.
    
    Args:
        client: ExoplanetArchiveClient instance
        transformer: Data transformer
        batch_size: Records per batch
        
    Returns:
        DLT source for TESS data
    """
    source_instance = TESSSource(client, transformer, batch_size)
    
    @dlt.source(name="tess_data")
    def _tess_source():
        yield source_instance.tess_candidates
    
    return _tess_source()


def kepler_source(
    client: Optional[ExoplanetArchiveClient] = None,
    transformer: Optional[DataTransformer] = None,
    batch_size: int = 1000
) -> DltSource:
    """
    Create a Kepler DLT source.
    
    Args:
        client: ExoplanetArchiveClient instance
        transformer: Data transformer
        batch_size: Records per batch
        
    Returns:
        DLT source for Kepler data
    """
    source_instance = KeplerSource(client, transformer, batch_size)
    
    @dlt.source(name="kepler_data")
    def _kepler_source():
        yield source_instance.kepler_objects
    
    return _kepler_source()


def microlensing_source(
    client: Optional[ExoplanetArchiveClient] = None,
    transformer: Optional[DataTransformer] = None,
    batch_size: int = 1000
) -> DltSource:
    """
    Create a microlensing DLT source.
    
    Args:
        client: ExoplanetArchiveClient instance
        transformer: Data transformer
        batch_size: Records per batch
        
    Returns:
        DLT source for microlensing data
    """
    source_instance = MicrolensingSource(client, transformer, batch_size)
    
    @dlt.source(name="microlensing_data")
    def _microlensing_source():
        yield source_instance.microlensing_events
    
    return _microlensing_source()