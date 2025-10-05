"""
Data models and constants for the NASA Exoplanet Archive SDK.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List


class TableName(Enum):
    """Available table names in the NASA Exoplanet Archive TAP service."""
    
    PLANETARY_SYSTEMS = "ps"
    PLANETARY_SYSTEMS_COMPOSITE = "pscomppars"
    TESS_TOI = "toi"
    MICROLENSING = "ml"
    STELLAR_HOSTS = "stellarhosts"
    KEPLER_NAMES = "keplernames"
    KEPLER_STELLAR = "keplerstellar"
    KEPLER_TIME_SERIES = "keplertimeseries"
    K2_NAMES = "k2names"
    K2_PLANETS_CANDIDATES = "k2pandc"
    K2_TARGETS = "k2targets"
    UKIRT_TIME_SERIES = "ukirttimeseries"
    KELT_TIME_SERIES = "kelttimeseries"
    SUPERWASP_TIME_SERIES = "superwasptimeseries"
    ATMOSPHERIC_SPECTROSCOPY = "spectra"
    HWO_EXEP_STARS = "di_stars_exep"
    TRANSITING_PLANETS = "TD"
    KOI_CUMULATIVE = "cumulative"
    KOI_Q1_Q6 = "q1_q6_koi"
    KOI_Q1_Q8 = "q1_q8_koi"
    KOI_Q1_Q12 = "q1_q12_koi"
    KOI_Q1_Q16 = "q1_q16_koi"
    KOI_Q1_Q17_DR24 = "q1_q17_dr24_koi"
    KOI_Q1_Q17_DR25 = "q1_q17_dr25_koi"
    KOI_Q1_Q17_DR25_SUP = "q1_q17_dr25_sup_koi"
    TCE_Q1_Q12 = "q1_q12_tce"
    TCE_Q1_Q16 = "q1_q16_tce"
    TCE_Q1_Q17_DR24 = "q1_q17_dr24_tce"
    TCE_Q1_Q17_DR25 = "q1_q17_dr25_tce"
    KEPLER_STELLAR_Q1_Q12 = "q1_q12_ks"
    KEPLER_STELLAR_Q1_Q16 = "q1_q16_ks"
    KEPLER_STELLAR_Q1_Q17_DR24 = "q1_q17_dr24_ks"
    KEPLER_STELLAR_Q1_Q17_DR25 = "q1_q17_dr25_ks"
    KEPLER_STELLAR_DR25_SUP = "q1_q17_dr25_sup_ks"


class OutputFormat(Enum):
    """Supported output formats for TAP queries."""
    
    VOTABLE = "votable"
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"


@dataclass
class QueryResponse:
    """Response from a TAP query."""
    
    data: Any
    format: OutputFormat
    url: str
    status_code: int
    headers: Dict[str, str]
    
    
@dataclass
class TableSchema:
    """Schema information for a table."""
    
    table_name: str
    columns: List[Dict[str, Any]]
    description: Optional[str] = None


@dataclass 
class ColumnInfo:
    """Information about a table column."""
    
    name: str
    data_type: str
    description: Optional[str] = None
    unit: Optional[str] = None
    width: Optional[int] = None


class DiscoveryMethod(Enum):
    """Common exoplanet discovery methods."""
    
    RADIAL_VELOCITY = "Radial Velocity"
    TRANSIT = "Transit"
    IMAGING = "Imaging"
    MICROLENSING = "Microlensing"
    ECLIPSE_TIMING_VARIATIONS = "Eclipse Timing Variations"
    ORBITAL_BRIGHTNESS_MODULATION = "Orbital Brightness Modulation"
    PULSAR_TIMING = "Pulsar Timing"
    PULSATION_TIMING_VARIATIONS = "Pulsation Timing Variations"
    ASTROMETRY = "Astrometry"


class SolutionType(Enum):
    """Solution types for planetary systems."""
    
    CONFIRMED = "CONFIRMED"
    CANDIDATE = "CANDIDATE"
    FALSE_POSITIVE = "FALSE POSITIVE"