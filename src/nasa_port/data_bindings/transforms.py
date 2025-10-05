"""
Data transformation utilities for NASA data pipelines.
"""

from typing import Any, Dict, List, Optional, Union, Callable
import re
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TransformRule:
    """Definition of a data transformation rule."""
    
    column: str
    transform_func: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None
    description: str = ""


class DataTransformer:
    """
    Data transformer for NASA exoplanet data.
    
    Provides common transformations for cleaning and normalizing
    NASA Exoplanet Archive data.
    """
    
    def __init__(self):
        """Initialize the transformer with default rules."""
        self.transform_rules: List[TransformRule] = []
        self._add_default_rules()
    
    def _add_default_rules(self):
        """Add default transformation rules for NASA data."""
        
        self.add_rule(
            column="*",  # Apply to all columns
            transform_func=self._normalize_column_name,
            description="Normalize column names to lowercase with underscores"
        )
        
        # Handle common null values
        self.add_rule(
            column="*",
            transform_func=self._handle_nulls,
            description="Convert various null representations to None"
        )
        
        self.add_rule(
            column="*",
            transform_func=self._convert_numeric,
            condition=lambda x: isinstance(x, str) and self._is_numeric_string(x),
            description="Convert numeric strings to numbers"
        )
    
    def add_rule(self, column: str, transform_func: Callable, condition: Optional[Callable] = None, description: str = ""):
        """
        Add a transformation rule.
        
        Args:
            column: Column name or '*' for all columns
            transform_func: Function to apply to the column value
            condition: Optional condition to check before applying transform
            description: Description of the transformation
        """
        rule = TransformRule(
            column=column,
            transform_func=transform_func,
            condition=condition,
            description=description
        )
        self.transform_rules.append(rule)
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single record according to the rules.
        
        Args:
            record: Input record as dictionary
            
        Returns:
            Transformed record
        """
        transformed = {}
        
        for original_key, value in record.items():
            key = self._normalize_column_name(original_key)
            transformed_value = value
            for rule in self.transform_rules:
                if rule.column == "*" or rule.column == key:
                    if rule.condition is None or rule.condition(transformed_value):
                        try:
                            transformed_value = rule.transform_func(transformed_value)
                        except Exception as e:
                            print(f"Transform error for {key}: {e}")
                            continue
            
            transformed[key] = transformed_value
        
        return transformed
    
    def transform_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform a batch of records.
        
        Args:
            records: List of records to transform
            
        Returns:
            List of transformed records
        """
        return [self.transform_record(record) for record in records]
    
    @staticmethod
    def _normalize_column_name(name: str) -> str:
        """Normalize column names to lowercase with underscores."""
        if not isinstance(name, str):
            return str(name)
        
        normalized = re.sub(r'[^\w]', '_', name.lower())
        normalized = re.sub(r'_+', '_', normalized)
        normalized = normalized.strip('_')
        return normalized
    
    @staticmethod
    def _handle_nulls(value: Any) -> Any:
        """Convert various null representations to None."""
        if value is None:
            return None
        
        if isinstance(value, str):
            null_values = {'', 'null', 'NULL', 'nan', 'NaN', 'N/A', 'n/a', '-', '--'}
            if value.strip() in null_values:
                return None
        
        return value
    
    @staticmethod
    def _is_numeric_string(value: str) -> bool:
        """Check if a string represents a number."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def _convert_numeric(value: Any) -> Any:
        """Convert numeric strings to appropriate number types."""
        if not isinstance(value, str):
            return value
        
        value = value.strip()
        if not value:
            return None
        
        try:
            if '.' not in value and 'e' not in value.lower():
                return int(value)
            else:
                return float(value)
        except ValueError:
            return value
    
    def add_unit_conversion(self, column: str, from_unit: str, to_unit: str, conversion_factor: float):
        """
        Add a unit conversion rule.
        
        Args:
            column: Column name to convert
            from_unit: Original unit
            to_unit: Target unit
            conversion_factor: Multiplication factor for conversion
        """
        def convert_units(value):
            if value is None or not isinstance(value, (int, float)):
                return value
            return value * conversion_factor
        
        self.add_rule(
            column=column,
            transform_func=convert_units,
            description=f"Convert {column} from {from_unit} to {to_unit}"
        )
    
    def add_date_parsing(self, column: str, date_format: str = None):
        """
        Add date parsing for a column.
        
        Args:
            column: Column name containing dates
            date_format: Expected date format (if None, tries to auto-detect)
        """
        def parse_date(value):
            if value is None or value == '':
                return None
            
            if isinstance(value, datetime):
                return value
            
            if not isinstance(value, str):
                return value
            
            try:
                if date_format:
                    return datetime.strptime(value, date_format)
                else:
                    # Try common formats
                    formats = [
                        '%Y-%m-%d',
                        '%Y/%m/%d', 
                        '%m/%d/%Y',
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S',
                    ]
                    
                    for fmt in formats:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    
                    return value  # Return original if no format matches
            except Exception:
                return value
        
        self.add_rule(
            column=column,
            transform_func=parse_date,
            description=f"Parse dates in {column}"
        )
    
    def add_categorical_mapping(self, column: str, mapping: Dict[str, Any]):
        """
        Add categorical value mapping.
        
        Args:
            column: Column name to map
            mapping: Dictionary mapping old values to new values
        """
        def map_categories(value):
            if value is None:
                return None
            
            # Handle case-insensitive mapping for strings
            if isinstance(value, str):
                for key, mapped_value in mapping.items():
                    if isinstance(key, str) and value.lower() == key.lower():
                        return mapped_value
            
            return mapping.get(value, value)
        
        self.add_rule(
            column=column,
            transform_func=map_categories,
            description=f"Map categorical values in {column}"
        )


class NASADataTransformer(DataTransformer):
    """
    Specialized transformer for NASA Exoplanet Archive data.
    
    Includes NASA-specific transformations and data cleaning rules.
    """
    
    def __init__(self):
        """Initialize with NASA-specific transformation rules."""
        super().__init__()
        self._add_nasa_specific_rules()
    
    def _add_nasa_specific_rules(self):
        """Add NASA-specific transformation rules."""
        
        # Convert Earth masses to Jupiter masses
        self.add_unit_conversion("pl_masse", "Earth masses", "Jupiter masses", 1/317.8)
        
        # Convert Earth radii to Jupiter radii  
        self.add_unit_conversion("pl_rade", "Earth radii", "Jupiter radii", 1/11.2)
        
        # Standardize discovery method names
        discovery_method_mapping = {
            "Transit": "transit",
            "Radial Velocity": "radial_velocity", 
            "Imaging": "direct_imaging",
            "Microlensing": "microlensing",
            "Eclipse Timing Variations": "eclipse_timing",
            "Orbital Brightness Modulation": "orbital_brightness",
        }
        
        self.add_categorical_mapping("discoverymethod", discovery_method_mapping)
        
        # Parse discovery dates
        self.add_date_parsing("disc_year")
        
        # Handle coordinate conversions (ensure numeric)
        for coord_col in ["ra", "dec", "glon", "glat"]:
            self.add_rule(
                column=coord_col,
                transform_func=lambda x: float(x) if x is not None and str(x).replace('.', '').replace('-', '').isdigit() else x,
                description=f"Ensure {coord_col} is numeric"
            )