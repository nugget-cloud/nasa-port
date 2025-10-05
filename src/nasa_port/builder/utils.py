"""
Utility functions for the NASA Exoplanet Archive SDK.
"""

import urllib.parse
from typing import Dict, Any, List, Optional


def url_encode_query(query: str) -> str:
    """
    URL encode a query string for TAP requests.
    
    Args:
        query: ADQL query string
        
    Returns:
        URL-encoded query string
    """
    # Replace spaces with + as per NASA TAP documentation
    query = query.replace(' ', '+')
    # URL encode special characters
    return urllib.parse.quote(query, safe='+,=')


def format_coordinate(coord: float, precision: int = 6) -> str:
    """
    Format a coordinate value for astronomical queries.
    
    Args:
        coord: Coordinate value in degrees
        precision: Decimal precision (default: 6)
        
    Returns:
        Formatted coordinate string
    """
    return f"{coord:.{precision}f}"


def parse_votable_columns(votable_content: str) -> List[Dict[str, Any]]:
    """
    Parse column information from VOTable XML content.
    
    Args:
        votable_content: VOTable XML content
        
    Returns:
        List of column information dictionaries
    """
    import xml.etree.ElementTree as ET
    
    try:
        root = ET.fromstring(votable_content)
        
        # Find FIELD elements which contain column information
        columns = []
        for field in root.iter():
            if field.tag.endswith('FIELD'):
                column_info = {
                    'name': field.get('name', ''),
                    'datatype': field.get('datatype', ''),
                    'unit': field.get('unit', ''),
                    'description': ''
                }
                
                # Look for DESCRIPTION child element
                desc_elem = field.find('.//*[local-name()="DESCRIPTION"]')
                if desc_elem is not None and desc_elem.text:
                    column_info['description'] = desc_elem.text.strip()
                
                columns.append(column_info)
        
        return columns
        
    except ET.ParseError:
        return []


def build_constraint_list(*conditions: str) -> str:
    """
    Build a list of WHERE conditions joined with AND.
    
    Args:
        *conditions: Variable number of condition strings
        
    Returns:
        Combined WHERE clause
    """
    valid_conditions = [cond.strip() for cond in conditions if cond and cond.strip()]
    if not valid_conditions:
        return ""
    
    return " AND ".join(valid_conditions)


def escape_sql_string(value: str) -> str:
    """
    Escape a string value for use in SQL queries.
    
    Args:
        value: String value to escape
        
    Returns:
        Escaped string value
    """
    # Escape single quotes by doubling them
    return value.replace("'", "''")


def format_number_range(min_val: Optional[float], max_val: Optional[float]) -> str:
    """
    Format a number range condition for SQL queries.
    
    Args:
        min_val: Minimum value (can be None)
        max_val: Maximum value (can be None)
        
    Returns:
        SQL condition string
    """
    conditions = []
    
    if min_val is not None:
        conditions.append(f">= {min_val}")
    
    if max_val is not None:
        conditions.append(f"<= {max_val}")
    
    return " AND ".join(conditions)


def validate_table_name(table_name: str, valid_tables: Optional[List[str]] = None) -> bool:
    """
    Validate a table name.
    
    Args:
        table_name: Table name to validate
        valid_tables: List of valid table names (optional)
        
    Returns:
        True if valid, False otherwise
    """
    if not table_name or not isinstance(table_name, str):
        return False
    
    # Basic validation - table name should not contain suspicious characters
    forbidden_chars = [';', '--', '/*', '*/', 'drop', 'delete', 'update', 'insert']
    table_lower = table_name.lower()
    
    for char in forbidden_chars:
        if char in table_lower:
            return False
    
    # Check against valid tables list if provided
    if valid_tables:
        return table_name in valid_tables
    
    return True


def parse_coordinate_string(coord_str: str) -> Optional[float]:
    """
    Parse coordinate string to float value.
    
    Args:
        coord_str: Coordinate string (e.g., "217.42896", "14h 29m 42.95s")
        
    Returns:
        Coordinate value in degrees or None if parsing fails
    """
    try:
        # Try direct float conversion first
        return float(coord_str)
    except ValueError:
        pass
    
    # TODO: Add parsing for sexagesimal coordinates (HMS/DMS format)
    # This would require more complex parsing logic
    
    return None


def format_table_response(data: Any, format_type: str) -> str:
    """
    Format response data for display.
    
    Args:
        data: Response data
        format_type: Format type ('json', 'csv', 'tsv', 'table')
        
    Returns:
        Formatted string representation
    """
    if format_type == 'json':
        import json
        return json.dumps(data, indent=2)
    
    elif format_type == 'csv' and isinstance(data, list):
        if not data:
            return ""
        
        import csv
        from io import StringIO
        
        output = StringIO()
        if isinstance(data[0], dict):
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        return output.getvalue()
    
    elif format_type == 'table' and isinstance(data, list):
        if not data:
            return "No data"
        
        # Simple table formatting
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            
            # Calculate column widths
            col_widths = {}
            for header in headers:
                col_widths[header] = len(header)
                for row in data:
                    if header in row and row[header] is not None:
                        col_widths[header] = max(col_widths[header], len(str(row[header])))
            
            # Build table
            lines = []
            
            # Header
            header_line = " | ".join(header.ljust(col_widths[header]) for header in headers)
            lines.append(header_line)
            lines.append("-" * len(header_line))
            
            # Rows
            for row in data:
                row_line = " | ".join(
                    str(row.get(header, "")).ljust(col_widths[header]) 
                    for header in headers
                )
                lines.append(row_line)
            
            return "\n".join(lines)
    
    return str(data)