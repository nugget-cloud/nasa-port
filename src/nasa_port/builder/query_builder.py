"""
Fluent query builder for constructing ADQL queries for the NASA Exoplanet Archive.
"""

from typing import List, Optional, Union, Any
import urllib.parse

from .models import TableName, DiscoveryMethod, SolutionType
from .spatial import SpatialConstraints


class QueryBuilder:
    """
    Fluent interface for building ADQL queries for the NASA Exoplanet Archive TAP service.
    
    Example usage:
        query = (QueryBuilder()
                .select(['pl_name', 'pl_masse', 'ra', 'dec'])
                .from_table(TableName.PLANETARY_SYSTEMS)
                .where('pl_masse > 0.5')
                .and_where('pl_masse < 2.0')
                .order_by('pl_masse', ascending=False)
                .build())
    """
    
    def __init__(self):
        """Initialize a new QueryBuilder."""
        self._select_columns: List[str] = []
        self._from_table: Optional[str] = None
        self._where_conditions: List[str] = []
        self._order_by_clause: Optional[str] = None
        self._limit_count: Optional[int] = None
        self._group_by_columns: List[str] = []
        self._having_condition: Optional[str] = None
    
    def select(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """
        Set the columns to select.
        
        Args:
            columns: Column name(s) to select. Use '*' for all columns.
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(columns, str):
            if columns == '*':
                self._select_columns = ['*']
            else:
                self._select_columns = [columns]
        else:
            self._select_columns = columns
        return self
    
    def from_table(self, table: Union[str, TableName]) -> 'QueryBuilder':
        """
        Set the table to query from.
        
        Args:
            table: Table name as string or TableName enum
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(table, TableName):
            self._from_table = table.value
        else:
            self._from_table = table
        return self
    
    def where(self, condition: str) -> 'QueryBuilder':
        """
        Add a WHERE condition.
        
        Args:
            condition: SQL condition string
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._where_conditions = [condition]  # Reset previous conditions
        return self
    
    def and_where(self, condition: str) -> 'QueryBuilder':
        """
        Add an AND WHERE condition.
        
        Args:
            condition: SQL condition string
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._where_conditions.append(f"AND {condition}")
        return self
    
    def or_where(self, condition: str) -> 'QueryBuilder':
        """
        Add an OR WHERE condition.
        
        Args:
            condition: SQL condition string
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._where_conditions.append(f"OR {condition}")
        return self
    
    def where_between(self, column: str, min_value: float, max_value: float) -> 'QueryBuilder':
        """
        Add a BETWEEN condition.
        
        Args:
            column: Column name
            min_value: Minimum value
            max_value: Maximum value
            
        Returns:
            QueryBuilder instance for method chaining
        """
        condition = f"{column} between {min_value} and {max_value}"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """
        Add an IN condition.
        
        Args:
            column: Column name
            values: List of values
            
        Returns:
            QueryBuilder instance for method chaining
        """
        # Format values for SQL
        formatted_values = []
        for value in values:
            if isinstance(value, str):
                formatted_values.append(f"'{value}'")
            else:
                formatted_values.append(str(value))
        
        values_str = ','.join(formatted_values)
        condition = f"{column} in ({values_str})"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_like(self, column: str, pattern: str, case_insensitive: bool = True) -> 'QueryBuilder':
        """
        Add a LIKE condition.
        
        Args:
            column: Column name
            pattern: Pattern to match (use % for wildcards)
            case_insensitive: Whether to use case-insensitive matching
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if case_insensitive:
            condition = f"upper({column}) like upper('{pattern}')"
        else:
            condition = f"{column} like '{pattern}'"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_confirmed(self) -> 'QueryBuilder':
        """
        Filter for confirmed planets only.
        
        Returns:
            QueryBuilder instance for method chaining
        """
        return self.where_like('soltype', '%CONF%')
    
    def where_candidates(self) -> 'QueryBuilder':
        """
        Filter for candidate planets only.
        
        Returns:
            QueryBuilder instance for method chaining
        """
        return self.where_like('soltype', '%CAND%')
    
    def where_discovery_method(self, method: Union[str, DiscoveryMethod]) -> 'QueryBuilder':
        """
        Filter by discovery method.
        
        Args:
            method: Discovery method as string or DiscoveryMethod enum
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(method, DiscoveryMethod):
            method_str = method.value
        else:
            method_str = method
            
        condition = f"discoverymethod = '{method_str}'"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_default_flag(self) -> 'QueryBuilder':
        """
        Filter for default solutions only.
        
        Returns:
            QueryBuilder instance for method chaining
        """
        condition = "default_flag=1"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_has_mass(self) -> 'QueryBuilder':
        """
        Filter for planets with mass measurements.
        
        Returns:
            QueryBuilder instance for method chaining
        """
        condition = "pl_masse > 0"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_has_radius(self) -> 'QueryBuilder':
        """
        Filter for planets with radius measurements.
        
        Returns:
            QueryBuilder instance for method chaining
        """
        condition = "pl_rade > 0"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_earth_sized(self, max_radius: float = 1.8) -> 'QueryBuilder':
        """
        Filter for Earth-sized planets.
        
        Args:
            max_radius: Maximum radius in Earth radii (default: 1.8)
            
        Returns:
            QueryBuilder instance for method chaining
        """
        condition = f"pl_rade <= {max_radius}"
        return self.and_where(condition) if self._where_conditions else self.where(condition)
    
    def where_spatial_circle(self, ra: float, dec: float, radius: float) -> 'QueryBuilder':
        """
        Add spatial constraint for circular region.
        
        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            radius: Radius in degrees
            
        Returns:
            QueryBuilder instance for method chaining
        """
        spatial_constraint = SpatialConstraints.circle(ra, dec, radius)
        return self.and_where(spatial_constraint) if self._where_conditions else self.where(spatial_constraint)
    
    def where_spatial_box(self, ra: float, dec: float, width: float, height: float) -> 'QueryBuilder':
        """
        Add spatial constraint for box region.
        
        Args:
            ra: Right ascension center in degrees
            dec: Declination center in degrees
            width: Box width in degrees
            height: Box height in degrees
            
        Returns:
            QueryBuilder instance for method chaining
        """
        spatial_constraint = SpatialConstraints.box(ra, dec, width, height)
        return self.and_where(spatial_constraint) if self._where_conditions else self.where(spatial_constraint)
    
    def order_by(self, column: str, ascending: bool = True) -> 'QueryBuilder':
        """
        Add ORDER BY clause.
        
        Args:
            column: Column to order by
            ascending: Whether to sort in ascending order (default: True)
            
        Returns:
            QueryBuilder instance for method chaining
        """
        direction = "ASC" if ascending else "DESC"
        self._order_by_clause = f"ORDER BY {column} {direction}"
        return self
    
    def limit(self, count: int) -> 'QueryBuilder':
        """
        Add LIMIT clause (Note: Not all TAP services support LIMIT).
        
        Args:
            count: Maximum number of records to return
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._limit_count = count
        return self
    
    def group_by(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """
        Add GROUP BY clause.
        
        Args:
            columns: Column(s) to group by
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(columns, str):
            self._group_by_columns = [columns]
        else:
            self._group_by_columns = columns
        return self
    
    def having(self, condition: str) -> 'QueryBuilder':
        """
        Add HAVING clause.
        
        Args:
            condition: Having condition
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._having_condition = condition
        return self
    
    def count(self, column: str = '*') -> 'QueryBuilder':
        """
        Select count of records.
        
        Args:
            column: Column to count (default: '*')
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._select_columns = [f"count({column}) as count"]
        return self
    
    def distinct(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """
        Select distinct values.
        
        Args:
            columns: Column(s) to select distinct values for
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(columns, str):
            columns = [columns]
        
        distinct_cols = [f"distinct({col})" if i == 0 else col for i, col in enumerate(columns)]
        self._select_columns = distinct_cols
        return self
    
    def build(self) -> str:
        """
        Build the final ADQL query string.
        
        Returns:
            Complete ADQL query string
            
        Raises:
            ValueError: If required components are missing
        """
        if not self._select_columns:
            raise ValueError("SELECT columns must be specified")
        
        if not self._from_table:
            raise ValueError("FROM table must be specified")
        
        # Build SELECT clause
        select_str = ','.join(self._select_columns)
        
        # Add TOP clause if limit is specified (ADQL standard)
        if self._limit_count:
            query_parts = [f"SELECT TOP {self._limit_count} {select_str}"]
        else:
            query_parts = [f"SELECT {select_str}"]
        
        # Add FROM clause
        query_parts.append(f"FROM {self._from_table}")
        
        # Add WHERE clause
        if self._where_conditions:
            # Remove leading AND/OR from first condition if present
            first_condition = self._where_conditions[0]
            if first_condition.startswith('AND '):
                first_condition = first_condition[4:]
            elif first_condition.startswith('OR '):
                first_condition = first_condition[3:]
            
            where_clause = first_condition
            if len(self._where_conditions) > 1:
                where_clause += ' ' + ' '.join(self._where_conditions[1:])
            
            query_parts.append(f"WHERE {where_clause}")
        
        # Add GROUP BY clause
        if self._group_by_columns:
            group_by_str = ','.join(self._group_by_columns)
            query_parts.append(f"GROUP BY {group_by_str}")
        
        # Add HAVING clause
        if self._having_condition:
            query_parts.append(f"HAVING {self._having_condition}")
        
        # Add ORDER BY clause
        if self._order_by_clause:
            query_parts.append(self._order_by_clause)
        
        # LIMIT clause is now handled in SELECT as TOP
        return ' '.join(query_parts)
    
    def to_url_encoded(self) -> str:
        """
        Build the query and return it URL-encoded for web requests.
        
        Returns:
            URL-encoded query string
        """
        query = self.build()
        # Replace spaces with + for URL encoding as per TAP documentation
        return query.replace(' ', '+')
    
    def __str__(self) -> str:
        """Return the built query string."""
        return self.build()
    
    def __repr__(self) -> str:
        """Return a representation of the QueryBuilder."""
        return f"QueryBuilder(select={self._select_columns}, from={self._from_table})"