import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Union
import json
import csv
from io import StringIO

from src.nasa_port.builder.models import OutputFormat, QueryResponse, TableSchema
from src.nasa_port.builder.query_builder import QueryBuilder


class ExoplanetArchiveError(Exception):
    pass

class ExoplanetArchiveClient:
    """
    Client for accessing NASA's Exoplanet Archive TAP service.
    
    This client provides methods for executing synchronous and asynchronous queries
    against the NASA Exoplanet Archive database using the Table Access Protocol (TAP).
    """
    
    BASE_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP"
    SYNC_URL = f"{BASE_URL}/sync"
    ASYNC_URL = f"{BASE_URL}/async"
    TABLES_URL = f"{BASE_URL}/tables"
    
    def __init__(self, timeout: int = 30):
        """
        Initialize the ExoplanetArchive client.
        
        Args:
            timeout: Request timeout in seconds (default: 30)
        """
        self.timeout = timeout
    
    def query(
        self,
        query: Union[str, QueryBuilder],
        output_format: OutputFormat = OutputFormat.JSON,
        async_query: bool = False
    ) -> QueryResponse:
        """
        Execute a TAP query against the Exoplanet Archive.
        
        Args:
            query: ADQL query string or QueryBuilder instance
            output_format: Desired output format (default: JSON)
            async_query: Whether to execute as asynchronous query (default: False)
            
        Returns:
            QueryResponse containing the results
            
        Raises:
            ExoplanetArchiveError: If the query fails
        """
        if isinstance(query, QueryBuilder):
            query_str = query.build()
        else:
            query_str = query
            
        return self._execute_query(query_str, output_format, async_query)
    
    def _execute_query(
        self,
        query: str,
        output_format: OutputFormat,
        async_query: bool = False
    ) -> QueryResponse:
        """Execute the actual HTTP request to the TAP service."""
        
        endpoint = self.ASYNC_URL if async_query else self.SYNC_URL
        
        params = {
            "query": query,
            "format": output_format.value
        }
        
        encoded_params = urllib.parse.urlencode(params)
        url = f"{endpoint}?{encoded_params}"
        
        try:
            request = urllib.request.Request(url)
            request.add_header("User-Agent", "NASA-Port-SDK/0.1.0")
            
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                content = response.read().decode('utf-8')
                headers = dict(response.headers)
                status_code = response.getcode()
                parsed_data = self._parse_response(content, output_format)
                
                return QueryResponse(
                    data=parsed_data,
                    format=output_format,
                    url=url,
                    status_code=status_code,
                    headers=headers
                )
                
        except urllib.error.HTTPError as e:
            error_msg = f"HTTP Error {e.code}: {e.reason}"
            if hasattr(e, 'read'):
                try:
                    error_content = e.read().decode('utf-8')
                    error_msg += f"\nDetails: {error_content}"
                except:
                    pass
            raise ExoplanetArchiveError(error_msg) from e
            
        except urllib.error.URLError as e:
            raise ExoplanetArchiveError(f"Connection error: {e.reason}") from e
            
        except Exception as e:
            raise ExoplanetArchiveError(f"Unexpected error: {str(e)}") from e
    
    def _parse_response(self, content: str, output_format: OutputFormat) -> Any:
        if output_format == OutputFormat.JSON:
            try:
                parsed = json.loads(content)
                # Return parsed data as-is - it should already be in the correct format
                return parsed
            except json.JSONDecodeError as e:
                # If JSON parsing fails, check if content looks like an error message
                if "ERROR" in content or "VOTABLE" in content:
                    # This is likely an error response, re-raise as ExoplanetArchiveError
                    raise ExoplanetArchiveError(f"Query failed: {content}")
                # Debug: print first 200 chars of content to understand the format
                print(f"DEBUG: JSON parse failed. Content preview: {content[:200]}...")
                # Otherwise return the raw content for debugging
                return content
            
        elif output_format == OutputFormat.CSV:
            reader = csv.DictReader(StringIO(content))
            return list(reader)
            
        elif output_format == OutputFormat.TSV:
            reader = csv.DictReader(StringIO(content), delimiter='\t')
            return list(reader)
            
        elif output_format == OutputFormat.VOTABLE:
            # For VOTable, return raw XML content
            return content
            
        else:
            return content
    
    def get_table_schema(self, table_name: Optional[str] = None) -> Union[TableSchema, Dict[str, Any]]:
        """
        Retrieve schema information for tables.
        
        Args:
            table_name: Specific table name to get schema for. If None, returns all tables.
            
        Returns:
            TableSchema for specific table or dict of all table information
        """
        try:
            if table_name:
                query = f"select * from TAP_SCHEMA.columns where table_name like '{table_name}'"
                response = self._execute_query(query, OutputFormat.JSON)
                
                columns = []
                if isinstance(response.data, list):
                    columns = response.data
                
                return TableSchema(
                    table_name=table_name,
                    columns=columns
                )
            else:
                request = urllib.request.Request(self.TABLES_URL)
                request.add_header("User-Agent", "NASA-Port-SDK/0.1.0")
                
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    content = response.read().decode('utf-8')
                    return {"xml_content": content}
                    
        except Exception as e:
            raise ExoplanetArchiveError(f"Failed to retrieve schema: {str(e)}") from e
    
    def list_tables(self) -> list:
        """
        Get a list of all available tables.
        
        Returns:
            List of table names and information
        """
        query = "select schema_name, table_name from TAP_SCHEMA.tables"
        response = self.query(query, OutputFormat.JSON)
        return response.data
    
    def count_records(self, table_name: str, where_clause: Optional[str] = None) -> int:
        """
        Count records in a table with optional constraints.
        
        Args:
            table_name: Name of the table to count
            where_clause: Optional WHERE clause constraints
            
        Returns:
            Number of records
        """
        query = f"select count(*) as count from {table_name}"
        if where_clause:
            query += f" where {where_clause}"
            
        response = self.query(query, OutputFormat.JSON)
        
        if isinstance(response.data, list) and len(response.data) > 0:
            return int(response.data[0].get('count', 0))
        
        return 0
    
    def create_query_builder(self) -> QueryBuilder:
        """
        Create a new QueryBuilder instance.
        
        Returns:
            QueryBuilder for fluent query construction
        """
        return QueryBuilder()