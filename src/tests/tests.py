import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from nasa_port import ExoplanetArchiveClient, QueryBuilder, TableName, OutputFormat
from nasa_port.builder.spatial import SpatialConstraints
from nasa_port.builder.query_builder import DiscoveryMethod


class TestQueryBuilder(unittest.TestCase):
    """Test the QueryBuilder class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.builder = QueryBuilder()
    
    def test_basic_select(self):
        """Test basic SELECT statement."""
        query = (self.builder
                 .select(['pl_name', 'pl_masse'])
                 .from_table(TableName.PLANETARY_SYSTEMS)
                 .build())
        
        expected = "SELECT pl_name,pl_masse FROM ps"
        self.assertEqual(query, expected)
    
    def test_select_all(self):
        """Test SELECT * statement."""
        query = (self.builder
                 .select('*')
                 .from_table('ps')
                 .build())
        
        expected = "SELECT * FROM ps"
        self.assertEqual(query, expected)
    
    def test_where_conditions(self):
        """Test WHERE conditions."""
        query = (self.builder
                 .select('*')
                 .from_table(TableName.PLANETARY_SYSTEMS)
                 .where('pl_masse > 0.5')
                 .and_where('pl_masse < 2.0')
                 .build())
        
        expected = "SELECT * FROM ps WHERE pl_masse > 0.5 AND pl_masse < 2.0"
        self.assertEqual(query, expected)
    
    def test_between_condition(self):
        """Test BETWEEN condition."""
        query = (self.builder
                 .select('pl_name')
                 .from_table('ps')
                 .where_between('pl_masse', 0.5, 2.0)
                 .build())
        
        expected = "SELECT pl_name FROM ps WHERE pl_masse between 0.5 and 2.0"
        self.assertEqual(query, expected)
    
    def test_order_by(self):
        """Test ORDER BY clause."""
        query = (self.builder
                 .select('*')
                 .from_table('ps')
                 .where('default_flag = 1')
                 .order_by('pl_masse', ascending=False)
                 .build())
        
        expected = "SELECT * FROM ps WHERE default_flag = 1 ORDER BY pl_masse DESC"
        self.assertEqual(query, expected)
    
    def test_count_query(self):
        """Test COUNT query."""
        query = (self.builder
                 .count('pl_name')
                 .from_table('ps')
                 .where('default_flag = 1')
                 .build())
        
        expected = "SELECT count(pl_name) as count FROM ps WHERE default_flag = 1"
        self.assertEqual(query, expected)
    
    def test_confirmed_planets(self):
        """Test confirmed planets filter."""
        query = (self.builder
                 .select('pl_name')
                 .from_table('ps')
                 .where_confirmed()
                 .build())
        
        # Should contain LIKE condition for confirmed planets
        self.assertIn("upper(soltype) like upper('%25CONF%25')", query)
    
    def test_discovery_method(self):
        """Test discovery method filter.""" 
        query = (self.builder
                 .select('pl_name')
                 .from_table('ps')
                 .where_discovery_method(DiscoveryMethod.TRANSIT)
                 .build())
        
        expected = "SELECT pl_name FROM ps WHERE discoverymethod = 'Transit'"
        self.assertEqual(query, expected)
    
    def test_missing_select(self):
        """Test error when SELECT is missing."""
        with self.assertRaises(ValueError):
            self.builder.from_table('ps').build()
    
    def test_missing_from(self):
        """Test error when FROM is missing."""
        with self.assertRaises(ValueError):
            self.builder.select('*').build()


class TestSpatialConstraints(unittest.TestCase):
    """Test the SpatialConstraints class."""
    
    def test_circle_constraint(self):
        """Test circular spatial constraint."""
        constraint = SpatialConstraints.circle(217.42896, -62.67947, 0.1)
        expected = "contains(point('icrs',ra,dec),circle('icrs',217.42896,-62.67947,0.1))=1"
        self.assertEqual(constraint, expected)
    
    def test_box_constraint(self):
        """Test box spatial constraint."""
        constraint = SpatialConstraints.box(217.42896, -62.67947, 0.1, 0.1)
        expected = "contains(point('icrs',ra,dec),box('icrs',217.42896,-62.67947,0.1,0.1))=1"
        self.assertEqual(constraint, expected)
    
    def test_polygon_constraint(self):
        """Test polygon spatial constraint."""
        coords = [(217., -62.), (218., -62.), (218., -63.), (217., -63.)]
        constraint = SpatialConstraints.polygon(coords)
        expected = "contains(point('icrs',ra,dec),polygon('icrs',217.0,-62.0,218.0,-62.0,218.0,-63.0,217.0,-63.0))=1"
        self.assertEqual(constraint, expected)
    
    def test_polygon_insufficient_vertices(self):
        """Test polygon with insufficient vertices."""
        coords = [(217., -62.), (218., -62.)]  # Only 2 points
        with self.assertRaises(ValueError):
            SpatialConstraints.polygon(coords)


class TestExoplanetArchiveClient(unittest.TestCase):
    """Test the ExoplanetArchiveClient class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.client = ExoplanetArchiveClient()
    
    def test_client_initialization(self):
        """Test client initialization."""
        self.assertIsInstance(self.client, ExoplanetArchiveClient)
        self.assertEqual(self.client.timeout, 30)
    
    def test_create_query_builder(self):
        """Test creating a query builder."""
        builder = self.client.create_query_builder()
        self.assertIsInstance(builder, QueryBuilder)
    
    # Note: We skip actual network tests to avoid dependencies on external services
    # In a real test suite, you might want to use mocking or test against a local service


if __name__ == '__main__':
    unittest.main()