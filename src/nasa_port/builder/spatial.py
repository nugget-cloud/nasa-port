from typing import List, Tuple, Union


class SpatialConstraints:
    """Helper class for creating spatial constraints in ADQL queries."""
    
    @staticmethod
    def circle(ra: float, dec: float, radius: float, coordinate_system: str = 'icrs') -> str:
        """
        Create a circular spatial constraint.
        
        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees  
            radius: Radius in degrees
            coordinate_system: Coordinate system (default: 'icrs')
            
        Returns:
            ADQL spatial constraint string for a circle
            
        Example:
            >>> SpatialConstraints.circle(217.42896, -62.67947, 0.1)
            "contains(point('icrs',ra,dec),circle('icrs',217.42896,-62.67947,0.1))=1"
        """
        return f"contains(point('{coordinate_system}',ra,dec),circle('{coordinate_system}',{ra},{dec},{radius}))=1"
    
    @staticmethod
    def box(ra: float, dec: float, width: float, height: float, coordinate_system: str = 'icrs') -> str:
        """
        Create a box (rectangle) spatial constraint.
        
        Args:
            ra: Right ascension center in degrees
            dec: Declination center in degrees
            width: Box width in degrees
            height: Box height in degrees  
            coordinate_system: Coordinate system (default: 'icrs')
            
        Returns:
            ADQL spatial constraint string for a box
            
        Example:
            >>> SpatialConstraints.box(217.42896, -62.67947, 0.1, 0.1)
            "contains(point('icrs',ra,dec),box('icrs',217.42896,-62.67947,0.1,0.1))=1"
        """
        return f"contains(point('{coordinate_system}',ra,dec),box('{coordinate_system}',{ra},{dec},{width},{height}))=1"
    
    @staticmethod  
    def polygon(coordinates: List[Tuple[float, float]], coordinate_system: str = 'icrs') -> str:
        """
        Create a polygon spatial constraint.
        
        Args:
            coordinates: List of (ra, dec) tuples defining polygon vertices
            coordinate_system: Coordinate system (default: 'icrs')
            
        Returns:
            ADQL spatial constraint string for a polygon
            
        Example:
            >>> coords = [(217., -62.), (218., -62.), (218., -63.), (217., -63.)]
            >>> SpatialConstraints.polygon(coords)
            "contains(point('icrs',ra,dec),polygon('icrs',217.0,-62.0,218.0,-62.0,218.0,-63.0,217.0,-63.0))=1"
        """
        if len(coordinates) < 3:
            raise ValueError("Polygon must have at least 3 vertices")
            
        coord_str = ','.join([f"{ra},{dec}" for ra, dec in coordinates])
        return f"contains(point('{coordinate_system}',ra,dec),polygon('{coordinate_system}',{coord_str}))=1"
    
    @staticmethod
    def near_object(ra: float, dec: float, radius: float = 0.1, coordinate_system: str = 'icrs') -> str:
        """
        Create a constraint to find objects near a specific coordinate.
        
        This is a convenience method that creates a circular constraint around a point.
        
        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            radius: Search radius in degrees (default: 0.1)
            coordinate_system: Coordinate system (default: 'icrs')
            
        Returns:
            ADQL spatial constraint string
        """
        return SpatialConstraints.circle(ra, dec, radius, coordinate_system)