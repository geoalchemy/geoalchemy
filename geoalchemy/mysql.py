from sqlalchemy import func
from geoalchemy.base import SpatialComparator, PersistentSpatialElement,\
    WKBSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction


class MySQLComparator(SpatialComparator):
    """Comparator class used for MySQL
    """
    def __getattr__(self, name):
        try:
            return SpatialComparator.__getattr__(self, name)
        except AttributeError:
            return getattr(mysql_functions, name)(self)


class MySQLPersistentSpatialElement(PersistentSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc
        
    def __getattr__(self, name):
        try:
            return PersistentSpatialElement.__getattr__(self, name)
        except AttributeError:
            return getattr(mysql_functions, name)(self)


class mysql_functions(functions):
    """Functions only supported by MySQL
    """
    
    class mbr_equal(BaseFunction):
        """MBREqual(g1, g2)"""
        pass
    
    class mbr_disjoint(BaseFunction):
        """MBRDisjoint(g1, g2)"""
        pass
    
    class mbr_intersects(BaseFunction):
        """MBRIntersects(g1, g2)"""
        pass
    
    class mbr_touches(BaseFunction):
        """MBRTouches(g1, g2)"""
        pass
    
    class mbr_within(BaseFunction):
        """MBRWithin(g1, g2)"""
        pass
    
    class mbr_overlaps(BaseFunction):
        """MBROverlaps(g1, g2)"""
        pass
    
    class mbr_contains(BaseFunction):
        """MBRContains(g1, g2)"""
        pass

    @staticmethod
    def _within_distance(compiler, geom1, geom2, distance, *args):
        """MySQL does not support the function distance, so we are doing
        a kind of "mbr_within_distance". The MBR of 'geom2' is expanded with
        the amount of 'distance' by manually changing the coordinates.
        Then we test if 'geom1' intersects this expanded MBR.
        """
        mbr = func.ExteriorRing(func.Envelope(geom2))

        lower_left = func.StartPoint(mbr)
        upper_right = func.PointN(mbr, 3)

        xmin = func.X(lower_left)
        ymin = func.Y(lower_left)
        xmax = func.X(upper_right)
        ymax = func.Y(upper_right)

        return func.Intersects(
                geom1,
                func.GeomFromText(
                    func.Concat('Polygon((',
                           xmin - distance, ' ', ymin - distance, ',',
                           xmax + distance, ' ', ymin - distance, ',',
                           xmax + distance, ' ', ymax + distance, ',',
                           xmin - distance, ' ', ymax + distance, ',',
                           xmin - distance, ' ', ymin - distance, '))'),
                    func.srid(geom2)))

class MySQLSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for MySQL."""
    
    __functions = {
                   functions.length : 'GLength',
                   functions.is_valid : None,
                   functions.is_simple : None,
                   functions.boundary : None,
                   functions.is_ring : None,
                   functions.centroid : None, 
                   functions.distance : None, # see also: http://bugs.mysql.com/bug.php?id=13600
                   functions.touches : None,
                   functions.crosses : None,
                   functions.transform : None,
                   functions.buffer : None,
                   functions.convex_hull : None,
                   functions.intersection : None,
                   functions.within_distance : None,
                   mysql_functions.mbr_equal : 'MBREqual',
                   mysql_functions.mbr_disjoint : 'MBRDisjoint',
                   mysql_functions.mbr_intersects : 'MBRIntersects',
                   mysql_functions.mbr_touches : 'MBRTouches',
                   mysql_functions.mbr_within : 'MBRWithin',
                   mysql_functions.mbr_overlaps : 'MBROverlaps',
                   mysql_functions.mbr_contains : 'MBRContains',
                   functions._within_distance : mysql_functions._within_distance
                   }

    def _get_function_mapping(self):
        return MySQLSpatialDialect.__functions
    
    def process_result(self, value, type):
        return MySQLPersistentSpatialElement(WKBSpatialElement(value, type.srid))
    
    def handle_ddl_after_create(self, bind, table, column):
        if column.type.spatial_index or not column.nullable:
            # MySQL requires NOT NULL for spatial indexed columns
            bind.execute("ALTER TABLE %s ADD %s %s NOT NULL" % 
                            (table.name, column.name, column.type.name))
        else:
            bind.execute("ALTER TABLE %s ADD %s %s" % 
                            (table.name, column.name, column.type.name))
        
        if column.type.spatial_index:
            bind.execute("CREATE SPATIAL INDEX idx_%s_%s ON %s(%s)" % 
                            (table.name, column.name, table.name, column.name))
            
