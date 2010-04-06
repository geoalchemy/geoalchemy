from geoalchemy.base import SpatialComparator, PersistentSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy import functions
from sqlalchemy import func


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


# Functions only supported by MySQL
class mysql_functions:
    # MBREqual
    class mbr_equal(functions._relation_function):
        pass
    
    # MBRDisjoint
    class mbr_disjoint(functions._relation_function):
        pass
    
    # MBRIntersects
    class mbr_intersects(functions._relation_function):
        pass
    
    # MBRTouches
    class mbr_touches(functions._relation_function):
        pass
    
    # MBRWithin
    class mbr_within(functions._relation_function):
        pass
    
    # MBROverlaps
    class mbr_overlaps(functions._relation_function):
        pass
    
    # MBRContains
    class mbr_contains(functions._relation_function):
        pass
    
    @staticmethod
    def _within_distance(compiler, geom1, geom2, distance):
        """MySQL does not support the function distance, so we are doing
        a kind of "mbr_within_distance".
        The MBR of 'geom2' is expanded with the amount of 'distance' by
        manually changing the coordinates. Then we test if 'geom1' intersects
        this expanded MBR.
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
                           xmin - distance, ' ', ymin - distance, '))'), func.srid(geom2)
                    )                                              
                )


class MySQLSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for MySQL."""
    
    __functions = {
                   functions.length : 'GLength',
                   functions.is_simple : None,
                   functions.boundary : None,
                   functions.is_ring : None,
                   functions.centroid : None, 
                   functions.distance : None, # see also: http://bugs.mysql.com/bug.php?id=13600
                   functions.touches : None,
                   functions.crosses : None,
                   functions.transform : None,
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
    
    def get_comparator(self):
        return MySQLComparator
    
    def process_result(self, wkb_element):
        return MySQLPersistentSpatialElement(wkb_element)
    
    def handle_ddl_after_create(self, bind, table, column):
        if column.type.spatial_index:
            bind.execute("ALTER TABLE %s ADD %s %s NOT NULL" % 
                            (table.name, column.name, column.type.name))
            bind.execute("CREATE SPATIAL INDEX idx_%s_%s ON %s(%s)" % 
                            (table.name, column.name, table.name, column.name))
        else:
            bind.execute("ALTER TABLE %s ADD %s %s" % 
                            (table.name, column.name, column.type.name))
