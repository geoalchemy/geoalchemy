from geoalchemy.base import SpatialComparator, PersistentSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy import functions


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
                   mysql_functions.mbr_equal : 'MBREqual',
                   mysql_functions.mbr_disjoint : 'MBRDisjoint',
                   mysql_functions.mbr_intersects : 'MBRIntersects',
                   mysql_functions.mbr_touches : 'MBRTouches',
                   mysql_functions.mbr_within : 'MBRWithin',
                   mysql_functions.mbr_overlaps : 'MBROverlaps',
                   mysql_functions.mbr_contains : 'MBRContains'
                   
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
