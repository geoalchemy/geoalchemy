from sqlalchemy import select, func

from geoalchemy.base import SpatialComparator, PersistentSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy import functions
from geoalchemy.mysql import mysql_functions


class SQLiteComparator(SpatialComparator):
    """Comparator class used for Spatialite
    """
    def __getattr__(self, name):
        try:
            return SpatialComparator.__getattr__(self, name)
        except AttributeError:
            return getattr(sqlite_functions, name)(self)


class SQLitePersistentSpatialElement(PersistentSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc
        
    def __getattr__(self, name):
        try:
            return PersistentSpatialElement.__getattr__(self, name)
        except AttributeError:
            return getattr(sqlite_functions, name)(self)


# Functions only supported by SQLite
class sqlite_functions(mysql_functions):
    # AsSVG
    class svg(functions._base_function):
        pass
    
    # AsFGF
    class fgf(functions._function_with_argument):
        pass
    
    # IsValid
    class is_valid(functions._base_function):
        pass
    
    @staticmethod
    def _within_distance(compiler, geom1, geom2, distance):
        return func.Distance(geom1, geom2) <= distance


class SQLiteSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for SQLite."""
    
    __functions = { 
                   functions.within_distance : None,
                   functions.length : 'GLength',
                   sqlite_functions.svg : 'AsSVG',
                   sqlite_functions.fgf : 'AsFGF',
                   sqlite_functions.is_valid : 'IsValid',
                   mysql_functions.mbr_equal : 'MBREqual',
                   mysql_functions.mbr_disjoint : 'MBRDisjoint',
                   mysql_functions.mbr_intersects : 'MBRIntersects',
                   mysql_functions.mbr_touches : 'MBRTouches',
                   mysql_functions.mbr_within : 'MBRWithin',
                   mysql_functions.mbr_overlaps : 'MBROverlaps',
                   mysql_functions.mbr_contains : 'MBRContains',
                   functions._within_distance : sqlite_functions._within_distance
                   }

    def _get_function_mapping(self):
        return SQLiteSpatialDialect.__functions
    
    def get_comparator(self):
        return SQLiteComparator
    
    def process_result(self, wkb_element):
        return SQLitePersistentSpatialElement(wkb_element)
    
    def handle_ddl_before_drop(self, bind, table, column):
        bind.execute(select([func.DiscardGeometryColumn(table.name, column.name)]).execution_options(autocommit=True))
    
    def handle_ddl_after_create(self, bind, table, column):
        bind.execute(select([func.AddGeometryColumn(table.name, 
                                                    column.name, 
                                                    column.type.srid, 
                                                    column.type.name, 
                                                    column.type.dimension)]).execution_options(autocommit=True))
        if column.type.spatial_index:
            bind.execute("CREATE INDEX idx_%s_%s ON %s(%s)" % 
                            (table.name, column.name, table.name, column.name))
            bind.execute("VACUUM %s" % table.name)
