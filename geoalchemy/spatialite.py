from sqlalchemy import select, func

from geoalchemy.base import SpatialComparator, PersistentSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction
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


class sqlite_functions(mysql_functions):
    """Functions only supported by SQLite
    """
    
    class svg(BaseFunction):
        """AsSVG(g)"""
        pass
    
    class fgf(BaseFunction):
        """AsFGF(g)"""
        pass
    
    class is_valid(BaseFunction):
        """IsValid(g)"""
        pass


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
                   mysql_functions.mbr_contains : 'MBRContains'
                   }

    def _get_function_mapping(self):
        return SQLiteSpatialDialect.__functions
    
    def get_comparator(self):
        return SQLiteComparator
    
    def process_result(self, wkb_element):
        return SQLitePersistentSpatialElement(wkb_element)
    
    def handle_ddl_before_drop(self, bind, table, column):
        if column.type.spatial_index and SQLiteSpatialDialect.supports_rtree(bind.dialect):
            bind.execute(select([func.DisableSpatialIndex(table.name, column.name)]).execution_options(autocommit=True))
            bind.execute("DROP TABLE idx_%s_%s" % (table.name, column.name));
        
        bind.execute(select([func.DiscardGeometryColumn(table.name, column.name)]).execution_options(autocommit=True))
    
    def handle_ddl_after_create(self, bind, table, column):
        bind.execute(select([func.AddGeometryColumn(table.name, 
                                                    column.name, 
                                                    column.type.srid, 
                                                    column.type.name, 
                                                    column.type.dimension,
                                                    0 if column.nullable else 1)]).execution_options(autocommit=True))
        if column.type.spatial_index and SQLiteSpatialDialect.supports_rtree(bind.dialect):
            bind.execute("SELECT CreateSpatialIndex('%s', '%s')" % (table.name, column.name))
            bind.execute("VACUUM %s" % table.name)
    
    @staticmethod  
    def supports_rtree(dialect):
        # R-Tree index is only supported since SQLite version 3.6.0
        return dialect.server_version_info[0] > 3 or (dialect.server_version_info[0] == 3 and 
                                                      dialect.server_version_info[1] >= 6)
