from sqlalchemy import Column, select, func, literal
from sqlalchemy.sql import expression
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.exc import NotSupportedError
from geoalchemy.base import SpatialElement, _to_gis, WKBSpatialElement, GeometryBase as Geometry, SpatialComparator, PersistentSpatialElement
from geoalchemy.comparator import SFSComparator, SFSComparatorFunctions
from geoalchemy.dialect import SpatialDialect 


class SQLiteComparatorFunctions(SFSComparatorFunctions):
    """This class defines functions for Spatialite that vary from SFS
    """

    @property
    def svg(self):
        return func.AsSVG(self._parse_clause())

    def fgf(self, precision=1):
        return func.AsFGF(self._parse_clause(), precision)

    @property
    def is_valid(self):
        return func.IsValid(self._parse_clause())

    @property
    def length(self):
        return func.GLength(self._parse_clause())

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

    # SFS functions that are not implemented by Spatialite
    def mbr_distance(self, other):
        raise NotImplementedError("At the moment Spatialite does not support the MBRDistance function.")

class SQLiteComparator(SQLiteComparatorFunctions, SpatialComparator):
    """Comparator class used for Spatialite
    """
    pass

class SQLiteSpatialElement(SQLiteComparatorFunctions, SpatialElement):
    """Represents a geometry value."""
    pass

class SQLitePersistentSpatialElement(SQLiteSpatialElement, PersistentSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc


class SQLiteSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for SQLite."""
    
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
