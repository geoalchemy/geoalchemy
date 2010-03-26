from sqlalchemy import Column, select, func, literal
from sqlalchemy.sql import expression
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.exc import NotSupportedError
from geoalchemy.base import SpatialElement, _to_gis, WKTSpatialElement, WKBSpatialElement, GeometryBase as Geometry, SpatialComparator
from geoalchemy.comparator import SFSComparator, SFSComparatorFunctions
from geoalchemy.dialect import SpatialDialect 

class MySQLComparatorFunctions(SFSComparatorFunctions):
    """This class defines functions for MySQL that vary from SFS
    """

    @property
    def svg(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    def fgf(self, precision=1):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    @property
    def is_valid(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    @property
    def length(self):
        return func.GLength(self._geom_from_wkb())
    
    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)
    
    # SFS functions that are not implemented by MySQL
    @property
    def is_simple(self):
        raise NotImplementedError("Current versions of MySQL do not support this operation yet.")

    @property
    def boundary(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    @property
    def is_ring(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")
    
    @property
    def centroid(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")
    
    def distance(self, geom):
        # see also: http://bugs.mysql.com/bug.php?id=13600
        raise NotImplementedError("At the moment MySQL does not support the Distance function.")

    def touches(self, geom):
        raise NotImplementedError("At the moment MySQL only supports MBR relations.")

    def crosses(self, geom):
        raise NotImplementedError("At the moment MySQL only supports MBR relations.")    

    def mbr_distance(self, geom):
        raise NotImplementedError("At the moment MySQL does not support the Distance function.")


class MySQLComparator(MySQLComparatorFunctions, SpatialComparator):
    """Comparator class used for MySQL
    """
    pass

class MySQLSpatialElement(MySQLComparatorFunctions, SpatialElement):
    """Represents a geometry value."""
    pass

class MySQLPersistentSpatialElement(MySQLSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc


class MySQLSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for MySQL."""
    
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
