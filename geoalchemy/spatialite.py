from sqlalchemy import Column, select, func, literal
from sqlalchemy.sql import expression
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.exc import NotSupportedError
from geoalchemy.base import SpatialElement, _to_gis, WKBSpatialElement, GeometryBase as Geometry
from geoalchemy.comparator import SFSComparator, SFSComparatorFunctions


class SQLiteComparatorFunctions(SFSComparatorFunctions):
    """This class defines functions for Spatialite that vary from SFS
    """

    @property
    def svg(self):
        return func.AsSVG(self._geom_from_wkb())

    def fgf(self, precision=1):
        return func.AsFGF(self._geom_from_wkb(), precision)

    @property
    def is_valid(self):
        return func.IsValid(self._geom_from_wkb())

    @property
    def length(self):
        return func.GLength(self._geom_from_wkb())

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

    # SFS functions that are not implemented by Spatialite
    def mbr_distance(self, other):
        raise NotImplementedError("At the moment Spatialite does not support the MBRDistance function.")

class SQLiteComparator(SQLiteComparatorFunctions):
    """Comparator class used for Spatialite
    """
    pass

class SQLiteSpatialElement(SQLiteComparatorFunctions, SpatialElement):
    """Represents a geometry value."""
    pass

class SQLitePersistentSpatialElement(SQLiteSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc
