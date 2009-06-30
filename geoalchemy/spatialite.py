from sqlalchemy import Column, select, func, literal
from sqlalchemy.sql import expression
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.exc import NotSupportedError
from geoalchemy.base import SpatialElement, _to_gis, GeometryBase as Geometry

# Python datatypes

class SQLiteSpatialElement(SpatialElement):
    """Represents a geometry value."""

    @property
    def wkt(self):
        return func.AsText(literal(self, Geometry))

    @property
    def wkb(self):
        return func.AsBinary(literal(self, Geometry))

    @property
    def svg(self):
        return func.AsSVG(literal(self, Geometry))

    def fgf(self, precision=1):
        return func.AsFGF(literal(self, Geometry), precision)

    @property
    def dimension(self):
        return func.Dimension(literal(self, Geometry))

    @property
    def srid(self):
        return func.SRID(literal(self, Geometry))

    @property
    def geometry_type(self):
        return func.GeometryType(literal(self, Geometry))

    @property
    def is_empty(self):
        return func.IsEmpty(literal(self, Geometry))

    @property
    def is_simple(self):
        return func.IsSimple(literal(self, Geometry))

    @property
    def is_valid(self):
        return func.IsValid(literal(self, Geometry))

    @property
    def boundary(self):
        return func.Boundary(literal(self, Geometry))

    @property
    def x(self):
        return func.X(literal(self, Geometry))

    @property
    def y(self):
        return func.Y(literal(self, Geometry))

    @property
    def envelope(self):
        return func.Envelope(literal(self, Geometry))

    @property
    def start_point(self):
        return func.StartPoint(literal(self, Geometry))

    @property
    def end_point(self):
        return func.EndPoint(literal(self, Geometry))

    @property
    def length(self):
        return func.GLength(literal(self, Geometry))

    @property
    def is_closed(self):
        return func.IsClosed(literal(self, Geometry))

    @property
    def is_ring(self):
        return func.IsRing(literal(self, Geometry))

    @property
    def num_points(self):
        return func.NumPoints(literal(self, Geometry))

    def point_n(self, n=1):
        return func.PointN(literal(self, Geometry), n)

    @property
    def centroid(self):
        return func.Centroid(literal(self, Geometry))

    @property
    def area(self):
        return func.Area(literal(self, Geometry))

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

class SQLitePersistentSpatialElement(SQLiteSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc

