from sqlalchemy import Column, select, func, literal
from sqlalchemy.sql import expression
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.exc import NotSupportedError
from geoalchemy.base import SpatialElement, _to_gis, GeometryBase as Geometry

class MySQLSpatialElement(SpatialElement):
    """Represents a geometry value."""

    @property
    def wkt(self):
        return func.AsText(literal(self, Geometry))

    @property
    def wkb(self):
        return func.AsBinary(literal(self, Geometry))

    @property
    def svg(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    def fgf(self, precision=1):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

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
        raise NotImplementedError("Current versions of MySQL do not support this operation yet.")

    @property
    def is_valid(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    @property
    def boundary(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

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
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    @property
    def num_points(self):
        return func.NumPoints(literal(self, Geometry))

    def point_n(self, n=1):
        return func.PointN(literal(self, Geometry), n)

    @property
    def centroid(self):
        raise NotImplementedError("At the moment MySQL does not support this operation.")

    @property
    def area(self):
        return func.Area(literal(self, Geometry))

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

    # OGC Geometry Relations

    def equals(self, geom):
        return func.Equals(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def distance(self, geom):
        raise NotImplementedError("At the moment MySQL does not support the Distance function.")

    def within_distance(self, geom, distance=0.0):
        return func.DWithin(literal(self, Geometry),
			literal(_to_gis(geom), Geometry), distance)

    def disjoint(self, geom):
        return func.Disjoint(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def intersects(self, geom):
        return func.Intersects(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def touches(self, geom):
        raise NotImplementedError("At the moment MySQL only supports MBR relations.")

    def crosses(self, geom):
        raise NotImplementedError("At the moment MySQL only supports MBR relations.")

    def within(self, geom):
        return func.Within(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def overlaps(self, geom):
        return func.Overlaps(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def gcontains(self, geom):
        return func.Contains(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def covers(self, geom):
        return func.Covers(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def covered_by(self, geom):
        return func.CoveredBy(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    # OGC Geometry Relations based on Minimum Bounding Rectangle

    def mbr_distance(self, geom):
        raise NotImplementedError("At the moment MySQL does not support the Distance function.")

    def mbr_within_distance(self, geom, distance=0.0):
        return func.MBRDWithin(literal(self, Geometry),
			literal(_to_gis(geom), Geometry), distance)

    def mbr_disjoint(self, geom):
        return func.MBRDisjoint(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def mbr_intersects(self, geom):
        return func.MBRIntersects(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def mbr_touches(self, geom):
        raise NotImplementedError("At the moment MySQL only supports MBR relations.")

    def mbr_within(self, geom):
        return func.MBRWithin(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def mbr_overlaps(self, geom):
        return func.MBROverlaps(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def mbr_contains(self, geom):
        return func.MBRContains(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def mbr_covers(self, geom):
        return func.MBRCovers(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))

    def mbr_covered_by(self, geom):
        return func.MBRCoveredBy(literal(self, Geometry),
			literal(_to_gis(geom), Geometry))


class MySQLPersistentSpatialElement(MySQLSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc

