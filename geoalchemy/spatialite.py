from sqlalchemy import Column, select, func, literal
from sqlalchemy.sql import expression
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.exc import NotSupportedError
from geoalchemy.base import SpatialElement, _to_gis, WKBSpatialElement, GeometryBase as Geometry
from geoalchemy.comparator import SFSComparator

class SQLiteSpatialElement(SpatialElement):
    """Represents a geometry value."""

    @property
    def __geom_from_wkb(self):
        if self.desc is not None and isinstance(self.desc, WKBSpatialElement):
            return func.GeomFromWKB(literal(self, Geometry), self.desc.srid)
        return literal(self, Geometry)

    @property
    def wkt(self):
        return func.AsText(self.__geom_from_wkb)

    @property
    def wkb(self):
        return func.AsBinary(self.__geom_from_wkb)

    @property
    def svg(self):
        return func.AsSVG(self.__geom_from_wkb)

    def fgf(self, precision=1):
        return func.AsFGF(self.__geom_from_wkb, precision)

    @property
    def dimension(self):
        return func.Dimension(self.__geom_from_wkb)

    @property
    def srid(self):
        return func.SRID(self.__geom_from_wkb)

    @property
    def geometry_type(self):
        return func.GeometryType(self.__geom_from_wkb)

    @property
    def is_empty(self):
        return func.IsEmpty(self.__geom_from_wkb)

    @property
    def is_simple(self):
        return func.IsSimple(self.__geom_from_wkb)

    @property
    def is_valid(self):
        return func.IsValid(self.__geom_from_wkb)

    @property
    def boundary(self):
        return func.Boundary(self.__geom_from_wkb)

    @property
    def x(self):
        return func.X(self.__geom_from_wkb)

    @property
    def y(self):
        return func.Y(self.__geom_from_wkb)

    @property
    def envelope(self):
        return func.Envelope(self.__geom_from_wkb)

    @property
    def start_point(self):
        return func.StartPoint(self.__geom_from_wkb)

    @property
    def end_point(self):
        return func.EndPoint(self.__geom_from_wkb)

    @property
    def length(self):
        return func.GLength(self.__geom_from_wkb)

    @property
    def is_closed(self):
        return func.IsClosed(self.__geom_from_wkb)

    @property
    def is_ring(self):
        return func.IsRing(self.__geom_from_wkb)

    @property
    def num_points(self):
        return func.NumPoints(self.__geom_from_wkb)

    def point_n(self, n=1):
        return func.PointN(self.__geom_from_wkb, n)

    @property
    def centroid(self):
        return func.Centroid(self.__geom_from_wkb)

    @property
    def area(self):
        return func.Area(self.__geom_from_wkb)

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

    # OGC Geometry Relations

    def equals(self, geom):
        return func.Equals(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def distance(self, geom):
        return func.Distance(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def within_distance(self, geom, distance=0.0):
        return func.DWithin(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom), distance)

    def disjoint(self, geom):
        return func.Disjoint(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def intersects(self, geom):
        return func.Intersects(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def touches(self, geom):
        return func.Touches(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def crosses(self, geom):
        return func.Crosses(self.__geom_from_wkb,
    		SFSComparator.geom_from_wkb(geom))

    def within(self, geom):
        return func.Within(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def overlaps(self, geom):
        return func.Overlaps(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def gcontains(self, geom):
        return func.Contains(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def covers(self, geom):
        return func.Covers(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def covered_by(self, geom):
        return func.CoveredBy(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    # OGC Geometry Relations based on Minimum Bounding Rectangle

    def mbr_within_distance(self, geom, distance=0.0):
        return func.MBRDWithin(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom), distance)

    def mbr_disjoint(self, geom):
        return func.MBRDisjoint(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_intersects(self, geom):
        return func.MBRIntersects(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_touches(self, geom):
        return func.MBRTouches(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_crosses(self, geom):
        return func.MBRCrosses(self.__geom_from_wkb,
    		SFSComparator.geom_from_wkb(geom))

    def mbr_within(self, geom):
        return func.MBRWithin(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_overlaps(self, geom):
        return func.MBROverlaps(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_contains(self, geom):
        return func.MBRContains(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_covers(self, geom):
        return func.MBRCovers(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))

    def mbr_covered_by(self, geom):
        return func.MBRCoveredBy(self.__geom_from_wkb,
			SFSComparator.geom_from_wkb(geom))



class SQLitePersistentSpatialElement(SQLiteSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc

