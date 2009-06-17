from sqlalchemy import Column, select, func, literal
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import expression
from geoalchemy.base import SpatialElement, Geometry, _to_dbms

# Python datatypes

class PGSpatialElement(SpatialElement):
    """Represents a geometry value."""

    # OGC Geometry Functions

    @property
    def wkt(self):
        return func.ST_AsText(literal(self, Geometry))

    @property
    def wkb(self):
        return func.ST_AsBinary(literal(self, Geometry))

    @property
    def svg(self):
        return func.ST_AsSVG(literal(self, Geometry))

    @property
    def gml(self):
        return func.ST_AsGML(literal(self, Geometry))

    @property
    def kml(self):
        return func.ST_AsKML(literal(self, Geometry))

    @property
    def geojson(self):
        return func.ST_AsGeoJson(literal(self, Geometry))

    @property
    def dimension(self):
        return func.ST_Dimension(literal(self, Geometry))

    @property
    def geometry_type(self):
        return func.ST_GeometryType(literal(self, Geometry))

    @property
    def is_empty(self):
        return func.ST_IsEmpty(literal(self, Geometry))

    @property
    def is_simple(self):
        return func.ST_IsSimple(literal(self, Geometry))

    @property
    def is_closed(self):
        return func.ST_IsClosed(literal(self, Geometry))

    @property
    def is_ring(self):
        return func.ST_IsRing(literal(self, Geometry))

    @property
    def length(self):
        return func.ST_Length(literal(self, Geometry))

    @property
    def area(self):
        return func.ST_Area(literal(self, Geometry))

    @property
    def centroid(self):
        return func.ST_Centroid(literal(self, Geometry))

    @property
    def boundary(self):
        return func.ST_Boundary(literal(self, Geometry))

    def buffer(self, length=0.0, num_segs=8):
        return func.ST_Buffer(literal(self, Geometry), length, num_segs)

    @property
    def convex_hull(self):
        return func.ST_ConvexHull(literal(self, Geometry))

    @property
    def envelope(self):
        return func.ST_Envelope(literal(self, Geometry))

    @property
    def start_point(self):
        return func.ST_StartPoint(literal(self, Geometry))

    @property
    def end_point(self):
        return func.ST_EndPoint(literal(self, Geometry))

    # OGC Geometry Relations

    def equals(self, geom):
        return func.ST_Equals(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def distance(self, geom):
        return func.ST_Distance(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def within_distance(self, geom, distance=0.0):
        return func.ST_DWithin(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry), distance)

    def disjoint(self, geom):
        return func.ST_Disjoint(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def intersects(self, geom):
        return func.ST_Intersects(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def touches(self, geom):
        return func.ST_Touches(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def crosses(self, geom):
        return func.ST_Crosses(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def within(self, geom):
        return func.ST_Within(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def overlaps(self, geom):
        return func.ST_Overlaps(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def contains(self, geom):
        return func.ST_Contains(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def covers(self, geom):
        return func.ST_Covers(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))

    def covered_by(self, geom):
        return func.ST_CoveredBy(literal(self, Geometry),
			literal(_to_dbms(geom), Geometry))


class PGPersistentSpatialElement(PGSpatialElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc


