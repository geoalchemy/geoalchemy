# -*- coding: utf-8 -*-
from sqlalchemy import Column, select, func, literal
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import expression
from geoalchemy.base import SpatialElement, _to_gis, WKBSpatialElement, GeometryBase as Geometry
from geoalchemy.comparator import SQLMMComparator

class PGSpatialElement(SpatialElement):
    """Represents a geometry value."""

    @property
    def __geom_from_wkb(self):
        if self.desc is not None and isinstance(self.desc, WKBSpatialElement):
            return func.ST_GeomFromWKB(literal(self, Geometry), self.desc.srid)
        return literal(self, Geometry)

    # OGC Geometry Functions

    @property
    def wkt(self):
        return func.ST_AsText(self.__geom_from_wkb)

    @property
    def wkb(self):
        return func.ST_AsBinary(self.__geom_from_wkb)

    @property
    def svg(self):
        return func.ST_AsSVG(self.__geom_from_wkb)

    @property
    def gml(self):
        return func.ST_AsGML(self.__geom_from_wkb)

    @property
    def kml(self):
        return func.ST_AsKML(self.__geom_from_wkb)

    @property
    def geojson(self):
        return func.ST_AsGeoJson(self.__geom_from_wkb)

    @property
    def dimension(self):
        return func.ST_Dimension(self.__geom_from_wkb)

    @property
    def srid(self):
        return func.ST_SRID(self.__geom_from_wkb)

    @property
    def geometry_type(self):
        return func.ST_GeometryType(self.__geom_from_wkb)

    @property
    def is_empty(self):
        return func.ST_IsEmpty(self.__geom_from_wkb)

    @property
    def is_simple(self):
        return func.ST_IsSimple(self.__geom_from_wkb)

    @property
    def is_closed(self):
        return func.ST_IsClosed(self.__geom_from_wkb)

    @property
    def is_ring(self):
        return func.ST_IsRing(self.__geom_from_wkb)

    @property
    def length(self):
        return func.ST_Length(self.__geom_from_wkb)

    @property
    def area(self):
        return func.ST_Area(self.__geom_from_wkb)

    @property
    def centroid(self):
        return func.ST_Centroid(self.__geom_from_wkb)

    @property
    def boundary(self):
        return func.ST_Boundary(self.__geom_from_wkb)

    def buffer(self, length=0.0, num_segs=8):
        return func.ST_Buffer(self.__geom_from_wkb, length, num_segs)

    @property
    def convex_hull(self):
        return func.ST_ConvexHull(self.__geom_from_wkb)

    @property
    def envelope(self):
        return func.ST_Envelope(self.__geom_from_wkb)

    @property
    def start_point(self):
        return func.ST_StartPoint(self.__geom_from_wkb)

    @property
    def end_point(self):
        return func.ST_EndPoint(self.__geom_from_wkb)

    @property
    def x(self):
        return func.ST_X(self.__geom_from_wkb)

    @property
    def y(self):
        return func.ST_Y(self.__geom_from_wkb)
        
    def transform(self, epsg=4326):
        return func.ST_Transform(self.__geom_from_wkb, epsg)

    # OGC Geometry Relations

    def equals(self, geom):
        return func.ST_Equals(self.__geom_from_wkb,
            SQLMMComparator.geom_from_wkb(geom))

    def distance(self, geom):
        return func.ST_Distance(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def within_distance(self, geom, distance=0.0):
        return func.ST_DWithin(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom), distance)

    def disjoint(self, geom):
        return func.ST_Disjoint(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def intersects(self, geom):
        return func.ST_Intersects(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def touches(self, geom):
        return func.ST_Touches(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def crosses(self, geom):
        return func.ST_Crosses(self.__geom_from_wkb,
    		SQLMMComparator.geom_from_wkb(geom))

    def within(self, geom):
        return func.ST_Within(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def overlaps(self, geom):
        return func.ST_Overlaps(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def gcontains(self, geom):
        return func.ST_Contains(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def covers(self, geom):
        return func.ST_Covers(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def covered_by(self, geom):
        return func.ST_CoveredBy(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))

    def intersection(self, geom):
        return func.ST_Intersection(self.__geom_from_wkb,
			SQLMMComparator.geom_from_wkb(geom))


class PGPersistentSpatialElement(PGSpatialElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc


