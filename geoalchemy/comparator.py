from sqlalchemy import func, literal
from geoalchemy.base import GeometryBase, SpatialComparator, _to_gis

class SFSComparator(SpatialComparator):
    """Spatial Comparators for implementing Geometry Relationships
    for OGC SFS geometries.
    """

    # Geometry relations using the spatial element geometry

    def equals(self, other):
        return func.Equals(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def distance(self, other):
        return func.Distance(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def disjoint(self, other):
        return func.Disjoint(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def intersects(self, other):
        return func.Intersects(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def touches(self, other):
        return func.Touches(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def crosses(self, other):
        return func.Crosses(self.__clause_element__(),
    			literal(_to_gis(other), GeometryBase))

    def within(self, other):
        return func.Within(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def overlaps(self, other):
        return func.Overlaps(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def gcontains(self, other):
        return func.Contains(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    # Geometry relations using Minimum Bounding Rectangle (MBR)

    def mbr_equals(self, other):
        return func.MBREquals(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_distance(self, other):
        return func.MBRDistance(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_disjoint(self, other):
        return func.MBRDisjoint(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_intersects(self, other):
        return func.MBRIntersects(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_touches(self, other):
        return func.MBRTouches(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_within(self, other):
        return func.MBRWithin(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_overlaps(self, other):
        return func.MBROverlaps(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def mbr_contains(self, other):
        return func.MBRContains(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))


class SQLMMComparator(SpatialComparator):
    """Spatial Comparators for implementing Geometry Relationships
    for OGC SQL/MM geometries.
    """

    def equals(self, other):
        return func.ST_Equals(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def distance(self, other):
        return func.ST_Distance(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def within_distance(self, other, distance=0.0):
        return func.ST_DWithin(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase), distance)

    def disjoint(self, other):
        return func.ST_Disjoint(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def intersects(self, other):
        return func.ST_Intersects(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def touches(self, other):
        return func.ST_Touches(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def crosses(self, other):
        return func.ST_Crosses(self.__clause_element__(),
    			literal(_to_gis(other), GeometryBase))

    def within(self, other):
        return func.ST_Within(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def overlaps(self, other):
        return func.ST_Overlaps(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def gcontains(self, other):
        return func.ST_Contains(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def covers(self, other):
        return func.ST_Covers(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def covered_by(self, other):
        return func.ST_CoveredBy(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def intersection(self, other):
        return func.ST_Intersection(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

