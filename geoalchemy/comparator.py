from sqlalchemy import func, literal
from geoalchemy.base import GeometryBase, SpatialComparator, _to_gis

class SFSComparator(SpatialComparator):
    """Intercepts standard Column operators on mapped class attributes
        and overrides their behavior.
    """

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

    def contains(self, other):
        return func.Contains(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))


class SQLMMComparator(SpatialComparator):
    """Intercepts standard Column operators on mapped class attributes
        and overrides their behavior.
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

    def contains(self, other):
        return func.ST_Contains(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def covers(self, other):
        return func.ST_Covers(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

    def covered_by(self, other):
        return func.ST_CoveredBy(self.__clause_element__(),
			literal(_to_gis(other), GeometryBase))

