from sqlalchemy import func, literal
from geoalchemy.base import GeometryBase, SpatialComparator, _to_gis, SpatialElement, WKTSpatialElement, WKBSpatialElement

class ComparatorFunctions(object):
    """This class provides methods to insert a SpatialElement into a query clause.
    """
    
    def _geom_from_wkb(self, geom=None):
        """This method is used to translate a SpatialElement.
        According to the type of geom, a conversion to the database geometry type is added or
        the column clause (column name) is returned.
            
        """
        if geom is None:
            geom = self
            
        if hasattr(geom, '__clause_element__'):
            return geom.__clause_element__()
        elif isinstance(geom, SpatialElement):
            if isinstance(geom, WKTSpatialElement):
                return func.GeomFromText(literal(geom.desc, GeometryBase), geom.srid)
            if isinstance(geom, WKBSpatialElement):
                return func.GeomFromWKB(literal(geom.desc, GeometryBase), geom.srid)  
            return func.GeomFromWKB(literal(geom.desc.desc, GeometryBase), geom.desc.srid)
        elif isinstance(geom, basestring):
            wkt = WKTSpatialElement(geom)
            return func.GeomFromText(literal(wkt, GeometryBase), wkt.srid)
            
        return literal(_to_gis(geom), GeometryBase)

    
class SFSComparatorFunctions(ComparatorFunctions):
    """Spatial functions following the OGC SFS standard.
    """

    # Geometry functions
    @property
    def wkt(self):
        return func.AsText(self._geom_from_wkb())

    @property
    def wkb(self):
        return func.AsBinary(self._geom_from_wkb()) # todo: in case we already have wkb, just return it

    @property
    def dimension(self):
        return func.Dimension(self._geom_from_wkb())

    @property
    def srid(self):
        return func.SRID(self._geom_from_wkb())

    @property
    def geometry_type(self):
        return func.GeometryType(self._geom_from_wkb())

    @property
    def is_empty(self):
        return func.IsEmpty(self._geom_from_wkb())

    @property
    def is_simple(self):
        return func.IsSimple(self._geom_from_wkb())

    @property
    def boundary(self):
        return func.Boundary(self._geom_from_wkb())

    @property
    def x(self):
        return func.X(self._geom_from_wkb())

    @property
    def y(self):
        return func.Y(self._geom_from_wkb())

    @property
    def envelope(self):
        return func.Envelope(self._geom_from_wkb())

    @property
    def start_point(self):
        return func.StartPoint(self._geom_from_wkb())

    @property
    def end_point(self):
        return func.EndPoint(self._geom_from_wkb())

    @property
    def length(self):
        return func.Length(self._geom_from_wkb())

    @property
    def is_closed(self):
        return func.IsClosed(self._geom_from_wkb())

    @property
    def is_ring(self):
        return func.IsRing(self._geom_from_wkb())

    @property
    def num_points(self):
        return func.NumPoints(self._geom_from_wkb())    
    
    def point_n(self, n=1):
        return func.PointN(self._geom_from_wkb(), n)
    
    @property
    def centroid(self):
        return func.Centroid(self._geom_from_wkb())

    @property
    def area(self):
        return func.Area(self._geom_from_wkb())

    # Geometry relations using the spatial element geometry
    
    def equals(self, other):
        return func.Equals(self._geom_from_wkb(),
            self._geom_from_wkb(other))

    def distance(self, other):
        return func.Distance(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def disjoint(self, other):
        return func.Disjoint(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def intersects(self, other):
        return func.Intersects(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def touches(self, other):
        return func.Touches(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def crosses(self, other):
        return func.Crosses(self._geom_from_wkb(),
    		self._geom_from_wkb(other))

    def within(self, other):
        return func.Within(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def overlaps(self, other):
        return func.Overlaps(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def gcontains(self, other):
        return func.Contains(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    # Geometry relations using Minimum Bounding Rectangle (MBR)

    def mbr_equal(self, other):
        return func.MBREqual(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_distance(self, other):
        return func.MBRDistance(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_disjoint(self, other):
        return func.MBRDisjoint(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_intersects(self, other):
        return func.MBRIntersects(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_touches(self, other):
        return func.MBRTouches(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_within(self, other):
        return func.MBRWithin(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_overlaps(self, other):
        return func.MBROverlaps(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def mbr_contains(self, other):
        return func.MBRContains(self._geom_from_wkb(),
			self._geom_from_wkb(other))
        
class SFSComparator(SpatialComparator, SFSComparatorFunctions):
    """Comparator class for SFS functions.
    """
    pass

class SQLMMComparatorFunctions(ComparatorFunctions):
    """Spatial functions following the OGC SQL/MM standard.
    """
    
    @property
    def wkt(self):
        return func.ST_AsText(self._geom_from_wkb())
    
    @property
    def wkb(self):
        return func.ST_AsBinary(self._geom_from_wkb())

    @property
    def gml(self):
        return func.ST_AsGML(self._geom_from_wkb())

    @property
    def dimension(self):
        return func.ST_Dimension(self._geom_from_wkb())

    @property
    def srid(self):
        return func.ST_SRID(self._geom_from_wkb())

    @property
    def geometry_type(self):
        return func.ST_GeometryType(self._geom_from_wkb())

    @property
    def is_empty(self):
        return func.ST_IsEmpty(self._geom_from_wkb())

    @property
    def is_simple(self):
        return func.ST_IsSimple(self._geom_from_wkb())

    @property
    def is_closed(self):
        return func.ST_IsClosed(self._geom_from_wkb())

    @property
    def is_ring(self):
        return func.ST_IsRing(self._geom_from_wkb())

    @property
    def length(self):
        return func.ST_Length(self._geom_from_wkb())

    @property
    def area(self):
        return func.ST_Area(self._geom_from_wkb())

    @property
    def centroid(self):
        return func.ST_Centroid(self._geom_from_wkb())

    @property
    def boundary(self):
        return func.ST_Boundary(self._geom_from_wkb())

    def buffer(self, length=0.0, num_segs=8):
        return func.ST_Buffer(self._geom_from_wkb(), length, num_segs)

    @property
    def convex_hull(self):
        return func.ST_ConvexHull(self._geom_from_wkb())

    @property
    def envelope(self):
        return func.ST_Envelope(self._geom_from_wkb())

    @property
    def start_point(self):
        return func.ST_StartPoint(self._geom_from_wkb())

    @property
    def end_point(self):
        return func.ST_EndPoint(self._geom_from_wkb())

    @property
    def x(self):
        return func.ST_X(self._geom_from_wkb())

    @property
    def y(self):
        return func.ST_Y(self._geom_from_wkb())
        
    def transform(self, epsg=4326):
        return func.ST_Transform(self._geom_from_wkb(), epsg)
       
    def equals(self, other):
        return func.ST_Equals(self._geom_from_wkb(),
            self._geom_from_wkb(other))

    def distance(self, other):
        return func.ST_Distance(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def within_distance(self, other, distance=0.0):
        return func.ST_DWithin(self._geom_from_wkb(),
			self._geom_from_wkb(other), distance)

    def disjoint(self, other):
        return func.ST_Disjoint(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def intersects(self, other):
        return func.ST_Intersects(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def touches(self, other):
        return func.ST_Touches(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def crosses(self, other):
        return func.ST_Crosses(self._geom_from_wkb(),
    		self._geom_from_wkb(other))

    def within(self, other):
        return func.ST_Within(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def overlaps(self, other):
        return func.ST_Overlaps(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def gcontains(self, other):
        return func.ST_Contains(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def covers(self, other):
        return func.ST_Covers(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def covered_by(self, other):
        return func.ST_CoveredBy(self._geom_from_wkb(),
			self._geom_from_wkb(other))

    def intersection(self, other):
        return func.ST_Intersection(self._geom_from_wkb(),
			self._geom_from_wkb(other))

class SQLMMComparator(SpatialComparator, SQLMMComparatorFunctions):
    """Comparator class for SQL/MM functions.
    """
    pass