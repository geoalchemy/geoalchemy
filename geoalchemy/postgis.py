# -*- coding: utf-8 -*-
from sqlalchemy import select, func, and_
from geoalchemy.base import SpatialComparator, PersistentSpatialElement, \
    WKBSpatialElement, WKTSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction

class PGComparator(SpatialComparator):
    """Comparator class used for PostGIS
    """
    def __getattr__(self, name):
        try:
            return SpatialComparator.__getattr__(self, name)
        except AttributeError:
            return getattr(pg_functions, name)(self)


class PGPersistentSpatialElement(PersistentSpatialElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc
        
    def __getattr__(self, name):
        try:
            return PersistentSpatialElement.__getattr__(self, name)
        except AttributeError:
            return getattr(pg_functions, name)(self)


class pg_functions(functions):
    """Functions only supported by PostGIS
    """
    
    class svg(BaseFunction):
        """AsSVG(g)"""
        pass
    
    class kml(BaseFunction):
        """AsKML(g)"""
        pass

    class gml(BaseFunction):
        """AsGML(g)"""
        pass
    
    class geojson(BaseFunction):
        """AsGeoJSON(g): available since PostGIS version 1.3.4"""
        pass
    
    class expand(BaseFunction):
        """Expand(g)"""
        pass

    @staticmethod
    def _within_distance(compiler, geom1, geom2, distance, *args):
        """ST_DWithin in early versions of PostGIS 1.3 does not work when
        distance = 0. So we are directly using the (correct) internal
        definition. Note that the definition changed in version 1.3.4, see also:
        http://postgis.refractions.net/docs/ST_DWithin.html
        """
        return and_(func.ST_Expand(geom2, distance).op('&&')(geom1),
                    func.ST_Expand(geom1, distance).op('&&')(geom2),
                    func.ST_Distance(geom1, geom2) <= distance)

class PGSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for PostGIS."""
    
    __functions = {
                   WKTSpatialElement: 'ST_GeomFromText',
                   WKBSpatialElement: 'ST_GeomFromWKB',
                   functions.wkt: 'ST_AsText',
                   functions.wkb: 'ST_AsBinary',
                   functions.dimension : 'ST_Dimension',
                   functions.srid : 'ST_SRID',
                   functions.geometry_type : 'ST_GeometryType',
                   functions.is_valid : 'ST_IsValid',
                   functions.is_empty : 'ST_IsEmpty',
                   functions.is_simple : 'ST_IsSimple',
                   functions.is_closed : 'ST_IsClosed',
                   functions.is_ring : 'ST_IsRing',
                   functions.num_points : 'ST_NumPoints',
                   functions.point_n : 'ST_PointN',
                   functions.length : 'ST_Length',
                   functions.area : 'ST_Area',
                   functions.x : 'ST_X',
                   functions.y : 'ST_Y',
                   functions.centroid : 'ST_Centroid',
                   functions.boundary : 'ST_Boundary',
                   functions.buffer : 'ST_Buffer',
                   functions.convex_hull : 'ST_ConvexHull',
                   functions.envelope : 'ST_Envelope',
                   functions.start_point : 'ST_StartPoint',
                   functions.end_point : 'ST_EndPoint',
                   functions.transform : 'ST_Transform',
                   functions.equals : 'ST_Equals',
                   functions.distance : 'ST_Distance',
                   functions.within_distance : 'ST_DWithin',
                   functions.disjoint : 'ST_Disjoint',
                   functions.intersects : 'ST_Intersects',
                   functions.touches : 'ST_Touches',
                   functions.crosses : 'ST_Crosses',
                   functions.within : 'ST_Within',
                   functions.overlaps : 'ST_Overlaps',
                   functions.gcontains : 'ST_Contains',
                   functions.covers : 'ST_Covers',
                   functions.covered_by : 'ST_CoveredBy',
                   functions.intersection : 'ST_Intersection',
                   functions.union : 'ST_Union',
                   functions.collect : 'ST_Collect',
                   functions.extent : 'ST_Extent',
                   pg_functions.svg : 'ST_AsSVG',
                   pg_functions.kml : 'ST_AsKML',
                   pg_functions.gml : 'ST_AsGML',
                   pg_functions.geojson : 'ST_AsGeoJSON',
                   pg_functions.expand : 'ST_Expand',
                   functions._within_distance : pg_functions._within_distance
                  }
    
    def _get_function_mapping(self):
        return PGSpatialDialect.__functions
    
    def process_result(self, value, type):
        if type.wkt_internal:
            return PGPersistentSpatialElement(WKTSpatialElement(value, type.srid))
        return PGPersistentSpatialElement(WKBSpatialElement(value, type.srid))
    
    def handle_ddl_before_drop(self, bind, table, column):
        bind.execute(select([func.DropGeometryColumn((table.schema or 'public'), table.name, column.name)]).execution_options(autocommit=True))
    
    def handle_ddl_after_create(self, bind, table, column):    
        bind.execute(select([func.AddGeometryColumn((table.schema or 'public'), 
                                                    table.name, 
                                                    column.name, 
                                                    column.type.srid, 
                                                    column.type.name, 
                                                    column.type.dimension)]).execution_options(autocommit=True))
        if column.type.spatial_index:
            bind.execute("CREATE INDEX \"idx_%s_%s\" ON \"%s\".\"%s\" USING GIST (%s)" % 
                            (table.name, column.name, (table.schema or 'public'), table.name, column.name))
            
        if not column.nullable:
            bind.execute("ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" SET not null" % 
                            ((table.schema or 'public'), table.name, column.name))
            
