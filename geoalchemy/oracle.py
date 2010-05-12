# -*- coding: utf-8 -*-
from sqlalchemy import select, func, exc
from geoalchemy.base import SpatialComparator, PersistentSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction
from geoalchemy.geometry import LineString, MultiLineString, GeometryCollection,\
    Geometry
from geoalchemy.base import WKTSpatialElement, WKBSpatialElement
    
import warnings

class OracleComparator(SpatialComparator):
    """Comparator class used for Oracle
    """
    def __getattr__(self, name):
        try:
            return SpatialComparator.__getattr__(self, name)
        except AttributeError:
            return getattr(oracle_functions, name)(self)


class OraclePersistentSpatialElement(PersistentSpatialElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc
        
    def __getattr__(self, name):
        try:
            return PersistentSpatialElement.__getattr__(self, name)
        except AttributeError:
            return getattr(oracle_functions, name)(self)


class oracle_functions(functions):
    """Functions only supported by Oracle
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


class OracleSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for Oracle."""
    
    __functions = {
                   functions.wkt: ['TO_CHAR', 'SDO_UTIL.TO_WKTGEOMETRY'],
                   WKTSpatialElement : 'MDSYS.SDO_GEOMETRY',
                   functions.wkb: 'SDO_UTIL.TO_WKBGEOMETRY',
                   WKBSpatialElement : 'SDO_GEOMETRY',
                   functions.dimension : 'Get_Dims',
                   functions.srid : 'ST_SRID',
                   functions.geometry_type : 'Get_GType',
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
                   functions.centroid : 'SDO_GEOM.SDO_CENTROID',
                   functions.boundary : 'ST_Boundary',
                   functions.buffer : 'ST_Buffer',
                   functions.convex_hull : 'ST_ConvexHull',
                   functions.envelope : 'ST_Envelope',
                   functions.start_point : 'ST_StartPoint',
                   functions.end_point : 'ST_EndPoint',
                   functions.transform : 'ST_Transform',
                   functions.equals : lambda params : (func.SDO_EQUAL(*params) == 'TRUE'),
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
                   oracle_functions.svg : 'ST_AsSVG',
                   oracle_functions.kml : 'ST_AsKML',
                   oracle_functions.gml : 'ST_AsGML',
                   oracle_functions.geojson : 'ST_AsGeoJSON',
                   oracle_functions.expand : 'ST_Expand'
                  }
    
    __member_functions = (
                          functions.dimension,
                          functions.geometry_type
                        
                    )
    
    def _get_function_mapping(self):
        return OracleSpatialDialect.__functions
    
    def is_member_function(self, function_class):
        return function_class in self.__member_functions
    
    def get_comparator(self):
        return OracleComparator
    
    def process_result(self, value, column_srid):
        value = self.process_wkb(value)
            
        return OraclePersistentSpatialElement(WKBSpatialElement(value, column_srid))

    def process_wkb(self, value):
        """SDO_UTIL.TO_WKBGEOMETRY(..) returns an object of cx_Oracle.LOB, which we 
        will transform into a buffer.
        """
        if value is not None:
            return buffer(value.read())
        else:
            return value
    
    def bind_wkb_value(self, wkb_element):
        """Append a transformation to BLOB using the Oracle function 'TO_BLOB'.
        """
        if wkb_element is not None and wkb_element.desc is not None:
            return func.TO_BLOB(wkb_element.desc)
        
        return None

    def handle_ddl_before_drop(self, bind, table, column):
        bind.execute("DELETE FROM USER_SDO_GEOM_METADATA WHERE table_name = '%s' AND column_name = '%s'" %
                            (table.name.upper(), column.name.upper()))
        
        if column.type.spatial_index and column.type.kwargs.has_key("diminfo"):
            bind.execute("DROP INDEX %s_%s_sidx" % (table.name, column.name))
          
    def handle_ddl_after_create(self, bind, table, column):    
        bind.execute("ALTER TABLE %s ADD %s %s" % 
                            (table.name, column.name, 'SDO_GEOMETRY')) #column.type.name))
        
        if not column.nullable:
            bind.execute("ALTER TABLE %s MODIFY %s NOT NULL" % (table.name, column.name))
        

        if not column.type.kwargs.has_key("diminfo"):
            warnings.warn("No DIMINFO given for '%s.%s', no entry in USER_SDO_GEOM_METADATA will be made "\
                    "and no spatial index will be created." % (table.name, column.name), 
                    exc.SAWarning, stacklevel=3)
        else:
            diminfo = column.type.kwargs["diminfo"]
        
            bind.execute("INSERT INTO USER_SDO_GEOM_METADATA (table_name, column_name, diminfo, srid) " +
                            "VALUES ('%s', '%s', %s, %s)" % 
                            (table.name, column.name, diminfo, column.type.srid))
            
            if column.type.spatial_index:
                bind.execute("CREATE INDEX %s_%s_sidx ON %s(%s) "\
                             "INDEXTYPE IS MDSYS.SPATIAL_INDEX%s" % 
                             (table.name, column.name, table.name, column.name, 
                              self.__get_index_parameters(column.type)))
    
    def __get_index_parameters(self, type):
        type_name = self.__get_oracle_gtype(type)
        
        if type_name == None:
            return ""
        else:
            return " PARAMETERS ('LAYER_GTYPE=%s')" % type_name
    
    def __get_oracle_gtype(self, type):
        """Maps the GeoAlchemy types to SDO_GTYPE values:
        http://download.oracle.com/docs/cd/B19306_01/appdev.102/b14255/sdo_objrelschema.htm#g1013735
        """
        if isinstance(type, LineString):
            return "LINE"
        elif isinstance(type, MultiLineString):
            return "MULTILINE"
        elif isinstance(type, GeometryCollection):
            return "COLLECTION"
        else:
            if type.name == Geometry.name:
                return None
            else:
                return type.name
        