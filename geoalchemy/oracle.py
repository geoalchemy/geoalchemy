# -*- coding: utf-8 -*-
from sqlalchemy import select, func, exc
from geoalchemy.base import SpatialComparator, PersistentSpatialElement,\
    GeometryBase
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction
from geoalchemy.geometry import LineString, MultiLineString, GeometryCollection,\
    Geometry
from geoalchemy.base import WKTSpatialElement, WKBSpatialElement
    
import warnings
from sqlalchemy.schema import Column

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
    
    class gtype(BaseFunction):
        """g.Get_GType()"""
        pass
        
    class dims(BaseFunction):
        """g.Get_Dims()"""
        pass
    
    class kml(BaseFunction):
        """TO_CHAR(SDO_UTIL.TO_KMLGEOMETRY(g))"""
        pass

    class gml(BaseFunction):
        """TO_CHAR(SDO_UTIL.TO_GMLGEOMETRY(g))"""
        pass

    class gml311(BaseFunction):
        """TO_CHAR(SDO_UTIL.TO_GML311GEOMETRY(g))"""
        pass
    
    class expand(BaseFunction):
        """Expand(g)"""
        pass

def CastToST_GeometryType(function, returns_geometry=False, relation_function=False):
    def get_function(function, geom_cast, params, returns_geometry, relation_function):

        if relation_function:
            geom2_cast = cast_param(params)
            
            if geom2_cast is not None:
                function = function(geom_cast, geom2_cast, *params)
            else:
                function = function(geom_cast, *params)
        else:
            function = function(geom_cast, *params)
            
        if returns_geometry:
            return func.ST_GEOMETRY.GET_SDO_GEOM(function)
        else:
            return function
    
    def cast_param(params):
        if len(params) > 0:
            geom = params[0]
            
            if isinstance(geom, (WKBSpatialElement, WKTSpatialElement)) and geom.geometry_type <> Geometry.name:
                params.pop(0)
                return getattr(func, 'ST_%s' % (geom.geometry_type))(geom)

            elif isinstance(geom, Column) and isinstance(geom.type, GeometryBase):
                params.pop(0)
                return getattr(func, 'ST_%s' % (geom.type.name))(geom)
        
        return None
    
    
    def function_handler(params):
        if len(params) > 0:
            geom_cast = cast_param(params)
            if geom_cast is not None:
                return get_function(function, geom_cast, params, returns_geometry, relation_function)
            
        return function(*params)
    
    return function_handler

class OracleSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for Oracle."""
    
    __functions = {
                   functions.wkt: ['TO_CHAR', 'SDO_UTIL.TO_WKTGEOMETRY'],
                   WKTSpatialElement : 'MDSYS.SDO_GEOMETRY',
                   functions.wkb: 'SDO_UTIL.TO_WKBGEOMETRY',
                   WKBSpatialElement : 'SDO_GEOMETRY',
                   functions.dimension : ['MDSYS.ST_GEOMETRY.ST_DIMENSION', 'MDSYS.ST_GEOMETRY'],
                   functions.srid : ['MDSYS.OGC_SRID', 'MDSYS.ST_GEOMETRY'],
                   functions.geometry_type : ['MDSYS.OGC_GeometryType', 'MDSYS.ST_GEOMETRY'],
                   functions.is_empty : ['MDSYS.OGC_IsEmpty', 'MDSYS.ST_GEOMETRY'],
                   functions.is_simple : ['MDSYS.OGC_IsSimple', 'MDSYS.ST_GEOMETRY'],
                   functions.is_closed : CastToST_GeometryType(func.MDSYS.OGC_IsClosed),
                   functions.is_ring : CastToST_GeometryType(func.MDSYS.OGC_IsRing),
                   functions.num_points : CastToST_GeometryType(func.MDSYS.OGC_NumPoints), #sdo_util.getnumvertices
                   functions.point_n : CastToST_GeometryType(func.MDSYS.OGC_PointN, True),
                   functions.length : 'SDO_GEOM.SDO_Length',
                   functions.area : 'SDO_GEOM.SDO_Area',
                   functions.x : CastToST_GeometryType(func.MDSYS.OGC_X),
                   functions.y : CastToST_GeometryType(func.MDSYS.OGC_Y),
                   functions.centroid : 'SDO_GEOM.SDO_CENTROID',
                   functions.boundary : CastToST_GeometryType(func.MDSYS.OGC_Boundary, True),
                   functions.buffer : 'SDO_GEOM.SDO_Buffer',
                   functions.convex_hull : 'SDO_GEOM.SDO_ConvexHull',
                   functions.envelope : CastToST_GeometryType(func.MDSYS.OGC_Envelope, True),
                   functions.start_point : CastToST_GeometryType(func.MDSYS.OGC_StartPoint, True),
                   functions.end_point : CastToST_GeometryType(func.MDSYS.OGC_EndPoint, True),
                   functions.transform : 'SDO_CS.TRANSFORM',
                   functions.equals : lambda params : (func.SDO_EQUAL(*params) == 'TRUE'),
                   functions.distance : 'SDO_GEOM.SDO_Distance',
                   functions.within_distance : lambda params : (func.SDO_GEOM.Within_Distance(*params) == 'TRUE'),
                   functions.disjoint : CastToST_GeometryType(func.MDSYS.OGC_Disjoint, relation_function=True),
                   
                   functions.intersects : CastToST_GeometryType(func.MDSYS.OGC_Intersects, relation_function=True),
                   functions.touches : lambda params : (func.SDO_TOUCH(*params) == 'TRUE'),
#                   functions.crosses : CastToST_GeometryType(func.MDSYS.OGC_Crosses, relation_function=True),
                   functions.crosses : CastToST_GeometryType(func.mdsys.st_geometry.st_crosses, relation_function=True),
                   functions.within : CastToST_GeometryType(func.MDSYS.OGC_Within, relation_function=True),
                   
                   functions.overlaps : lambda params : (func.SDO_OVERLAPS(*params) == 'TRUE'),
                   functions.gcontains : lambda params : (func.SDO_CONTAINS(*params) == 'TRUE'),
                   functions.covers : lambda params : (func.SDO_COVERS(*params) == 'TRUE'),
                   functions.covered_by : lambda params : (func.SDO_COVEREDBY(*params) == 'TRUE'),
                   
                   functions.intersection : 'ST_Intersection',
                   
                   oracle_functions.gtype : 'Get_GType',
                   oracle_functions.dims : 'Get_Dims',
                   oracle_functions.kml : ['TO_CHAR', 'SDO_UTIL.TO_KMLGEOMETRY'],
                   oracle_functions.gml : ['TO_CHAR', 'SDO_UTIL.TO_GMLGEOMETRY'],
                   oracle_functions.gml311 : ['TO_CHAR', 'SDO_UTIL.TO_GML311GEOMETRY'],
                   oracle_functions.expand : 'ST_Expand'
                  }
    
    __member_functions = (
                          oracle_functions.dims,
                          oracle_functions.gtype
                        
                    )
    
    def _get_function_mapping(self):
        return OracleSpatialDialect.__functions
    
    def is_member_function(self, function_class):
        return function_class in self.__member_functions
    
    def get_comparator(self):
        return OracleComparator
    
    def process_result(self, value, column_srid, geometry_type):
        value = self.process_wkb(value)
            
        return OraclePersistentSpatialElement(WKBSpatialElement(value, column_srid, geometry_type))

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
        