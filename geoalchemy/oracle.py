# -*- coding: utf-8 -*-
from sqlalchemy import select, func, exc
from geoalchemy.base import SpatialComparator, PersistentSpatialElement,\
    GeometryBase
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction, check_comparison, BooleanFunction
from geoalchemy.geometry import LineString, MultiLineString, GeometryCollection,\
    Geometry
from geoalchemy.base import WKTSpatialElement, WKBSpatialElement
    
import warnings
from sqlalchemy.schema import Column
from sqlalchemy.sql.expression import table, column, and_, text
from sqlalchemy.orm.attributes import InstrumentedAttribute

"""Currently cx_Oracle does not support the insertion of NULL values into geometry columns 
as bind parameter, see http://sourceforge.net/mailarchive/forum.php?thread_name=AANLkTikNG4brmQJiua2FQS8zUwk8rNgLHoe6SZ32f1gQ%40mail.gmail.com&forum_name=cx-oracle-users

For still being able to insert NULL, this variable can be used::
    
    spot_null = Spot(spot_height=None, spot_location=ORACLE_NULL_GEOMETRY)
    
"""
ORACLE_NULL_GEOMETRY = text('NULL')

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

def ST_GeometryFunction(function, returns_geometry = False, relation_function = False, 
                          returns_boolean = False, compare_value = 1, default_cast = False):
    """Functions inside MDSYS.OGC_* (OGC SF) and MDSYS.ST_GEOMETRY.ST_* (SQL MM) expect ST_GEOMETRY 
    instead of SDO_GEOMETRY, this method adds a cast.
    
    Some functions like OGC_X or OGC_IsClosed only work if the geometry (the first parameter) is casted 
    to a ST_GEOMETRY subtype, for example: 'OGC_X(ST_POINT(SDO_GEOMETRY(..)))'. 
    This method tries to get the geometry type from WKTSpatialElement, WKBSpatialElement and columns and 
    adds a corresponding cast (if possible).
    If the geometry type can not be identified, no cast is added and the user manually has to add 
    a cast, for example: 'session.scalar(functions.x(func.ST_POINT(spot.spot_location.transform(2249)))'
    
    Functions like OGC_IsEmpty do not require a cast to a subtype. In this case (``default_cast = True``),
    a cast to 'ST_GEOMETRY' is always added.
    
    If ``relation_function`` is set to ``True``, a cast is also added for the second parameter. 

    If the database function returns a new geometry (`returns_geometry = True``) a back-cast to
    SDO_GEOMETRY ('ST_GEOMETRY.GET_SDO_GEOM(..)') is added.  
    """
    assert(not (returns_geometry and returns_boolean)) # both can not be true
     
    def get_function(function, geom_cast, params, returns_geometry, relation_function):
        if relation_function:
            # cast 2nd geometry
            geom2_cast = cast_param(params)
            
            if geom2_cast is not None:
                function = function(geom_cast, geom2_cast, *params)
            else:
                function = function(geom_cast, *params)
        else:
            function = function(geom_cast, *params)
            
        if returns_geometry:
            # add cast to SDO_GEOMETRY
            return func.ST_GEOMETRY.GET_SDO_GEOM(function)
        else:
            return function
    
    def cast_param(params):
        if len(params) > 0:
            geom = params[0]
            
            if default_cast:
                params.pop(0)
                return func.ST_GEOMETRY(geom)
            
            elif isinstance(geom, (WKBSpatialElement, WKTSpatialElement)) and geom.geometry_type <> Geometry.name:
                params.pop(0)
                return getattr(func, 'ST_%s' % (geom.geometry_type))(geom)

            elif isinstance(geom, Column) and isinstance(geom.type, GeometryBase):
                params.pop(0)
                return getattr(func, 'ST_%s' % (geom.type.name))(geom)
        
        return None
    
    
    def function_handler(params, within_column_clause):
        if len(params) > 0:
            geom_cast = cast_param(params)
            if geom_cast is not None:
                return check_comparison(get_function(function, geom_cast, params, returns_geometry, relation_function),
                                          within_column_clause, returns_boolean, compare_value)
            
        return check_comparison(function(*params), within_column_clause, returns_boolean, compare_value)
    
    return function_handler

def DimInfoFunction(function, returns_boolean = False, compare_value = 'TRUE'):
    """Some Oracle functions expect a 'dimension info array' (DIMINFO) for each geometry. This method
    tries to append a corresponding DIMINFO for every geometry in the parameter list.
    
    For geometry columns a subselect is added which queries the DIMINFO from 'ALL_SDO_GEOM_METADATA'.
    For WKTSpatialElement/WKBSpatialElement objects, that were queried from the database, the DIMINFO 
    will be added as text representation (if possible).
    
    Most functions that require DIMINFO also accept a tolerance value instead of the DIMINFO. If you want to 
    use a tolerance value for geometry columns or WKBSpatialElement objects that come from the database, 
    the flag 'auto_diminfo' has to be set to 'False' when calling the function::
        
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        session.scalar(l.lake_geom.area) # DIMINFO is added automatically
        session.scalar(l.lake_geom.area(tolerance, auto_diminfo=False)) # DIMINFO is not added
    
    see also: http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objrelschema.htm#sthref300
    """
    
    def function_handler(params, within_column_clause, **flags):
        if flags.get('auto_diminfo', True):
            # insert diminfo for all geometries in params
            params = list(params)
            
            i = 0
            length = len(params)
            while i < length:
                diminfo = None
                if isinstance(params[i], (WKBSpatialElement, WKTSpatialElement)) and hasattr(params[i], 'DIMINFO'):
                    # the attribute DIMINFO is set in OracleSpatialDialect.process_result()
                    diminfo = params[i].DIMINFO
                    
                elif isinstance(params[i], Column) and isinstance(params[i].type, GeometryBase):
                    diminfo = OracleSpatialDialect.get_diminfo_select(params[i])
                    
                if diminfo is not None:
                    i += 1
                    # insert DIMINFO after the geometry
                    params.insert(i, diminfo)
                    length += 1
                    
                i += 1

        return check_comparison(function(*params), within_column_clause, returns_boolean, compare_value)
    
    return function_handler

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
    
    # Spatial Operators
    # http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_operat.htm#insertedID0
    
    class sdo_filter(BaseFunction):
        """SDO_FILTER(g1, g2, param)"""
        pass
    
    class sdo_nn(BaseFunction):
        """SDO_NN(g1, g2, param [, number])"""
        pass
    
    class sdo_nn_distance(BaseFunction):
        """SDO_NN_DISTANCE(number)"""
        pass
    
    class sdo_relate(BaseFunction):
        """SDO_RELATE(g1, g2, param)"""
        pass
    
    class sdo_within_distance(BaseFunction):
        """SDO_WITHIN_DISTANCE(g1, g2, param)"""
        pass
    
    class sdo_anyinteract(BaseFunction):
        """SDO_ANYINTERACT(g1, g2)"""
        pass
    
    class sdo_contains(BaseFunction):
        """SDO_CONTAINS(g1, g2)"""
        pass
    
    class sdo_coveredby(BaseFunction):
        """SDO_COVEREDBY(g1, g2)"""
        pass
    
    class sdo_covers(BaseFunction):
        """SDO_COVERS(g1, g2)"""
        pass
    
    class sdo_equal(BaseFunction):
        """SDO_EQUAL(g1, g2)"""
        pass
    
    class sdo_inside(BaseFunction):
        """SDO_INSIDE(g1, g2)"""
        pass
    
    class sdo_on(BaseFunction):
        """SDO_ON(g1, g2)"""
        pass
    
    class sdo_overlapbdydisjoint(BaseFunction):
        """SDO_OVERLAPBDYDISJOINT(g1, g2)"""
        pass
    
    class sdo_overlapbdyintersect(BaseFunction):
        """SDO_OVERLAPBDYINTERSECT(g1, g2)"""
        pass
    
    class sdo_overlaps(BaseFunction):
        """SDO_OVERLAPS(g1, g2)"""
        pass
    
    class sdo_touch(BaseFunction):
        """SDO_TOUCH(g1, g2)"""
        pass
    
    # Selection of functions of the SDO_GEOM package
    # http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objgeom.htm#insertedID0
    
    class sdo_geom_sdo_area(BaseFunction):
        """SDO_GEOM.SDO_AREA()"""
        pass
    
    class sdo_geom_sdo_buffer(BaseFunction):
        """SDO_GEOM.SDO_BUFFER()"""
        pass
    
    class sdo_geom_sdo_centroid(BaseFunction):
        """SDO_GEOM.SDO_CENTROID()"""
        pass
    
    class sdo_geom_sdo_concavehull(BaseFunction):
        """SDO_GEOM.SDO_CONCAVEHULL()"""
        pass
    
    class sdo_geom_sdo_concavehull_boundary(BaseFunction):
        """SDO_GEOM.SDO_CONCAVEHULL_BOUNDARY()"""
        pass
    
    class sdo_geom_sdo_convexhull(BaseFunction):
        """SDO_GEOM.SDO_CONVEXHULL()"""
        pass
    
    class sdo_geom_sdo_difference(BaseFunction):
        """SDO_GEOM.SDO_DIFFERENCE()"""
        pass
    
    class sdo_geom_sdo_distance(BaseFunction):
        """SDO_GEOM.SDO_DISTANCE()"""
        pass
    
    class sdo_geom_sdo_intersection(BaseFunction):
        """SDO_GEOM.SDO_INTERSECTION()"""
        pass
    
    class sdo_geom_sdo_length(BaseFunction):
        """SDO_GEOM.SDO_LENGTH()"""
        pass
    
    class sdo_geom_sdo_mbr(BaseFunction):
        """SDO_GEOM.SDO_MBR()"""
        pass
    
    class sdo_geom_sdo_pointonsurface(BaseFunction):
        """SDO_GEOM.SDO_POINTONSURFACE()"""
        pass
    
    class sdo_geom_sdo_union(BaseFunction):
        """SDO_GEOM.SDO_UNION()"""
        pass
    
    class sdo_geom_sdo_xor(BaseFunction):
        """SDO_GEOM.SDO_XOR()"""
        pass
    
    class sdo_geom_sdo_within_distance(BaseFunction):
        """SDO_GEOM.WITHIN_DISTANCE()"""
        pass
 
    @staticmethod
    def _within_distance(compiler, geom1, geom2, distance, additional_params={}):
        """If the first parameter is a geometry column, then the Oracle operator
        SDO_WITHIN_DISTANCE is called and Oracle makes use of the spatial index of
        this column.

        If the first parameter is not a geometry column but a function, which is
        the case when a coordinate transformation had to be added by the spatial
        filter, then the function SDO_GEOM.WITHIN_DISTANCE is called.
        SDO_GEOM.WITHIN_DISTANCE does not make use of a spatial index and requires
        additional parameters: either a tolerance value or a dimension information
        array (DIMINFO) for both geometries. These parameters can be specified when
        defining the spatial filter, e.g.::

            additional_params={'tol': '0.005'}

            or

            from sqlalchemy.sql.expression import text
            diminfo = text("MDSYS.SDO_DIM_ARRAY("\
                "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.000000005),"\
                "MDSYS.SDO_DIM_ELEMENT('LATITUDE', -90, 90, 0.000000005)"\
                ")")
            additional_params={'dim1': diminfo, 'dim2': diminfo}

            filter = create_default_filter(request, Spot, additional_params=additional_params)
            proto.count(request, filter=filter)

        For its distance calculation Oracle by default uses meter as unit for
        geodetic data (like EPSG:4326) and otherwise the 'unit of measurement
        associated with the data'. The unit used for the 'distance' value can be
        changed by adding an entry to 'additional_params'. Valid units are defined
        in the view 'sdo_dist_units'::

            additional_params={'params': 'unit=km'}

        SDO_WITHIN_DISTANCE accepts further parameters, which can also be set using
        the name 'params' together with the unit::

            additional_params={'params': 'unit=km max_resolution=10'}


        Valid options for 'additional_params' are:

            params
                A String containing additional parameters, for example the unit.

            tol
                The tolerance value used for the SDO_GEOM.WITHIN_DISTANCE function call.

            dim1 and dim2
                If the parameter 'tol' is not set, these two parameters have to be
                set. 'dim1' is the DIMINFO for the first geometry (the reprojected
                geometry column) and 'dim2' is the DIMINFO for the second geometry
                (the input geometry from the request). Values for 'dim1' and 'dim2'
                have to be SQLAlchemy expressions, either literal text (text(..))
                or a select query.

        Note that 'tol' or 'dim1'/'dim2' only have to be set when the input
        geometry from the request uses a different CRS than the geometry column!


        SDO_WITHIN_DISTANCE:
        http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_operat.htm#i77653

        SDO_GEOM.WITHIN_DISTANCE:
        http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objgeom.htm#i856373

        DIMINFO:
        http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objrelschema.htm#i1010905

        TOLERANCE:
        http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_intro.htm#i884589
        """

        params = additional_params.get('params', '')
        if isinstance(geom1, Column):
            return (func.SDO_WITHIN_DISTANCE(
                        geom1, geom2,
                        'distance=%s %s' % (distance, params)) == 'TRUE')
        else:
            dim1 = additional_params.get('dim1', None)
            dim2 = additional_params.get('dim2', None)
            if dim1 is not None and dim2 is not None:
                return (func.SDO_GEOM.WITHIN_DISTANCE(geom1, dim1,
                                                 distance,
                                                 geom2, dim2,
                                                 params) == 'TRUE')
            else:
                tol = additional_params.get('tol', None)
                if tol is not None:
                    return (func.SDO_GEOM.WITHIN_DISTANCE(geom1,
                                                 distance,
                                                 geom2,
                                                 tol,
                                                 params) == 'TRUE')
                else:
                    raise Exception('No dimension information ("dim1" and "dim2") or '\
                                    'tolerance value ("tol") specified for calling '\
                                    'SDO_GEOM.WITHIN_DISTANCE on Oracle, which is '\
                                    'required when reprojecting.')

class OracleSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for Oracle."""
    
    __functions = {
                   functions.wkt: ['TO_CHAR', 'SDO_UTIL.TO_WKTGEOMETRY'],
                   WKTSpatialElement : 'MDSYS.SDO_GEOMETRY',
                   functions.wkb: 'SDO_UTIL.TO_WKBGEOMETRY',
                   WKBSpatialElement : 'MDSYS.SDO_GEOMETRY',
                   functions.dimension : ['MDSYS.ST_GEOMETRY.ST_DIMENSION', 'MDSYS.ST_GEOMETRY'],
                   functions.srid : ['MDSYS.OGC_SRID', 'MDSYS.ST_GEOMETRY'],
                   functions.geometry_type : ['MDSYS.OGC_GeometryType', 'MDSYS.ST_GEOMETRY'],
                   functions.is_valid : None,
                   functions.is_empty : ST_GeometryFunction(func.MDSYS.OGC_IsEmpty, returns_boolean=True, default_cast=True),
                   functions.is_simple : ST_GeometryFunction(func.MDSYS.OGC_IsSimple, returns_boolean=True, default_cast=True),
                   functions.is_closed : ST_GeometryFunction(func.MDSYS.OGC_IsClosed, returns_boolean=True),
                   functions.is_ring : ST_GeometryFunction(func.MDSYS.OGC_IsRing, returns_boolean=True),
                   functions.num_points : ST_GeometryFunction(func.MDSYS.OGC_NumPoints),
                   functions.point_n : ST_GeometryFunction(func.MDSYS.OGC_PointN, True),
                   functions.length : DimInfoFunction(func.SDO_GEOM.SDO_Length),
                   functions.area : DimInfoFunction(func.SDO_GEOM.SDO_Area),
                   functions.x : ST_GeometryFunction(func.MDSYS.OGC_X),
                   functions.y : ST_GeometryFunction(func.MDSYS.OGC_Y),
                   functions.centroid : DimInfoFunction(func.SDO_GEOM.SDO_CENTROID),
                   functions.boundary : ST_GeometryFunction(func.MDSYS.ST_GEOMETRY.ST_Boundary, True),
                   functions.buffer : DimInfoFunction(func.SDO_GEOM.SDO_Buffer),
                   functions.convex_hull : DimInfoFunction(func.SDO_GEOM.SDO_ConvexHull),
                   functions.envelope : ST_GeometryFunction(func.MDSYS.ST_GEOMETRY.ST_Envelope, True),
                   functions.start_point : ST_GeometryFunction(func.MDSYS.OGC_StartPoint, True),
                   functions.end_point : ST_GeometryFunction(func.MDSYS.OGC_EndPoint, True),
                   functions.transform : 'SDO_CS.TRANSFORM',
                   # note: we are not using SDO_Equal because it always requires a spatial index
                   functions.equals : ST_GeometryFunction(func.MDSYS.OGC_EQUALS, returns_boolean=True, relation_function=True, default_cast=True), 
                   functions.distance : DimInfoFunction(func.SDO_GEOM.SDO_Distance),
                   functions.within_distance : DimInfoFunction(func.SDO_GEOM.Within_Distance, returns_boolean=True),
                   functions.disjoint : ST_GeometryFunction(func.MDSYS.OGC_Disjoint, relation_function=True, returns_boolean=True, default_cast=True),                   
                   functions.intersects : ST_GeometryFunction(func.MDSYS.OGC_Intersects, relation_function=True, returns_boolean=True, default_cast=True),
                   functions.touches : ST_GeometryFunction(func.MDSYS.OGC_Touch, relation_function=True, returns_boolean=True, default_cast=True),
                   functions.crosses : ST_GeometryFunction(func.MDSYS.OGC_Cross, relation_function=True, returns_boolean=True, default_cast=True),                   
                   functions.within : ST_GeometryFunction(func.MDSYS.OGC_Within, relation_function=True, returns_boolean=True, default_cast=True),                   
                   functions.overlaps : ST_GeometryFunction(func.MDSYS.OGC_Overlap, relation_function=True, returns_boolean=True, default_cast=True),                   
                   functions.gcontains : ST_GeometryFunction(func.MDSYS.OGC_Contains, relation_function=True, returns_boolean=True, default_cast=True),
                   functions.covers : None, # use oracle_functions.sdo_covers 
                   functions.covered_by : None, # use oracle_functions.sdo_coveredby
                   functions.intersection : DimInfoFunction(func.SDO_GEOM.SDO_INTERSECTION),
                   # aggregate_union => SDO_AGGR_UNION ?
                   
                   oracle_functions.gtype : 'Get_GType',
                   oracle_functions.dims : 'Get_Dims',
                   oracle_functions.kml : ['TO_CHAR', 'SDO_UTIL.TO_KMLGEOMETRY'],
                   oracle_functions.gml : ['TO_CHAR', 'SDO_UTIL.TO_GMLGEOMETRY'],
                   oracle_functions.gml311 : ['TO_CHAR', 'SDO_UTIL.TO_GML311GEOMETRY'],
                   
                   oracle_functions.sdo_filter : BooleanFunction(func.SDO_FILTER),
                   oracle_functions.sdo_nn : BooleanFunction(func.SDO_NN),
                   oracle_functions.sdo_nn_distance : 'SDO_NN_DISTANCE',
                   oracle_functions.sdo_relate : BooleanFunction(func.SDO_RELATE),
                   oracle_functions.sdo_within_distance : BooleanFunction(func.SDO_WITHIN_DISTANCE),
                   oracle_functions.sdo_anyinteract : BooleanFunction(func.SDO_ANYINTERACT),
                   oracle_functions.sdo_contains : BooleanFunction(func.SDO_CONTAINS),
                   oracle_functions.sdo_coveredby : BooleanFunction(func.SDO_COVEREDBY),
                   oracle_functions.sdo_covers : BooleanFunction(func.SDO_COVERS),
                   oracle_functions.sdo_equal : BooleanFunction(func.SDO_EQUAL),
                   oracle_functions.sdo_inside : BooleanFunction(func.SDO_INSIDE),
                   oracle_functions.sdo_on : BooleanFunction(func.SDO_ON),
                   oracle_functions.sdo_overlapbdydisjoint : BooleanFunction(func.SDO_OVERLAPBDYDISJOINT),
                   oracle_functions.sdo_overlapbdyintersect : BooleanFunction(func.SDO_OVERLAPBDYINTERSECT),
                   oracle_functions.sdo_overlaps : BooleanFunction(func.SDO_OVERLAPS),
                   oracle_functions.sdo_touch : BooleanFunction(func.SDO_TOUCH),
                   
                   # same as functions.area
                   oracle_functions.sdo_geom_sdo_area : DimInfoFunction(func.SDO_GEOM.SDO_Area), 
                   # same as functions.buffer
                   oracle_functions.sdo_geom_sdo_buffer : DimInfoFunction(func.SDO_GEOM.SDO_Buffer), 
                   # same as functions.centroid
                   oracle_functions.sdo_geom_sdo_centroid : DimInfoFunction(func.SDO_GEOM.SDO_CENTROID), 
                   oracle_functions.sdo_geom_sdo_concavehull : 'SDO_GEOM.SDO_CONCAVEHULL', 
                   oracle_functions.sdo_geom_sdo_concavehull_boundary : 'SDO_GEOM.SDO_CONCAVEHULL_BOUNDARY',
                   # same as functions.convexhull
                   oracle_functions.sdo_geom_sdo_convexhull : DimInfoFunction(func.SDO_GEOM.SDO_CONVEXHULL), 
                   # same as functions.distance
                   oracle_functions.sdo_geom_sdo_difference : DimInfoFunction(func.SDO_GEOM.SDO_DIFFERENCE), 
                   # same as functions.intersection
                   oracle_functions.sdo_geom_sdo_difference : DimInfoFunction(func.SDO_GEOM.SDO_INTERSECTION), 
                   # same as functions.length
                   oracle_functions.sdo_geom_sdo_length : DimInfoFunction(func.SDO_GEOM.SDO_LENGTH), 
                   oracle_functions.sdo_geom_sdo_mbr : DimInfoFunction(func.SDO_GEOM.SDO_MBR), 
                   oracle_functions.sdo_geom_sdo_pointonsurface : DimInfoFunction(func.SDO_GEOM.SDO_POINTONSURFACE), 
                   oracle_functions.sdo_geom_sdo_union : DimInfoFunction(func.SDO_GEOM.SDO_UNION), 
                   oracle_functions.sdo_geom_sdo_xor : DimInfoFunction(func.SDO_GEOM.SDO_XOR), 
                   # same as functions.within_distance
                   oracle_functions.sdo_geom_sdo_within_distance : DimInfoFunction(func.SDO_GEOM.Within_Distance, returns_boolean=True),

                   functions._within_distance : oracle_functions._within_distance
                  }
    
    __member_functions = (
                          oracle_functions.dims,
                          oracle_functions.gtype
                    )
    
    METADATA_TABLE = table('ALL_SDO_GEOM_METADATA', column('diminfo'), column('table_name'), column('column_name'))
    
    def _get_function_mapping(self):
        return OracleSpatialDialect.__functions
    
    def is_member_function(self, function_class):
        return function_class in self.__member_functions
    
    def process_result(self, value, type):
        value = self.process_wkb(value)
        wkb_element = WKBSpatialElement(value, type.srid, type.name)    
        
        if type.kwargs.has_key("diminfo"):
            # also set the DIMINFO data so that in can be used in function calls, see DimInfoFunction()
            if not type.kwargs.has_key("diminfo_sql"):
                # cache the SQLAlchemy text literal
                type.kwargs["diminfo_sql"] = text(type.kwargs["diminfo"])
            wkb_element.DIMINFO = type.kwargs["diminfo_sql"]
        
        return OraclePersistentSpatialElement(wkb_element)

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

    @staticmethod
    def get_diminfo_select(column):
        """Returns a select which queries the DIMINFO array from 'ALL_SDO_GEOM_METADATA'
        for the passed in column.
        
        see: http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objrelschema.htm#sthref300
        """
        if isinstance(column, InstrumentedAttribute):
            column = column.property.columns[0]
        
        return select([OracleSpatialDialect.METADATA_TABLE.c.diminfo]).where(
                                                and_(OracleSpatialDialect.METADATA_TABLE.c.table_name == column.table.name.upper(),
                                                     OracleSpatialDialect.METADATA_TABLE.c.column_name == column.name.upper()))

    def handle_ddl_before_drop(self, bind, table, column):
        bind.execute("DELETE FROM USER_SDO_GEOM_METADATA WHERE table_name = '%s' AND column_name = '%s'" %
                            (table.name.upper(), column.name.upper()))
        
        if column.type.spatial_index and column.type.kwargs.has_key("diminfo"):
            bind.execute("DROP INDEX %s_%s_sidx" % (table.name, column.name))
          
    def handle_ddl_after_create(self, bind, table, column):    
        bind.execute("ALTER TABLE %s ADD %s %s" % 
                            (table.name, column.name, 'SDO_GEOMETRY'))
        
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
