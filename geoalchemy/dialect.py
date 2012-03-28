from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy import func
from geoalchemy.functions import functions
from geoalchemy.base import WKTSpatialElement, WKBSpatialElement,\
    DBSpatialElement

class SpatialDialect(object):
    """This class bundles all required classes and methods to support 
    a database dialect. It is supposed to be subclassed.
    The child classes must be added to DialectManager.__initialize_dialects(),
    so that they can be used.
    
    """
    
    __functions = {
                   functions.wkt: 'AsText',
                   WKTSpatialElement : 'GeomFromText',
                   functions.wkb: 'AsBinary',
                   WKBSpatialElement : 'GeomFromWKB',
                   DBSpatialElement : '',
                   functions.dimension : 'Dimension',
                   functions.srid : 'SRID',
                   functions.geometry_type : 'GeometryType',
                   functions.is_valid : 'IsValid',
                   functions.is_empty : 'IsEmpty',
                   functions.is_simple : 'IsSimple',
                   functions.is_closed : 'IsClosed',
                   functions.is_ring : 'IsRing',
                   functions.num_points : 'NumPoints',
                   functions.point_n : 'PointN',
                   functions.length : 'Length',
                   functions.area : 'Area',
                   functions.x : 'X',
                   functions.y : 'Y',
                   functions.centroid : 'Centroid',
                   functions.boundary : 'Boundary',
                   functions.buffer : 'Buffer',
                   functions.convex_hull : 'ConvexHull',
                   functions.envelope : 'Envelope',
                   functions.start_point : 'StartPoint',
                   functions.end_point : 'EndPoint',
                   functions.transform : 'Transform',
                   functions.equals : 'Equals',
                   functions.distance : 'Distance',
                   functions.within_distance : 'DWithin',
                   functions.disjoint : 'Disjoint',
                   functions.intersects : 'Intersects',
                   functions.touches : 'Touches',
                   functions.crosses : 'Crosses',
                   functions.within : 'Within',
                   functions.overlaps : 'Overlaps',
                   functions.gcontains : 'Contains',
                   functions.covers : 'Covers',
                   functions.covered_by : 'CoveredBy',
                   functions.intersection : 'Intersections',
                   functions.union : None,
                   functions.collect : None,
                   functions.extent : None,
                   functions._within_distance: lambda compiler, geom1, geom2, dist:
                                                   func.DWithin(geom1, geom2, dist)
                  }
    
    def get_function(self, function_class):
        """This method is called to translate a generic function into a database
        dialect specific function.
        
        It either returns a string, a list or a method that returns a Function object.
        
            - String: A function name, e.g.::
                    
                    functions.wkb: 'SDO_UTIL.TO_WKBGEOMETRY'
            
            - List: A list of function names that are called cascaded, e.g.::
            
                    functions.wkt: ['TO_CHAR', 'SDO_UTIL.TO_WKTGEOMETRY'] is compiled as:
                    TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(..))
                    
            - Method: A method that accepts a list/set of arguments and returns a Function object, e.g.::
            
                    functions.equals : lambda params, within_column_clause : (func.SDO_EQUAL(*params) == 'TRUE')
    
        """
        if self._get_function_mapping() is not None:
            if function_class in self._get_function_mapping():
                if self._get_function_mapping()[function_class] is None:
                    raise NotImplementedError("Operation '%s' is not supported for '%s'" 
                                                % (function_class.__name__, self.__class__.__name__)) 
                else:
                    return self._get_function_mapping()[function_class]
        
        return SpatialDialect.__functions[function_class]
    
    def is_member_function(self, function_class):   
        """Returns True if the passed-in function should be called as member 
        function. E.g. ``Point.the_geom.dims`` is compiled as
        ``points.the_geom.Get_Dims()``.

        """
        return False
    
    def is_property(self, function_class):   
        """Returns True if the passed-in function should be called as property, 
        E.g. ``Point.the_geom.x`` is compiled as (for MS Server)
        ``points.the_geom.x``.

        """
        return False
    
    def _get_function_mapping(self):
        """This method can be overridden in subclasses, to set a database specific name
        for a function, or to add new functions, that are only supported by the database.
        
        """
        return None
    
    def process_result(self, value, type):
        """This method is called when a geometry value from the database is
        transformed into a SpatialElement object. It receives an WKB binary sequence, either as Buffer (for PostGIS
        and Spatialite), a String (for MySQL) or a cx_Oracle.LOB (for Oracle), and is supposed to return a 
        subclass of SpatialElement, e.g. PGSpatialElement or MySQLSpatialElement.
        
        """
        raise NotImplementedError("Method SpatialDialect.process_result must be implemented in subclasses.")
    
    def process_wkb(self, value):
        """This method is called from functions._WKBType.process_result_value() to convert
        the result of functions.wkb() into a usable format. 
        
        """
        return value
    
    def bind_wkb_value(self, wkb_element):
        """This method is called from base.__compile_wkbspatialelement() to insert
        the value of base.WKBSpatialElement into a query.
        
        """
        return None if wkb_element is None else wkb_element.desc
    
    def handle_ddl_after_create(self, bind, table, column):
        """This method is called after the mapped table was created in the database
        by SQLAlchemy. It is used to create a geometry column for the created table.
        
        """
        pass
    
    def handle_ddl_before_drop(self, bind, table, column):
        """This method is called after the mapped table was deleted from the database
        by SQLAlchemy. It can be used to delete the geometry column.
        
        """
        pass


class DialectManager(object):
    """This class is responsible for finding a spatial dialect (e.g. PGSpatialDialect or MySQLSpatialDialect)
    for a SQLAlchemy database dialect.
    
    It can be used by calling "DialectManager.get_spatial_dialect(dialect)", which returns the
    corresponding spatial dialect.
    The spatial dialect has to be listed in __initialize_dialects().
    
    """
    
    # all available spatial dialects {(SQLAlchemy dialect class: spatial dialect class)}    
    __dialects_mapping = None
    
    # all instantiated dialects {(spatial dialect class: spatial dialect instance)}    
    __spatial_dialect_instances = {}     
    
    @staticmethod
    def __initialize_dialects():
        #further spatial dialects can be added here
        from geoalchemy.postgis import PGSpatialDialect
        from geoalchemy.mysql import MySQLSpatialDialect
        from geoalchemy.spatialite import SQLiteSpatialDialect
        from geoalchemy.oracle import OracleSpatialDialect
        from geoalchemy.mssql import MSSpatialDialect
            
        DialectManager.__dialects_mapping = {
                PGDialect: PGSpatialDialect,
                SQLiteDialect: SQLiteSpatialDialect,
                MySQLDialect: MySQLSpatialDialect,
                OracleDialect: OracleSpatialDialect,
                MSDialect: MSSpatialDialect
                }

    @staticmethod
    def __dialects():
        if DialectManager.__dialects_mapping is None:
            DialectManager.__initialize_dialects()
            
        return DialectManager.__dialects_mapping
    
    @staticmethod
    def get_spatial_dialect(dialect):
        """This method returns a spatial dialect instance for a given SQLAlchemy dialect.
        The instances are cached, so that for every spatial dialect exists only one instance.
        
        """
        possible_spatial_dialects = [spatial_dialect for (dialect_sqlalchemy, spatial_dialect) 
                                     in DialectManager.__dialects().iteritems() 
                                     if isinstance(dialect, dialect_sqlalchemy)]
        
        if possible_spatial_dialects:
            # take the first possible spatial dialect
            spatial_dialect = possible_spatial_dialects[0]
            if spatial_dialect not in DialectManager.__spatial_dialect_instances:
                # if there is no instance for the given dialect yet, create one
                spatial_dialect_instance = spatial_dialect()
                DialectManager.__spatial_dialect_instances[spatial_dialect] = spatial_dialect_instance
                
            return DialectManager.__spatial_dialect_instances[spatial_dialect]
        else:
            raise NotImplementedError('Dialect "%s" is not supported by GeoAlchemy' % (dialect.name))
        
