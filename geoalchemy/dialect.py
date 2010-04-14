from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect
from geoalchemy.functions import functions

class SpatialDialect(object):
    """This class bundles all required classes and methods to support 
    a database dialect. It is supposed to be subclassed.
    The child classes must be added to DialectManager.__initialize_dialects(),
    so that they can be used.
    
    """
    
    __functions = {
                   functions.wkt: 'AsText',
                   functions.wkb: 'AsBinary',
                   functions.dimension : 'Dimension',
                   functions.srid : 'SRID',
                   functions.geometry_type : 'GeometryType',
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
                   functions.intersection : 'Intersections'
                  }
    
    def get_function_name(self, function_class):
        if self._get_function_mapping() is not None:
            if function_class in self._get_function_mapping():
                if self._get_function_mapping()[function_class] is None:
                    raise NotImplementedError("Operation '%s' is not supported for '%s'" 
                                                % (function_class.__name__, self.__class__.__name__)) 
                else:
                    return self._get_function_mapping()[function_class]
        
        return SpatialDialect.__functions[function_class]
    
    def _get_function_mapping(self):
        """This method can be overridden in subclasses, to set a database specific name
        for a function, or to add new functions, that are only supported by the database.
        """
        return None
    
    def get_comparator(self):
        """Returns the comparator class that contains all database operations
        you can execute on geometries. This method has to be overridden.
        The returned class must be a subclass of geoalchemy.comparator.SpatialComparator.
        
        """
        raise NotImplementedError("Method SpatialDialect.get_comparator must be implemented in subclasses.")
    
    def process_result(self, wkb_element):
        """This method is called when a geometry value from the database is
        transformed into a SpatialElement object. It receives an object of geoalchemy.base.WKBSpatialElement
        and is supposed to return a subclass of SpatialElement, e.g. PGSpatialElement or MySQLSpatialElement.
        
        """
        raise NotImplementedError("Method SpatialDialect.process_result must be implemented in subclasses.")
    
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
            
        DialectManager.__dialects_mapping = {
                PGDialect: PGSpatialDialect,
                SQLiteDialect: SQLiteSpatialDialect,
                MySQLDialect: MySQLSpatialDialect
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
            raise NotImplementedError
        