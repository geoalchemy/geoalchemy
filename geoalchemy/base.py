from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import expression
from sqlalchemy.types import TypeEngine

from utils import from_wkt
from functions import functions

# Base classes for geoalchemy

class SpatialElement(object):
    """Represents a geometry value."""

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)
    
    def __getattr__(self, name):
        return getattr(functions, name)(self)
#
    def __get_wkt(self, session):
        """This method converts the object into a WKT geometry. It takes into
        account that WKTSpatialElement does not have to make a new query
        to retrieve the WKT geometry.
        
        """
        if isinstance(self, WKTSpatialElement):
            # for WKTSpatialElement we don't need to make a new query
            return self.desc 
        else:
            return session.scalar(self.wkt)       

    def geom_type(self, session):
        wkt = self.__get_wkt(session)
        return from_wkt(wkt)["type"]

    def coords(self, session):
        wkt = self.__get_wkt(session)
        return from_wkt(wkt)["coordinates"]

class WKTSpatialElement(SpatialElement, expression.Function):
    """Represents a Geometry value expressed within application code; i.e. in
    the OGC Well Known Text (WKT) format.
    
    Extends expression.Function so that the value is interpreted as 
    GeomFromText(value) in a SQL expression context.
    
    """
    
    def __init__(self, desc, srid=4326):
        assert isinstance(desc, basestring)
        self.desc = desc
        self.srid = srid
        expression.Function.__init__(self, "GeomFromText", desc, srid)

    @property
    def geom_wkt(self):
        # directly return WKT value
        return self.desc

class WKBSpatialElement(SpatialElement, expression.Function):
    """Represents a Geometry value as expressed in the OGC Well
    Known Binary (WKB) format.
    
    Extends expression.Function so that the value is interpreted as 
    GeomFromWKB(value) in a SQL expression context.
    
    """
    
    def __init__(self, desc, srid=4326):
        assert isinstance(desc, (basestring, buffer))
        self.desc = desc
        self.srid = srid
        expression.Function.__init__(self, "GeomFromWKB", desc, srid)
        

class DBSpatialElement(SpatialElement, expression.Function):
    """This class can be used to wrap a geometry returned by a 
    spatial database operation.
    
    For example: 
    element = DBSpatialElement(session.scalar(r.geom.buffer(10.0)))
    session.scalar(element.wkt)
    
    """
    
    def __init__(self, desc):
        self.desc = desc
        expression.Function.__init__(self, "", desc)

class PersistentSpatialElement(SpatialElement):
    """Represents a Geometry value loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc
    
    @property    
    def geom_wkb(self):
        if self.desc is not None and isinstance(self.desc, WKBSpatialElement):
            return self.desc.desc
        else:
            return None

class GeometryBase(TypeEngine):
    """Base Geometry column type for all spatial databases.
    
    Converts bind/result values to/from a generic Persistent value.
    This is used as a base class and overridden into dialect specific
    Persistent values.
    """
    
    name = 'GEOMETRY'
    
    def __init__(self, dimension=2, srid=4326, spatial_index=True, **kwargs):
        self.dimension = dimension
        self.srid = srid
        self.spatial_index = spatial_index
        super(GeometryBase, self).__init__(**kwargs)
    
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                if isinstance(value, SpatialElement):
                    if isinstance(value.desc, SpatialElement):
                        return value.desc.desc
                    return value.desc
                else:
                    return value
            else:
                return value
        return process
        
    def result_processor(self, dialect, coltype=None):
        def process(value):
            if value is not None:
                return PersistentSpatialElement(value)
            else:
                return value
        return process

# ORM integration

def _to_gis(value, srid_db):
    """Interpret a value as a GIS-compatible construct."""

    if hasattr(value, '__clause_element__'):
        return value.__clause_element__()
    elif isinstance(value, SpatialElement):
        if isinstance(value.desc, (WKBSpatialElement, WKTSpatialElement)):
            return _check_srid(value.desc, srid_db)
        return _check_srid(value, srid_db)
    elif isinstance(value, basestring):
        return _check_srid(WKTSpatialElement(value), srid_db)
    elif isinstance(value, expression.ClauseElement):
        return value
    elif value is None:
        return None
    else:
        raise Exception("Invalid type")
    
def _check_srid(spatial_element, srid_db):
    """Check if the SRID of the spatial element which we are about to insert
    into the database equals the SRID used for the geometry column.
    If not, a transformation is added.
    """
    if srid_db is None or not hasattr(spatial_element, 'srid'):
        return spatial_element
    
    if spatial_element.srid == srid_db:
        return spatial_element
    else:
        return functions.transform(spatial_element, srid_db)

class SpatialComparator(ColumnProperty.ColumnComparator):
    """Intercepts standard Column operators on mapped class attributes
        and overrides their behavior.
        
        A comparator class makes sure that queries like 
        "session.query(Lake).filter(Lake.lake_geom.gcontains(..)).all()" can be executed.
    """
    
    def __getattr__(self, name):
        return getattr(functions, name)(self)
        
    # override the __eq__() operator (allows to use '==' on geometries)
    def __eq__(self, other): 
        return functions.equals(self, other)
    