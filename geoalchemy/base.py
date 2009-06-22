from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import expression
from sqlalchemy.types import TypeEngine

# Base classes for python datatypes

class SpatialElement(object):
    """Represents a geometry value."""

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

class PersistentSpatialElement(SpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc

class WKTSpatialElement(SpatialElement, expression.Function):
    """Represents a Geometry value as expressed within application code; i.e. in wkt format.
    
    Extends expression.Function so that the value is interpreted as 
    GeomFromText(value) in a SQL expression context.
    
    """
    
    def __init__(self, desc, srid=4326):
        assert isinstance(desc, basestring)
        self.desc = desc
        expression.Function.__init__(self, "GeomFromText", desc, srid)

class WKBSpatialElement(SpatialElement, expression.Function):
    """Represents a Geometry value as expressed in wkb format.
    
    Extends expression.Function so that the value is interpreted as 
    GeomFromWKB(value) in a SQL expression context.
    
    """
    
    def __init__(self, desc, srid=4326):
        assert isinstance(desc, basestring)
        self.desc = desc
        expression.Function.__init__(self, "GeomFromWKB", desc, srid)


class Geometry(TypeEngine):
    """Base Geometry column type.
    
    Converts bind/result values to/from a PersistentSpatialElement.
    
    """
    
    name = 'GEOMETRY'
    
    def __init__(self, dimension=None, srid=4326):
        self.dimension = dimension
        self.srid = srid
    
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.desc
            else:
                return value
        return process
        
    def result_processor(self, dialect):
        def process(value):
            if value is not None:
                return PersistentSpatialElement(value)
            else:
                return value
        return process

# ORM integration

def _to_dbms(value):
    """Interpret a value as a GIS-compatible construct."""

    if hasattr(value, '__clause_element__'):
        return value.__clause_element__()
    elif isinstance(value, (expression.ClauseElement, SpatialElement)):
        return value
    elif isinstance(value, basestring):
        return WKTSpatialElement(value)
    elif value is None:
        return None
    else:
        raise Exception("Invalid type")

class SpatialComparator(ColumnProperty.ColumnComparator):
    """Intercepts standard Column operators on mapped class attributes
        and overrides their behavior.
    """

    # override the __eq__() operator
    def __eq__(self, other):
        return self.__clause_element__().op('~=')(_to_dbms(other))

    # add a custom operator
    def intersects(self, other):
        return self.__clause_element__().op('&&')(_to_dbms(other)) 

