from sqlalchemy import Column, select, func, literal
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql import expression

# Python datatypes

class SpatialElement(object):
    """Represents a geometry value."""

    @property
    def wkt(self):
        return func.ST_AsText(literal(self, Geometry))

    @property
    def wkb(self):
        return func.ST_AsBinary(literal(self, Geometry))

    @property
    def length(self):
        return func.ST_Length(literal(self, Geometry))

    @property
    def area(self):
        return func.ST_Area(literal(self, Geometry))

    @property
    def centroid(self):
        return func.ST_Centroid(literal(self, Geometry))

    @property
    def boundary(self):
        return func.ST_Boundary(literal(self, Geometry))

    def buffer(self, length=0.0, num_segs=8):
        return func.ST_Buffer(literal(self, Geometry), length, num_segs)

    @property
    def convex_hull(self):
        return func.ST_ConvexHull(literal(self, Geometry))

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
    
    def __init__(self, desc, srid=-1):
        assert isinstance(desc, basestring)
        self.desc = desc
        expression.Function.__init__(self, "ST_GeomFromText", desc, srid)


# SQL datatypes.

class Geometry(TypeEngine):
    """Base PostGIS Geometry column type.
    
    Converts bind/result values to/from a PersistentSpatialElement.
    
    """
    
    name = 'GEOMETRY'
    
    def __init__(self, dimension=None, srid=-1):
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

# other datatypes can be added as needed, which 
# currently only affect DDL statements.

class Point(Geometry):
    name = 'POINT'
    
class Curve(Geometry):
    name = 'CURVE'
    
class LineString(Curve):
    name = 'LINESTRING'

# ... etc.


# DDL integration

class GeometryDDL(object):
    """A DDL extension which integrates SQLAlchemy table create/drop 
    methods with PostGis' AddGeometryColumn/DropGeometryColumn functions.
    
    Usage::
    
        sometable = Table('sometable', metadata, ...)
        
        GeometryDDL(sometable)

        sometable.create()
    
    """
    
    def __init__(self, table):
        for event in ('before-create', 'after-create', 'before-drop', 'after-drop'):
            table.ddl_listeners[event].append(self)
        self._stack = []
        
    def __call__(self, event, table, bind):
        if event in ('before-create', 'before-drop'):
            regular_cols = [c for c in table.c if not isinstance(c.type, Geometry)]
            gis_cols = set(table.c).difference(regular_cols)
            self._stack.append(table.c)
            table._columns = expression.ColumnCollection(*regular_cols)
            
            if event == 'before-drop':
                for c in gis_cols:
                    bind.execute(select([func.DropGeometryColumn('public', table.name, c.name)], autocommit=True))
                
        elif event == 'after-create':
            table._columns = self._stack.pop()
            
            for c in table.c:
                if isinstance(c.type, Geometry):
                    bind.execute(select([func.AddGeometryColumn(table.name, c.name, c.type.srid, c.type.name, c.type.dimension)], autocommit=True))
        elif event == 'after-drop':
            table._columns = self._stack.pop()

# ORM integration

def _to_postgis(value):
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


class SpatialAttribute(AttributeExtension):
    """Intercepts 'set' events on a mapped instance attribute and 
    converts the incoming value to a GIS expression.
    
    """
    
    def set(self, state, value, oldvalue, initiator):
        return _to_postgis(value)
            
class SpatialComparator(ColumnProperty.ColumnComparator):
    """Intercepts standard Column operators on mapped class attributes
    and overrides their behavior.
    
    """
    
    # override the __eq__() operator
    def __eq__(self, other):
        return self.__clause_element__().op('~=')(_to_postgis(other))

    # add a custom operator
    def intersects(self, other):
        return self.__clause_element__().op('&&')(_to_postgis(other))
        
    # any number of Spatial operators can be overridden/added here
    # using the techniques above.
        

def GeometryColumn(*args, **kw):
    """Define a declarative column property with GIS behavior.
    
    This just produces orm.column_property() with the appropriate
    extension and comparator_factory arguments.  The given arguments
    are passed through to Column.  The declarative module extracts
    the Column for inclusion in the mapped table.
    
    """
    return column_property(
                Column(*args, **kw), 
                extension=SpatialAttribute(), 
                comparator_factory=SpatialComparator
            )

