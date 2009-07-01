from sqlalchemy import Column, select, func, literal
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql import expression
from sqlalchemy.databases.postgres import PGDialect
from sqlalchemy.databases.sqlite import SQLiteDialect
from geoalchemy.postgis import PGPersistentSpatialElement
from geoalchemy.spatialite import SQLitePersistentSpatialElement
from geoalchemy.base import SpatialElement, WKTSpatialElement, SpatialComparator, GeometryBase, _to_gis

# SQL datatypes.

class Geometry(GeometryBase):
    """Base Geometry column type.
    
    Converts bind/result values to/from a PersistentSpatialElement.
    
    """
    
    def result_processor(self, dialect):
        def process(value):
            if value is not None:
                if isinstance(dialect, PGDialect):
                    return PGPersistentSpatialElement(value)
                if isinstance(dialect, SQLiteDialect):
                    return SQLitePersistentSpatialElement(value)
                else:
                    raise NotImplementedError
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

class Polygon(Geometry):
    name = 'POLYGON'

class MultiPoint(Geometry):
    name = 'MULTIPOINT'

class MultiLineString(Geometry):
    name = 'MULTILINESTRING'

class MultiPolygon(Geometry):
    name = 'MULTIPOLYGON'


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
                    if bind.dialect.__class__.__name__ == 'PGDialect':
                        bind.execute(select([func.DropGeometryColumn('public', table.name, c.name)], autocommit=True))
                    break
                
        elif event == 'after-create':
            table._columns = self._stack.pop()
            
            for c in table.c:
                if isinstance(c.type, Geometry):
                    bind.execute(select([func.AddGeometryColumn(table.name, c.name, c.type.srid, c.type.name, c.type.dimension)], autocommit=True))
        elif event == 'after-drop':
            table._columns = self._stack.pop()


class SpatialAttribute(AttributeExtension):
    """Intercepts 'set' events on a mapped instance attribute and 
    converts the incoming value to a GIS expression.
    
    """
    
    def set(self, state, value, oldvalue, initiator):
        return _to_gis(value)
            
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

