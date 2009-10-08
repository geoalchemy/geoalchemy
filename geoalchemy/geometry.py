from sqlalchemy import Column, select, func, literal
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql import expression
from sqlalchemy.databases.postgres import PGDialect
from sqlalchemy.databases.sqlite import SQLiteDialect
from sqlalchemy.databases.mysql import MySQLDialect
from geoalchemy.postgis import PGPersistentSpatialElement
from geoalchemy.spatialite import SQLitePersistentSpatialElement
from geoalchemy.mysql import MySQLPersistentSpatialElement
from geoalchemy.base import SpatialElement, WKTSpatialElement, GeometryBase, _to_gis
from geoalchemy.comparator import SFSComparator, SQLMMComparator

class Geometry(GeometryBase):
    """Geometry column type. This is the base class for all other
    geometry types like Point, LineString, Polygon, etc.
    
    Converts bind/result values to/from a dialect specific persistent
    geometry value.
    
    """
    
    def result_processor(self, dialect):
        def process(value):
            if value is not None:
                if isinstance(dialect, PGDialect):
                    return PGPersistentSpatialElement(value)
                if isinstance(dialect, SQLiteDialect):
                    return SQLitePersistentSpatialElement(value)
                if isinstance(dialect, MySQLDialect):
                    return MySQLPersistentSpatialElement(value)
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

class GeometryCollection(Geometry):
    name = 'GEOMETRYCOLLECTION'


class GeometryDDL(object):
    """A DDL extension which integrates SQLAlchemy table create/drop 
    methods with AddGeometryColumn/DropGeometryColumn functions of
    spatial databases.
    
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
			bind.execute(select([func.DropGeometryColumn((table.schema or 'public'), table.name, c.name)], autocommit=True))
                    elif bind.dialect.__class__.__name__ == 'SQLiteDialect':
                        bind.execute(select([func.DiscardGeometryColumn(table.name, c.name)], autocommit=True))
                    else:
                        pass
                    break
                
        elif event == 'after-create':
            table._columns = self._stack.pop()
            
            for c in table.c:
                if isinstance(c.type, Geometry):
                    if bind.dialect.__class__.__name__ == 'PGDialect':
			bind.execute(select([func.AddGeometryColumn((table.schema or 'public'), table.name, c.name, c.type.srid, c.type.name, c.type.dimension)], autocommit=True))
                        if c.type.spatial_index:
			    bind.execute("CREATE INDEX idx_%s_%s ON %s.%s USING GIST (%s GIST_GEOMETRY_OPS)" % (table.name, c.name, (table.schema or 'public'), table.name, c.name))
                    elif bind.dialect.__class__.__name__ == 'SQLiteDialect':
                        bind.execute(select([func.AddGeometryColumn(table.name, c.name, c.type.srid, c.type.name, c.type.dimension)], autocommit=True))
                        if c.type.spatial_index:
                            bind.execute("CREATE INDEX idx_%s_%s ON %s(%s)" % (table.name, c.name, table.name, c.name))
                            bind.execute("VACUUM %s" % table.name)
                    elif bind.dialect.__class__.__name__ == 'MySQLDialect':
                        if c.type.spatial_index:
                            bind.execute("ALTER TABLE %s ADD %s %s NOT NULL" % (
                                table.name, c.name, c.type.name))
                            bind.execute("CREATE SPATIAL INDEX idx_%s_%s ON %s(%s)" % (table.name, c.name, table.name, c.name))
                        else:
                            bind.execute("ALTER TABLE %s ADD %s %s" % (
                                table.name, c.name, c.type.name))
                    else:
                      pass
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
    sfs = False
    if kw.has_key("sfs"): sfs = kw.pop("sfs")
    if sfs:
        return column_property(
                Column(*args, **kw), 
                extension=SpatialAttribute(), 
                comparator_factory=SFSComparator
        )
    return column_property(
        Column(*args, **kw), 
        extension=SpatialAttribute(), 
        comparator_factory=SQLMMComparator
    )

