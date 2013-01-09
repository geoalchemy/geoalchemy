import warnings

from sqlalchemy import Column, Table, exc
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.sql.expression import Alias
from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql.base import PGDialect

from geoalchemy.base import GeometryBase, _to_gis, SpatialComparator
from geoalchemy.dialect import DialectManager
from geoalchemy.functions import functions

class Geometry(GeometryBase):
    """Geometry column type. This is the base class for all other
    geometry types like Point, LineString, Polygon, etc.
    
    Converts bind/result values to/from a dialect specific persistent
    geometry value.
    
    """
    
    def result_processor(self, dialect, coltype=None):
        def process(value):
            if value is not None:
                return DialectManager.get_spatial_dialect(dialect).process_result(value, self)
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

    try:
        from sqlalchemy import event as _event
    except ImportError:
        # SQLAlchemy 0.6
        use_event = False
        columns_attribute = '_columns'
    else:
        # SQLALchemy 0.7
        use_event = True
        columns_attribute = 'columns'
    
    def __init__(self, table):
        if self.use_event:
            self._event.listen(table, 'before_create', self.before_create)
            self._event.listen(table, 'before_drop', self.before_drop)
            self._event.listen(table, 'after_create', self.after_create)
            self._event.listen(table, 'after_drop', self.after_drop)
        else:
            for e in ('before-create', 'after-create',
                      'before-drop', 'after-drop'):
                table.ddl_listeners[e].append(self)
        self._stack = []
        
    def __call__(self, event, table, bind):
        spatial_dialect = DialectManager.get_spatial_dialect(bind.dialect)
        if event in ('before-create', 'before-drop'):
            """Remove geometry column from column list (table._columns), so that it 
            does not show up in the create statement ("create table tab (..)"). 
            Afterwards (on event 'after-create') restore the column list from self._stack.
            """
            regular_cols = [c for c in table.c if not isinstance(c.type, Geometry)]
            gis_cols = set(table.c).difference(regular_cols)
            self._stack.append(table.c)
            setattr(table, self.columns_attribute,
                    expression.ColumnCollection(*regular_cols))
            
            if event == 'before-drop':
                for c in gis_cols:
                    spatial_dialect.handle_ddl_before_drop(bind, table, c)
                
        elif event == 'after-create':
            setattr(table, self.columns_attribute, self._stack.pop())
            
            for c in table.c:
                if isinstance(c.type, Geometry):
                    spatial_dialect.handle_ddl_after_create(bind, table, c)

        elif event == 'after-drop':
            setattr(table, self.columns_attribute, self._stack.pop())

    def before_create(self, target, connection, **kw):
        self('before-create', target, connection)

    def before_drop(self, target, connection, **kw):
        self('before-drop', target, connection)

    def after_create(self, target, connection, **kw):
        self('after-create', target, connection)

    def after_drop(self, target, connection, **kw):
        self('after-drop', target, connection)


class SpatialAttribute(AttributeExtension):
    """Intercepts 'set' events on a mapped instance attribute and 
    converts the incoming value to a GIS expression.
    
    """
    
    def set(self, state, value, oldvalue, initiator):
        return _to_gis(value, self.__get_srid(initiator))
 
    def __get_srid(self, initiator):
        """Returns the SRID used for the geometry column that is connected
        to this SpatialAttribute instance."""
        try:
            return initiator.parent_token.columns[0].type.srid
        except Exception:
            return None
 
class GeometryExtensionColumn(Column):
    pass
        
@compiles(GeometryExtensionColumn)
def compile_column(element, compiler, **kw):
    if isinstance(element.table, (Table, Alias)):
        if kw.has_key("within_columns_clause") and kw["within_columns_clause"] == True:
            if element.type.wkt_internal:
                if isinstance(compiler.dialect, PGDialect):
                    return compiler.process(functions.wkt(element))
                warnings.warn("WKT Internal GeometryColumn type not "
                    "compatible with %s dialect. Defaulting back to WKB"
                    % compiler.dialect.name, exc.SAWarning)
            return compiler.process(functions.wkb(element))
        
    return compiler.visit_column(element, **kw)
     
            
def GeometryColumn(*args, **kw):
    """Define a declarative column property with GIS behavior.
    
    This just produces orm.column_property() with the appropriate
    extension and comparator_factory arguments.  The given arguments
    are passed through to Column.  The declarative module extracts
    the Column for inclusion in the mapped table.
    
    This method can also be used for non-declarative mappings to 
    set the properties for a geometry column when defining the mapping.
    
    """
    if kw.has_key("comparator"):
        comparator = kw.pop("comparator")
    else:
        comparator = SpatialComparator
    
    if isinstance(args[0], GeometryExtensionColumn):
        # if used for non-declarative, use the column of the table definition
        column = args[0]
        args = args[1:]
    else:
        # if used for declarative, create a new column
        column = GeometryExtensionColumn(*args, **kw) 
    
    return column_property(
        column, 
        extension=SpatialAttribute(), 
        comparator_factory=comparator
    )

