from sqlalchemy.sql.expression import Function, ClauseElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import literal
from sqlalchemy.types import NullType, TypeDecorator
import types
import re

WKT_REGEX = re.compile('.*\\(.*\\).*')

def parse_clause(clause, compiler):
    """This method is used to translate a clause element (geometries, functions, ..).
    According to the type of the clause, a conversion to the database geometry type is added or
    the column clause (column name) or the cascaded clause element is returned.
        
    """
    from geoalchemy.base import SpatialElement, WKTSpatialElement, WKBSpatialElement, DBSpatialElement, GeometryBase
    
    if hasattr(clause, '__clause_element__'):
        # for example a column name
        return clause.__clause_element__()
    elif isinstance(clause, ClauseElement):
        # for cascaded clause elements, like other functions
        return clause
    elif isinstance(clause, SpatialElement):
        if isinstance(clause, (WKTSpatialElement, WKBSpatialElement)):
            return clause
        if isinstance(clause, DBSpatialElement):
            return literal(clause.desc, GeometryBase)    
        return clause.desc
    elif isinstance(clause, basestring) and WKT_REGEX.match(clause):
        return WKTSpatialElement(clause)
    
    # for raw parameters    
    return literal(clause)


def _get_function(element, compiler, params, within_column_clause):
    """For elements of type BaseFunction, the database specific function data 
    is looked up and a executable sqlalchemy.sql.expression.Function object 
    is returned.
    """
    from geoalchemy.dialect import DialectManager 
    database_dialect = DialectManager.get_spatial_dialect(compiler.dialect)
    function_data = database_dialect.get_function(element.__class__)
    
    if isinstance(function_data, list):
        """if we have a list of function names, create cascaded Function objects
        for all function names in the list::
        
            ['TO_CHAR', 'SDO_UTIL.TO_WKTGEOMETRY'] --> TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(..))
        """
        function = None
        for name in reversed(function_data):
            packages = name.split('.')
            
            if function is None:
                """for the innermost function use the passed-in parameters as argument,
                otherwise use the prior created function
                """
                args = params
            else:
                args = [function]
                
            function = Function(packages.pop(-1), 
                        packagenames=packages,
                        *args
                        )
        
        return function
    
    elif isinstance(function_data, types.FunctionType):
        """if we have a function, call this function with the parameters and return the
        created Function object
        """
        return function_data(params, within_column_clause, **(element.flags))
    
    else:
        packages = function_data.split('.')
        
        return Function(packages.pop(-1), 
                        packagenames=packages, 
                        *params
                        )

    
class BaseFunction(Function):
    """Represents a database function.
    
    When the function is used on a geometry column (r.geom.point_n(2) or Road.geom.point_n(2)),
    additional arguments are set using __call__. The column or geometry the function is called on 
    is stored inside the constructor (see base.SpatialComparator.__getattr__()).
    When the function is called directly (functions.point_n(..., 2)),
    all arguments are set using the constructor.
    """
    
    def __init__(self, *arguments, **kwargs):
        self.arguments = arguments
        self.flags = kwargs.copy()
        
        Function.__init__(self, self.__class__.__name__, **kwargs)
        
    def __call__(self, *arguments, **kwargs):
        if len(arguments) > 0:
            self.arguments =  self.arguments + arguments
        
        if len(kwargs) > 0:
            self.flags.update(kwargs)
        
        return self
    

@compiles(BaseFunction)
def __compile_base_function(element, compiler, **kw):
    
    params = [parse_clause(argument, compiler) for argument in element.arguments]
    
    from geoalchemy.dialect import DialectManager 
    database_dialect = DialectManager.get_spatial_dialect(compiler.dialect)

    if not database_dialect.is_member_function(element.__class__):
        function = _get_function(element, compiler, params, kw.get('within_columns_clause', False))
        return compiler.process(function)
    else:
        geometry = params.pop(0)
        
        function_name = database_dialect.get_function(element.__class__)
        
        return "%s.%s(%s)" % (
            compiler.process(geometry),
            function_name,
            ", ".join([compiler.process(e) for e in params]) 
            ) 

class functions:
    """Functions that implement OGC SFS or SQL/MM and that are supported by most databases
    """
    
    class wkt(BaseFunction):
        """AsText(g)"""
        pass

    class wkb(BaseFunction):
        """AsBinary(g)"""
        def __init__(self, *arguments):
            BaseFunction.__init__(self, type_=_WKBType, *arguments)
        
    class dimension(BaseFunction):
        """Dimension(g)"""
        pass
    
    class srid(BaseFunction):
        """SRID(g)"""
        pass
    
    class geometry_type(BaseFunction):
        """GeometryType(g)"""
        pass
    
    class is_empty(BaseFunction):
        """IsEmpty(g)"""
        pass
    
    class is_simple(BaseFunction):
        """IsSimple(g)"""
        pass
    
    class is_closed(BaseFunction):
        """IsClosed(g)"""
        pass
    
    class is_ring(BaseFunction):
        """IsRing(g)"""
        pass
    
    class num_points(BaseFunction):
        """NumPoints(g)"""
        pass
    
    class point_n(BaseFunction):
        """PointN(g, n)"""
        pass
    
    class length(BaseFunction):
        """Length(g)"""
        pass
    
    class area(BaseFunction):
        """Area(g)"""
        pass
    
    class x(BaseFunction):
        """X(g)"""
        pass
    
    class y(BaseFunction):
        """Y(g)"""
        pass
    
    class centroid(BaseFunction):
        """Centroid(g)"""
        pass
    
    class boundary(BaseFunction):
        """Boundary"""
        pass
    
    class buffer(BaseFunction):
        """Buffer(g, n)"""
        pass
    
    class convex_hull(BaseFunction):
        """ConvexHull(g)"""
        pass
    
    class envelope(BaseFunction):
        """Envelope(g)"""
        pass
    
    class start_point(BaseFunction):
        """StartPoint(g)"""
        pass
    
    class end_point(BaseFunction):
        """EndPoint(g)"""
        pass
    
    class transform(BaseFunction):
        """Transform(g, srid)"""
        pass

    class equals(BaseFunction):
        """Equals(g1, g2)"""
        pass
    
    class distance(BaseFunction):
        """Distance(g1, g2)"""
        pass
    
    class within_distance(BaseFunction):
        """DWithin(g1, g2, d)"""
        pass
    
    class disjoint(BaseFunction):
        """Disjoint(g1, g2)"""
        pass
    
    class intersects(BaseFunction):
        """Intersects(g1, g2)"""
        pass
    
    class touches(BaseFunction):
        """Touches(g1, g2)"""
        pass
    
    class crosses(BaseFunction):
        """Crosses(g1, g2)"""
        pass
    
    class within(BaseFunction):
        """Within(g1, g2)"""
        pass
    
    class overlaps(BaseFunction):
        """Overlaps(g1, g2)"""
        pass
    
    class gcontains(BaseFunction):
        """Contains(g1, g2)"""
        pass
    
    class covers(BaseFunction):
        """Covers(g1, g2)"""
        pass
    
    class covered_by(BaseFunction):
        """CoveredBy(g1, g2)"""
        pass
    
    class intersection(BaseFunction):
        """Intersection(g1, g2)"""
        pass


class _WKBType(TypeDecorator):
    """A helper type which makes sure that the WKB sequence returned from queries like 
    'session.scalar(r.road_geom.wkb)', has the same type as the attribute 'geom_wkb' which
    is filled when querying an object of a mapped class, for example with 'r = session.query(Road).get(1)'::
    
        eq_(session.scalar(self.r.road_geom.wkb), self.r.road_geom.geom_wkb)
    
    This type had to be introduced, because for Oracle 'SDO_UTIL.TO_WKBGEOMETRY(..)' returned the type 
    cx_Oracle.LOB and not a buffer.
    
    To modify the behavior of 'process_result_value' for a specific database dialect, overwrite the 
    method 'process_wkb' in that dialect.
    
    This class is used inside :class:`functions.wkb`.
    
    """

    impl = NullType

    def process_result_value(self, value, dialect):
        if value is not None:
            from geoalchemy.dialect import DialectManager 
            database_dialect = DialectManager.get_spatial_dialect(dialect)
            
            return database_dialect.process_wkb(value)
        else:
            return value

    def copy(self):
        return _WKBType()
    
