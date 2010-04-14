from sqlalchemy.sql.expression import Function, ClauseElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import func, literal


def _parse_clause(clause, compiler):
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
        if isinstance(clause, WKTSpatialElement):
            return func.GeomFromText(literal(clause.desc, GeometryBase), clause.srid)
        if isinstance(clause, WKBSpatialElement):
            return func.GeomFromWKB(literal(clause.desc, GeometryBase), clause.srid)
        if isinstance(clause, DBSpatialElement):
            return literal(clause.desc, GeometryBase)    
        return func.GeomFromWKB(literal(clause.desc.desc, GeometryBase), clause.desc.srid)
    elif isinstance(clause, basestring):
        wkt = WKTSpatialElement(clause)
        return func.GeomFromText(literal(wkt, GeometryBase), wkt.srid)
    
    # for raw parameters    
    return literal(clause)


def __get_function(element, compiler):
    """For elements of type _base_function, the database specific function name 
    is looked up and a executable Function object is created with this name.
    """
    from geoalchemy.dialect import DialectManager 
    database_dialect = DialectManager.get_spatial_dialect(compiler.dialect)
    function_name = database_dialect.get_function_name(element.__class__)
    
    # getattr(func, function_name) is like calling func.(the value of function_name)
    return getattr(func, function_name)
    
class _base_function(Function):
    """Represents a database function.
    
    When the function is used on a geometry column (r.geom.point_n(2) or Road.geom.point_n(2)),
    an additional argument is set using __call__. The column or geometry the function is called on 
    is stored inside the constructor.
    When the function is called directly (functions.point_n(..., 2)),
    all arguments are set using the constructor.
    """
    
    def __init__(self, *arguments):
        self.arguments = arguments
        
        Function.__init__(self, self.__class__.__name__)
        
    def __call__(self, *arguments):
        if len(arguments) > 0:
            self.arguments =  self.arguments + arguments
            
        return self

@compiles(_base_function)
def __compile_base_function(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    params = [_parse_clause(argument, compiler) for argument in element.arguments]
    
    return compiler.process(function(*params))

class functions:
    """Functions that implement OGC SFS or SQL/MM and that are supported by most databases
    """
    
    # AsText
    class wkt(_base_function):
        pass
    
    # AsBinary
    class wkb(_base_function):
        pass
    
    # Dimension
    class dimension(_base_function):
        pass
    
    # SRID
    class srid(_base_function):
        pass
    
    # GeometryType
    class geometry_type(_base_function):
        pass
    
    # IsEmpty
    class is_empty(_base_function):
        pass
    
    # IsSimple
    class is_simple(_base_function):
        pass
    
    # IsClosed
    class is_closed(_base_function):
        pass
    
    # IsRing
    class is_ring(_base_function):
        pass
    
    # NumPoints
    class num_points(_base_function):
        pass
    
    # PointN
    class point_n(_base_function):
        pass
    
    # Length
    class length(_base_function):
        pass
    
    # Area
    class area(_base_function):
        pass
    
    # X
    class x(_base_function):
        pass
    
    # Y
    class y(_base_function):
        pass
    
    # Centroid
    class centroid(_base_function):
        pass
    
    # Boundary
    class boundary(_base_function):
        pass
    
    # Buffer
    class buffer(_base_function):
        pass
    
    # ConvexHull
    class convex_hull(_base_function):
        pass
    
    # Envelope
    class envelope(_base_function):
        pass
    
    # StartPoint
    class start_point(_base_function):
        pass
    
    # EndPoint
    class end_point(_base_function):
        pass
    
    # Transform
    class transform(_base_function):
        pass
    
    # Equals
    class equals(_base_function):
        pass
    
    # Distance
    class distance(_base_function):
        pass
    
    # DWithin
    class within_distance(_base_function):
        pass
    
    # Disjoint
    class disjoint(_base_function):
        pass
    
    # Intersects
    class intersects(_base_function):
        pass
    
    # Touches
    class touches(_base_function):
        pass
    
    # Crosses
    class crosses(_base_function):
        pass
    
    # Within
    class within(_base_function):
        pass
    
    # Overlaps
    class overlaps(_base_function):
        pass
    
    # Contains
    class gcontains(_base_function):
        pass
    
    # Covers
    class covers(_base_function):
        pass
    
    # CoveredBy
    class covered_by(_base_function):
        pass
    
    # Intersection
    class intersection(_base_function):
        pass
    
    # like DWithin, but MBR may be used (for MySQL)
    class _within_distance(_base_function):
        pass

@compiles(functions._within_distance)
def __compile__within_distance(element, compiler, **kw):
    from geoalchemy.dialect import DialectManager 
    database_dialect = DialectManager.get_spatial_dialect(compiler.dialect)
    function = database_dialect.get_function_name(element.__class__)
    
    return compiler.process(function(compiler, _parse_clause(element.arguments[0], compiler), 
                                     _parse_clause(element.arguments[1], compiler), element.arguments[2]))