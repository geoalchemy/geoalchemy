from sqlalchemy.sql.expression import Function, ClauseElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import func, literal


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
    """For elements of type BaseFunction, the database specific function name 
    is looked up and a executable sqlalchemy.sql.expression.Function object 
    is created with this name.
    """
    from geoalchemy.dialect import DialectManager 
    database_dialect = DialectManager.get_spatial_dialect(compiler.dialect)
    function_name = database_dialect.get_function_name(element.__class__)
    
    # getattr(func, function_name) is like calling func.(the value of function_name)
    return getattr(func, function_name)
    
class BaseFunction(Function):
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

@compiles(BaseFunction)
def __compile_base_function(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    params = [parse_clause(argument, compiler) for argument in element.arguments]
    
    return compiler.process(function(*params))

class functions:
    """Functions that implement OGC SFS or SQL/MM and that are supported by most databases
    """
    
    # AsText
    class wkt(BaseFunction):
        pass
    
    # AsBinary
    class wkb(BaseFunction):
        pass
    
    # Dimension
    class dimension(BaseFunction):
        pass
    
    # SRID
    class srid(BaseFunction):
        pass
    
    # GeometryType
    class geometry_type(BaseFunction):
        pass
    
    # IsEmpty
    class is_empty(BaseFunction):
        pass
    
    # IsSimple
    class is_simple(BaseFunction):
        pass
    
    # IsClosed
    class is_closed(BaseFunction):
        pass
    
    # IsRing
    class is_ring(BaseFunction):
        pass
    
    # NumPoints
    class num_points(BaseFunction):
        pass
    
    # PointN
    class point_n(BaseFunction):
        pass
    
    # Length
    class length(BaseFunction):
        pass
    
    # Area
    class area(BaseFunction):
        pass
    
    # X
    class x(BaseFunction):
        pass
    
    # Y
    class y(BaseFunction):
        pass
    
    # Centroid
    class centroid(BaseFunction):
        pass
    
    # Boundary
    class boundary(BaseFunction):
        pass
    
    # Buffer
    class buffer(BaseFunction):
        pass
    
    # ConvexHull
    class convex_hull(BaseFunction):
        pass
    
    # Envelope
    class envelope(BaseFunction):
        pass
    
    # StartPoint
    class start_point(BaseFunction):
        pass
    
    # EndPoint
    class end_point(BaseFunction):
        pass
    
    # Transform
    class transform(BaseFunction):
        pass
    
    # Equals
    class equals(BaseFunction):
        pass
    
    # Distance
    class distance(BaseFunction):
        pass
    
    # DWithin
    class within_distance(BaseFunction):
        pass
    
    # Disjoint
    class disjoint(BaseFunction):
        pass
    
    # Intersects
    class intersects(BaseFunction):
        pass
    
    # Touches
    class touches(BaseFunction):
        pass
    
    # Crosses
    class crosses(BaseFunction):
        pass
    
    # Within
    class within(BaseFunction):
        pass
    
    # Overlaps
    class overlaps(BaseFunction):
        pass
    
    # Contains
    class gcontains(BaseFunction):
        pass
    
    # Covers
    class covers(BaseFunction):
        pass
    
    # CoveredBy
    class covered_by(BaseFunction):
        pass
    
    # Intersection
    class intersection(BaseFunction):
        pass
    