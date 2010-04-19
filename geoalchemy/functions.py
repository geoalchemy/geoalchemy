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
    
    class wkt(BaseFunction):
        """AsText(g)"""
        pass
    
    class wkb(BaseFunction):
        """AsBinary(g)"""
        pass
    
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
    