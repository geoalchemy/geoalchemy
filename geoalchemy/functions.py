from sqlalchemy.sql.expression import Function
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import func, literal


def _parse_clause(clause, compiler):
    """This method is used to translate a clause element (geometries, functions, ..).
    According to the type of the clause, a conversion to the database geometry type is added or
    the column clause (column name) is returned.
        
    """
    from geoalchemy.base import SpatialElement, WKTSpatialElement, WKBSpatialElement, DBSpatialElement, GeometryBase, _to_gis
    
    if hasattr(clause, '__clause_element__'):
        # for example a column name
        return clause.__clause_element__()
    elif isinstance(clause, Function):
        # for cascaded functions
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
        
    return literal(_to_gis(clause), GeometryBase)


def __get_function(element, compiler):
    """For elements of type _base_function, the database specific function name 
    is looked up and a executable Function object is created with this name.
    """
    from geoalchemy.dialect import DialectManager 
    database_dialect = DialectManager.get_spatial_dialect(compiler.dialect)
    function_name = database_dialect.get_function_name(element.__class__)
    
    # getattr(func, function_name) is like calling func.(the value of function_name)
    return getattr(func, function_name)
    
# Functions without additional argument
class _base_function(Function):
    """Represents a database function that can be called without additional argument.
    For example: AsText(geometry)
        session.scalar(s.geom.wkt)
        session.scalar(functions.wkt(..))
    """
    
    def __init__(self, clause, *clauses, **kw):
        self.clause = clause
        
        Function.__init__(self, self.__class__.__name__, *clauses, **kw)
        
    def __call__(self):
        return self
    
@compiles(_base_function)
def __compile_base_function(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    return compiler.process(function(_parse_clause(element.clause, compiler)))


# Functions with one additional argument
class _function_with_argument(_base_function):
    """Represents a database function that must be called with one additional argument.
    When the function is used on a geometry column (r.geom.point_n(2) or Road.geom.point_n(2)),
    the argument is set using __call__.
    When the function is called directly (functions.point_n(..., 2)),
    the argument is set using the constructor.
    
    For example: PointN(geometry, n)
        session.scalar(s.geom.point_n(2))
        session.scalar(functions.point_n(..., 2))
    """
    def __init__(self, clause=None, argument=None, *clauses, **kw):
        self.argument = argument
        
        _base_function.__init__(self, clause, self.__class__.__name__, *clauses, **kw)
        
    def __call__(self, argument=None):
        if argument is not None:
            # __call__ may also be called inside the SQLAlchemy compiler,
            # therefore only set the argument, if it is not None.
            self.argument = argument
        return self 
    
@compiles(_function_with_argument)
def __compile_function_with_argument(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    return compiler.process(function(_parse_clause(element.clause, compiler), element.argument))


# Functions with two additional arguments
class _function_with_two_arguments(_base_function):
    """Same as _function_with_argument but with two arguments.
    
    For example: Buffer(geometry, m, n)
        session.scalar(s.geom.buffer(10, 2))
        session.scalar(functions.buffer(..., 10, 2))
    """
    def __init__(self, clause=None, argument_one=None, argument_two=None, *clauses, **kw):
        self.argument_one = argument_one
        self.argument_two = argument_two
        
        _base_function.__init__(self, clause, self.__class__.__name__, *clauses, **kw)
        
    def __call__(self, argument_one=None, argument_two=None):
        if argument_one is not None:
            self.argument_one = argument_one
        
        if argument_two is not None:
            self.argument_two = argument_two
            
        return self 
    
@compiles(_function_with_two_arguments)
def __compile_function_with_two_arguments(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    if element.argument_one is not None and element.argument_two is not None:
        return compiler.process(function(_parse_clause(element.clause, compiler), element.argument_one, element.argument_two))
    elif element.argument_one is not None:
        return compiler.process(function(_parse_clause(element.clause, compiler), element.argument_one))
    else:
        return compiler.process(function(_parse_clause(element.clause, compiler)))


# Functions that accept two geometries
class _relation_function(_base_function):
    """Represents a function that accepts two geometries.
    
    For example: Touches(geometry_a, geometry_b)
        session.scalar(s.geom.touches(...))
        session.scalar(functions.touches(..., ...))
    """
    def __init__(self, clause=None, other_clause=None, *clauses, **kw):
        self.other_clause = other_clause
        
        _base_function.__init__(self, clause, self.__class__.__name__, *clauses, **kw)
        
    def __call__(self, other_clause=None):
        if other_clause is not None:
            self.other_clause = other_clause
        return self      

@compiles(_relation_function)
def __compile_relation_function(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    return compiler.process(function(_parse_clause(element.clause, compiler), 
                                     _parse_clause(element.other_clause, compiler)))


# Functions that accept two geometries and one additional argument
class _relation_function_with_argument(_base_function):
    """Same as _relation_function but with one additional argument.
    
    For example: DWithin(geometry_a, geometry_b, n)
        session.scalar(s.geom.within_distance(..., 10))
        session.scalar(functions.within_distance(..., ..., 10))
    """
    def __init__(self, clause=None, other_clause=None, argument=None, *clauses, **kw):
        self.other_clause = other_clause
        self.argument = argument
        
        _base_function.__init__(self, clause, self.__class__.__name__, *clauses, **kw)
        
    def __call__(self, other_clause=None, argument=None):
        if other_clause is not None:
            self.other_clause = other_clause
        
        if argument is not None:
            self.argument = argument
            
        return self      

@compiles(_relation_function_with_argument)
def __compile_relation_function_with_argument(element, compiler, **kw):
    function = __get_function(element, compiler)
    
    return compiler.process(function(_parse_clause(element.clause, compiler), 
                                     _parse_clause(element.other_clause, compiler), element.argument))


# Functions that implement OGC SFS or SQL/MM and that are supported by most databases

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
class point_n(_function_with_argument):
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
class buffer(_function_with_two_arguments):
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
class transform(_function_with_argument):
    pass

# Equals
class equals(_relation_function):
    pass

# Distance
class distance(_relation_function):
    pass

# WithinDistance
class within_distance(_relation_function_with_argument):
    pass

# Disjoint
class disjoint(_relation_function):
    pass

# Intersects
class intersects(_relation_function):
    pass

# Touches
class touches(_relation_function):
    pass

# Crosses
class crosses(_relation_function):
    pass

# Within
class within(_relation_function):
    pass

# Overlaps
class overlaps(_relation_function):
    pass

# Contains
class gcontains(_relation_function):
    pass

# Covers
class covers(_relation_function):
    pass

# CoveredBy
class covered_by(_relation_function):
    pass

# Intersection
class intersection(_relation_function):
    pass
        