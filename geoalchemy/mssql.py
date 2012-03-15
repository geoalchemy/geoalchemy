# -*- coding: utf-8 -*-

import warnings
from sqlalchemy import func, cast, exc
from sqlalchemy.types import VARBINARY
from sqlalchemy.sql.expression import text
from sqlalchemy.ext.compiler import compiles
from geoalchemy.base import WKBSpatialElement, WKTSpatialElement, PersistentSpatialElement, DBSpatialElement, SpatialComparator
from geoalchemy.dialect import SpatialDialect
from geoalchemy.functions import functions, BaseFunction, BooleanFunction
from geoalchemy.geometry import Geometry

u"""
:mod:`geoalchemy.mssql` -- MS SQL Server 2008 Spatial Dialect
=============================================================

This module provides the :class:`~geoalchemy.dialect.SpatialDialect` for
connecting to MS SQL Server 2008 databases that contain spatial data.

Most OGC functionality is fully supported, but there are a few exceptions.
See :class:`geoalchemy.mssql.MSSpatialDialect` for details.

There are also a number of MS SQL Server 2008 specific functions that are
available. See :class:`geoalchemy.mssql.ms_functions` for details.

To include a spatial index the bounding box for the data must be defined in the
column definition. If no bounding box is specified then no spatial index will
be created.

>>> class Road(Base):
>>>    __tablename__ = 'ROADS'
>>>
>>>    road_id = Column(Integer, primary_key=True)
>>>    road_name = Column(String(255))
>>>    road_geom = GeometryColumn(Geometry(2, bounding_box='(xmin=-180, ymin=-90, xmax=180, ymax=90)'), nullable=False)

There is also currently no support for inserting `None` geometries. You must
use :data:`geoalchemy.mssql.MS_SPATIAL_NULL` to explicitly insert NULL
geometries.

It is not possible to restrict the geometry columns to a specific geometry
type.

.. moduleauthor:: Mark Hall <Mark.Hall@nationalarchives.gov.uk>
"""

MS_SPATIAL_NULL = text('null')
"""There is a bug causing errors when trying to insert None values into
nullable columns. Use this constant instead."""

class MSComparator(SpatialComparator):
    """Comparator class used for MS SQL Server 2008
    """
    def __getattr__(self, name):
        try:
            return SpatialComparator.__getattr__(self, name)
        except AttributeError:
            return getattr(ms_functions, name)(self)

class MSPersistentSpatialElement(PersistentSpatialElement):
    """Represents a Geometry as loaded from an MS SQL Server 2008 database.
    """
    def __init__(self, desc):
        self.desc = desc
    
    def __getattr__(self, name):
        try:
            return PersistentSpatialElement.__getattr__(self, name)
        except AttributeError:
            return getattr(ms_functions, name)(self)

@compiles(VARBINARY, 'mssql')
def compile_varbinary(element, compiler, **kw):
    """Compiler function to handle VARBINARY(max).
    
    Should be removed when SQLAlchemy supports this natively.
    """
    if str(element.length).lower() == 'max':
        return "VARBINARY(max)"
    else:
        return compiler.visit_VARBINARY(element, **kw)

def CastDBSpatialElementFunction():
    """Wrapper required for handling the :class:`geoalchemy.base.DBSpatialElement`.
    
    This adds the necessary casts so that a :class:`geoalchemy.base.DBSpatialElement`
    is recognised as a spatial element by the SQL Server. The reason for this
    is that in SQL Server the geometry data is a subclass of VARBINARY and the
    sub-classing information gets lost between queries. The cast provided by
    this function guarantees that SQL Server knows the data is a geometry.
    """
    def function_handler(params, within_column_clause):
        return cast(cast(params[0], VARBINARY('max')), Geometry)
    
    return function_handler

class ms_functions(functions):
    """MS SQL Server specific geometry functions.
    """
    class gml(BaseFunction):
        """g.AsGML()
        
        GML representation of the geometry. Does not include the SRS.
        """
        pass
    
    class text_zm(BaseFunction):
        """p.AsTextZM()
        
        The :class:`~geoalchemy.geometry.Point` WKT representation augmented
        with Z and M values. Only valid for :class:`~geoalchemy.geometry.Point`
        geometries.
        """
        pass
    
    class buffer_with_tolerance(BaseFunction):
        """g.BufferWithTolerance(distance, tolerance, relative)
        
        Creates a buffer with the given tolerance values.
        """
        pass
    
    class filter(BaseFunction):
        """g1.Filter(g2)
        
        An index-only intersection query. Can return false positives. If no
        index is defined then behaves like g1.intersection(g2).
        """
        pass
    
    class instance_of(BaseFunction):
        """g.InstanceOf(geometry_type_name)
        
        Tests whether the geometry is of the given geometry type.
        """
        pass
    
    class m(BaseFunction):
        """p.M
        
        Returns the M value for the given :class:`~geoalchemy.geometry.Point`.
        Only valid for :class:`~geoalchemy.geometry.Point` geometries.
        """
        pass
    
    class make_valid(BaseFunction):
        """g.MakeValid()
        
        Converts an invalid :class:`~geoalchemy.geometry.Geometry` into a
        valid one. This can return a different type of
        :class:`~geoalchemy.geometry.Geometry`.
        """
        pass
    
    class reduce(BaseFunction):
        """g.Reduce(tolerance)
        
        Returns an approximation of the :class:`~geoalchemy.geometry.Geometry`
        using the Douglas-Peucker algorithm.
        """
        pass
    
    class to_string(BaseFunction):
        """g.ToString()
        
        Equivalent to :class:`~geoalchemy.mssql.ms_functions.text_zm` except
        that for NULL geometries it will return the string 'NULL'.
        """
        pass
    
    class z(BaseFunction):
        """p.Z
        
        Returns the M value for the given :class:`~geoalchemy.geometry.Point`.
        Only valid for :class:`~geoalchemy.geometry.Point` geometries.
        """
        pass

class MSSpatialDialect(SpatialDialect):
    """The :class:`~geoalchemy.dialect.SpatialDialect` for accessing MS
    SQL Server 2008 spatial data.
    
    For the standard OGC functions there are a few differences in the SQL
    Server 2008 implementation that need to be taken into account:
    
    * g.centroid -- Only returns results for :class:`~geoalchemy.geometry.Polygon`
      and :class:`~geoalchemy.geometry.MultiPolygon`. Returns 'NULL' for all
      other :class:`~geoalchemy.geometry.Geometry`
    * g.envelope -- Will always return a :class:`~geoalchemy.geometry.Polygon`
      regardless of the type of :class:`~geoalchemy.geometry.Geometry` it
      was called on
    * g.buffer -- Only supports the buffer distance as a parameter
    
    Some standard functions are not available:
    
    * g.transform
    * g.within_distance
    * g.covers
    * g.covers_by
    * g.intersection
    
    For SQL Server 2008 specific functions see :class:`~geoalchemy.mssql.ms_functions`.
    """
    __functions = {
                   functions.wkt: 'STAsText',
                   WKTSpatialElement: 'geometry::STGeomFromText',
                   functions.wkb: 'STAsBinary',
                   WKBSpatialElement : 'geometry::STGeomFromWKB',
                   DBSpatialElement : CastDBSpatialElementFunction(),
                   functions.dimension : 'STDimension',
                   functions.srid : 'STSrid',
                   functions.geometry_type : 'STGeometryType',
                   functions.is_empty : 'STIsEmpty',
                   functions.is_simple : 'STIsSimple',
                   functions.is_closed : 'STIsClosed',
                   functions.is_ring : 'STIsRing',
                   functions.num_points : 'STNumPoints',
                   functions.point_n : 'STPointN',
                   functions.length : 'STLength',
                   functions.area : 'STArea',
                   functions.x : 'STX',
                   functions.y : 'STY',
                   functions.centroid : 'STCentroid',
                   functions.boundary : 'STBoundary',
                   functions.buffer : 'STBuffer',
                   functions.convex_hull : 'STConvexHull',
                   functions.envelope : 'STEnvelope',
                   functions.start_point : 'STStartPoint',
                   functions.end_point : 'STEndPoint',
                   functions.transform : None,
                   functions.equals : BooleanFunction(func.STEquals, 1),
                   functions.distance : 'STDistance',
                   functions.within_distance : None,
                   functions.disjoint : BooleanFunction(func.STDisjoint, 1),
                   functions.intersects : BooleanFunction(func.STIntersects, 1),
                   functions.touches : BooleanFunction(func.STTouches, 1),
                   functions.crosses : BooleanFunction(func.STCrosses, 1),
                   functions.within : BooleanFunction(func.STWithin, 1),
                   functions.overlaps : BooleanFunction(func.STOverlaps, 1),
                   functions.gcontains : BooleanFunction(func.STContains, 1),
                   functions.covers : None,
                   functions.covered_by : None,
                   functions.intersection : None,
                   functions.is_valid : BooleanFunction(func.STIsValid, 1),
                   # Not tested
                   # functions.aggregate_union : 'STUnion', 
                   ms_functions.gml : 'AsGml',
                   ms_functions.text_zm : 'AsTextZM',
                   ms_functions.buffer_with_tolerance : 'BufferWithTolerance',
                   ms_functions.filter : BooleanFunction(func.Filter, 1),
                   ms_functions.instance_of : BooleanFunction(func.InstanceOf, 1),
                   ms_functions.m : 'M',
                   ms_functions.make_valid : 'MakeValid',
                   ms_functions.reduce : 'Reduce',
                   ms_functions.to_string : 'ToString',
                   ms_functions.z : 'Z'
                  }
    
    __member_functions = (
                          functions.wkt,
                          functions.wkb,
                          functions.dimension,
                          functions.geometry_type,
                          functions.is_empty,
                          functions.is_simple,
                          functions.is_closed,
                          functions.is_ring,
                          functions.num_points,
                          functions.point_n,
                          functions.length,
                          functions.area,
                          functions.centroid,
                          functions.boundary,
                          functions.buffer,
                          functions.convex_hull,
                          functions.envelope,
                          functions.start_point,
                          functions.end_point,
                          functions.equals,
                          functions.distance,
                          functions.disjoint,
                          functions.intersects,
                          functions.touches,
                          functions.crosses,
                          functions.within,
                          functions.overlaps,
                          functions.gcontains,
                          functions.is_valid,
                          ms_functions.gml,
                          ms_functions.text_zm,
                          ms_functions.buffer_with_tolerance,
                          ms_functions.filter,
                          ms_functions.instance_of,
                          ms_functions.make_valid,
                          ms_functions.reduce,
                          ms_functions.to_string
                         )
    
    __properties = (
                    functions.srid,
                    functions.x,
                    functions.y,
                    ms_functions.m,
                    ms_functions.z
                   )
    
    def _get_function_mapping(self):
        return MSSpatialDialect.__functions
    
    def process_result(self, value, type):
        return MSPersistentSpatialElement(WKBSpatialElement(value, type.srid))
    
    def handle_ddl_after_create(self, bind, table, column):
        nullable = "NOT NULL"
        if column.nullable:
            nullable = "NULL"
            
        bind.execute("ALTER TABLE [%s].[%s] ADD [%s] %s %s" %
                     (table.schema or 'dbo', table.name, column.name, 'GEOMETRY', nullable))
        
        if column.type.spatial_index:
            if "bounding_box" in column.type.kwargs:
                bind.execute("CREATE SPATIAL INDEX [%s_%s] ON [%s].[%s]([%s]) WITH (BOUNDING_BOX = %s)" %
                             (table.name, column.name, table.schema or 'dbo', table.name, column.name, column.type.kwargs["bounding_box"]))
            else:
                warnings.warn("No bounding_box given for '[%s].[%s].[%s]' no spatial index will be created." %
                              (table.schema or 'dbo', table.name, column.name), 
                              exc.SAWarning, stacklevel=3)
            
    def is_member_function(self, function_class):
        return function_class in self.__member_functions
    
    def is_property(self, function_class):
        return function_class in self.__properties
    
