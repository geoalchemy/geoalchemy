# -*- coding: utf-8 -*-
from sqlalchemy import Column, select, func, literal
from sqlalchemy.orm import column_property
from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import expression
from geoalchemy.base import SpatialElement, _to_gis, WKBSpatialElement, GeometryBase as Geometry, SpatialComparator
from geoalchemy.comparator import SQLMMComparator, SQLMMComparatorFunctions
from geoalchemy.dialect import SpatialDialect 

class PGComparatorFunctions(SQLMMComparatorFunctions):
    """This class contains functions supported by PostGIS that are not part of SQL MM
    """

    # OGC Geometry Functions
    @property
    def svg(self):
        return func.ST_AsSVG(self._geom_from_wkb())

    @property
    def kml(self):
        return func.ST_AsKML(self._geom_from_wkb())

    @property
    def geojson(self):
        return func.ST_AsGeoJson(self._geom_from_wkb())
    
    # override the __eq__() operator
    def __eq__(self, other):
        return self.__clause_element__().op('~=')(self._geom_from_wkb(other))

    # add a custom operator
    def intersects(self, other):
        return self.__clause_element__().op('&&')(self._geom_from_wkb(other)) 

class PGComparator(PGComparatorFunctions, SpatialComparator):
    """Comparator class used for PostGIS
    """


class PGSpatialElement(PGComparatorFunctions, SpatialElement):
    """Represents a geometry value."""
    pass


class PGPersistentSpatialElement(PGSpatialElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc


class PGSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for PostGIS."""
    
    def get_comparator(self):
        return PGComparator
    
    def process_result(self, wkb_element):
        return PGPersistentSpatialElement(wkb_element)
    
    def handle_ddl_before_drop(self, bind, table, column):
        bind.execute(select([func.DropGeometryColumn((table.schema or 'public'), table.name, column.name)]).execution_options(autocommit=True))
    
    def handle_ddl_after_create(self, bind, table, column):    
        bind.execute(select([func.AddGeometryColumn((table.schema or 'public'), 
                                                    table.name, 
                                                    column.name, 
                                                    column.type.srid, 
                                                    column.type.name, 
                                                    column.type.dimension)]).execution_options(autocommit=True))
        if column.type.spatial_index:
            bind.execute("CREATE INDEX idx_%s_%s ON %s.%s USING GIST (%s GIST_GEOMETRY_OPS)" % 
                            (table.name, column.name, (table.schema or 'public'), table.name, column.name))
            