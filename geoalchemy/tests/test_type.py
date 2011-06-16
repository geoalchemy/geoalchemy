from unittest import TestCase
from nose.tools import ok_, eq_

class TestType(TestCase):

    def test_cast_geometry(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import Geometry
        c = cast('', Geometry)
        eq_(str(c), 'CAST(:param_1 AS GEOMETRY)')

    def test_cast_point(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import Point
        c = cast('', Point)
        eq_(str(c), 'CAST(:param_1 AS POINT)')

    def test_cast_curve(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import Curve
        c = cast('', Curve)
        eq_(str(c), 'CAST(:param_1 AS CURVE)')

    def test_cast_linestring(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import LineString
        c = cast('', LineString)
        eq_(str(c), 'CAST(:param_1 AS LINESTRING)')

    def test_cast_polygon(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import Polygon
        c = cast('', Polygon)
        eq_(str(c), 'CAST(:param_1 AS POLYGON)')

    def test_cast_multipoint(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import MultiPoint
        c = cast('', MultiPoint)
        eq_(str(c), 'CAST(:param_1 AS MULTIPOINT)')

    def test_cast_multilinestring(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import MultiLineString
        c = cast('', MultiLineString)
        eq_(str(c), 'CAST(:param_1 AS MULTILINESTRING)')

    def test_cast_multipolygon(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import MultiPolygon
        c = cast('', MultiPolygon)
        eq_(str(c), 'CAST(:param_1 AS MULTIPOLYGON)')

    def test_cast_geometrycollection(self):
        from sqlalchemy import cast
        from geoalchemy.geometry import GeometryCollection
        c = cast('', GeometryCollection)
        eq_(str(c), 'CAST(:param_1 AS GEOMETRYCOLLECTION)')



