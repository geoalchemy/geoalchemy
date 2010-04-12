from unittest import TestCase
from nose.tools import ok_, raises

from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

from geoalchemy.dialect import DialectManager
from geoalchemy.postgis import PGSpatialDialect
from geoalchemy.mysql import MySQLSpatialDialect
from geoalchemy.spatialite import SQLiteSpatialDialect

class TestDialectManager(TestCase):

    def test_get_spatial_dialect(self):
        spatial_dialect = DialectManager.get_spatial_dialect(PGDialect_psycopg2())
        ok_(isinstance(spatial_dialect, PGSpatialDialect))
        ok_(isinstance(DialectManager.get_spatial_dialect(MySQLDialect()), MySQLSpatialDialect))
        ok_(isinstance(DialectManager.get_spatial_dialect(SQLiteDialect()), SQLiteSpatialDialect))
        spatial_dialect2 = DialectManager.get_spatial_dialect(PGDialect_psycopg2())
        ok_(spatial_dialect is spatial_dialect2, "only one instance per dialect should be created")
    
    @raises(NotImplementedError)
    def test_get_spatial_dialect_unknown_dialect(self):
        DialectManager.get_spatial_dialect(OracleDialect())