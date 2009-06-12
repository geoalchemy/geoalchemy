from unittest import TestCase
from binascii import b2a_hex
from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        Numeric, func, literal, select)
from sqlalchemy.orm import sessionmaker, column_property
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy.postgis import (Geometry, GeometryColumn, GeometryDDL,
	WKTSpatialElement)

engine = create_engine('postgres://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
session = sessionmaker(bind=engine)()
Base = declarative_base(metadata=metadata)

class Road(Base):
    __tablename__ = 'roads'

    road_id = Column(Integer, primary_key=True)
    road_name = Column(String)
    road_geom = GeometryColumn(Geometry(2))

class Lake(Base):
    __tablename__ = 'lakes'

    lake_id = Column(Integer, primary_key=True)
    lake_name = Column(String)
    lake_geom = GeometryColumn(Geometry(2))

class Spot(Base):
    __tablename__ = 'spots'

    spot_id = Column(Integer, primary_key=True)
    spot_height = Column(Numeric)
    spot_location = GeometryColumn(Geometry(2))

# enable the DDL extension, which allows CREATE/DROP operations
# to work correctly.  This is not needed if working with externally
# defined tables.    
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)
GeometryDDL(Spot.__table__)

class TestGeometry(TestCase):

    def setUp(self):

        metadata.drop_all()
        metadata.create_all()

        # Add objects.  We can use strings...
        session.add_all([
            Road(road_name='Jeff Rd', road_geom='SRID=-1;LINESTRING(191232 243118,191108 243242)'),
            Road(road_name='Geordie Rd', road_geom='SRID=-1;LINESTRING(191232 243118,191108 243242)'),
            Road(road_name='Paul St', road_geom='SRID=-1;LINESTRING(192783 228138,192612 229814)'),
            Road(road_name='Graeme Ave', road_geom='SRID=-1;LINESTRING(189412 252431,189631 259122)'),
            Road(road_name='Phil Tce', road_geom='SRID=-1;LINESTRING(190131 224148,190871 228134)'),
            Lake(lake_name='My Lake', lake_geom='SRID=-1;POLYGON((0 0,4 0,4 4,0 4,0 0))'),
            Lake(lake_name='Lake White', lake_geom='SRID=-1;POLYGON((18800 276600,189243 243747,190131 242324,192332 264553,190233 286654, 18800 276600))'),
            Lake(lake_name='Lake Blue', lake_geom='SRID=-1;POLYGON((192334 276600,193225 245576,1892234 242324,192332 264553,194233 256654, 192334 276600))'),
            Lake(lake_name='Lake Deep', lake_geom='SRID=-1;POLYGON((155937 276899, 157909 263969, 181796 268352, 155937 276899))'),
            Spot(spot_height=433.44, spot_location='SRID=-1;POINT(186677 248865)'),
            Spot(spot_height=308.04, spot_location='SRID=-1;POINT(192345 238865)'),
            Spot(spot_height=329.23, spot_location='SRID=-1;POINT(189224 227537)'),
            Spot(spot_height=407.22, spot_location='SRID=-1;POINT(194736 289654)'),
        ])

        # or use an explicit WKTSpatialElement (similar to saying func.GeomFromText())
        self.r = Road(road_name='Dave Cres', road_geom=WKTSpatialElement('SRID=-1;LINESTRING(198231 263418,198213 268322)', -1))
        session.add(self.r)
        session.commit()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()

    # Test Geometry Functions

    def test_wkt(self):
        assert session.scalar(self.r.road_geom.wkt) == 'LINESTRING(198231 263418,198213 268322)'

    def test_wkb(self):
        assert b2a_hex(session.scalar(self.r.road_geom.wkb)).upper() == '01020000000200000000000000B832084100000000E813104100000000283208410000000088601041'

    def test_svg(self):
        assert session.scalar(self.r.road_geom.svg) == 'M 198231 -263418 198213 -268322'

    def test_gml(self):
        assert session.scalar(self.r.road_geom.gml) == '<gml:LineString><gml:coordinates>198231,263418 198213,268322</gml:coordinates></gml:LineString>'

    def test_dimension(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.dimension) == 1
        assert session.scalar(l.lake_geom.dimension) == 2

    def test_geometry_type(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.geometry_type) == 'ST_LineString'
        assert session.scalar(l.lake_geom.geometry_type) == 'ST_Polygon'

    def test_is_empty(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.is_empty) == 0
        assert session.scalar(l.lake_geom.is_empty) == 0

    def test_is_simple(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.is_simple) == 1
        assert session.scalar(l.lake_geom.is_simple) == 1

    def test_is_closed(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.is_closed) == 0
        assert session.scalar(l.lake_geom.is_closed) == 1

    def test_is_ring(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.is_ring) == 0

    def test_persistent(self):
        assert str(self.r.road_geom) == "01020000000200000000000000B832084100000000E813104100000000283208410000000088601041"

    def test_eq(self):
        r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(189412 252431,189631 259122)').one()
        r3 = session.query(Road).filter(Road.road_geom == r1.road_geom).one()
        assert r1 is r2 is r3

    def test_intersects(self):
        r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert [(r.road_name, session.scalar(r.road_geom.wkt)) for r in session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()] == [('Graeme Ave', 'LINESTRING(189412 252431,189631 259122)')]

    def test_length(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.length) == 6694.5830340656803

    def test_area(self):
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(l.lake_geom.area) == 16

    def test_centroid(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.centroid) == '0101000000000000008C2207410000000004390F41'
        assert session.scalar(l.lake_geom.centroid) == '010100000000000000000000400000000000000040'

    def test_boundary(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.boundary) == '010400000002000000010100000000000000201F07410000000078D00E41010100000000000000F82507410000000090A10F41'

    def test_buffer(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.buffer(length=10.0, num_segs=8)) == '01030000000100000023000000F90FF60AA8250741746AF69D92A10F41D2A2F816AA250741699A662AA2A10F4183227521AF2507416F712004B1A10F411BD6D3F8B62507412ACC0A99BEA10F41E55CF04FC125074148FE8763CAA10F41B49910C1CD250741B95398EFD3A10F41867BCDD1DB25074122414FDFDAA10F41EE2DC7F7EA25074193686FEEDEA10F41746AF69DFA25074107F009F5DFA10F41699A662A0A2607412E5D07E9DDA10F416F712004192607417DDD8ADED8A10F412ACC0A9926260741E5292C07D1A10F4148FE8763322607411BA30FB0C6A10F41B95398EF3B2607414C66EF3EBAA10F4122414FDF422607417A84322EACA10F4193686FEE4626074112D238089DA10F4107F009F5472607418C9509628DA10F4107F009F56F1F07418C95096275D00E412E5D07E96D1F0741976599D565D00E417DDD8ADE681F0741918EDFFB56D00E41E5292C07611F0741D633F56649D00E411BA30FB0561F0741B801789C3DD00E414C66EF3E4A1F074147AC671034D00E417A84322E3C1F0741DEBEB0202DD00E4112D238082D1F07416D97901129D00E418C9509621D1F0741F90FF60A28D00E41976599D50D1F0741D2A2F8162AD00E41918EDFFBFE1E0741832275212FD00E41D633F566F11E07411BD6D3F836D00E41B801789CE51E0741E55CF04F41D00E4147AC6710DC1E0741B49910C14DD00E41DEBEB020D51E0741867BCDD15BD00E416D979011D11E0741EE2DC7F76AD00E41F90FF60AD01E0741746AF69D7AD00E41F90FF60AA8250741746AF69D92A10F41'

    def test_convex_hull(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.convex_hull) == '01020000000200000000000000201F07410000000078D00E4100000000F82507410000000090A10F41'

    def test_envelope(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.envelope) == '0103000000010000000500000000000000201F07410000000078D00E4100000000201F07410000000090A10F4100000000F82507410000000090A10F4100000000F82507410000000078D00E4100000000201F07410000000078D00E41'

    def test_start_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.start_point) == '010100000000000000201F07410000000078D00E41'

    def test_end_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert session.scalar(r.road_geom.end_point) == '010100000000000000F82507410000000090A10F41'

    # Test Geometry Relationships

    def test_equals(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        r3 = session.query(Road).filter(Road.road_name=='Paul St').one()
        assert session.scalar(r1.road_geom.equals(r2.road_geom))
        assert not session.scalar(r1.road_geom.equals(r3.road_geom))

    def test_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        assert session.scalar(r1.road_geom.distance(r2.road_geom)) == 9344.20339033778
        assert session.scalar(r1.road_geom.distance(r3.road_geom)) == 0.0

    def test_within_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        assert not session.scalar(r1.road_geom.within_distance(r2.road_geom, 100.0))
        assert session.scalar(r1.road_geom.within_distance(r3.road_geom, 100.0))

    def test_disjoint(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        assert session.scalar(r1.road_geom.disjoint(r2.road_geom))
        assert not session.scalar(r1.road_geom.disjoint(r3.road_geom))

    def test_intersects(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        assert not session.scalar(r1.road_geom.intersects(r2.road_geom))
        assert session.scalar(r1.road_geom.intersects(r3.road_geom))

    def test_touches(self):
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        l3 = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(l1.lake_geom.touches(l2.lake_geom))
        assert not session.scalar(l1.lake_geom.touches(l3.lake_geom))

    def test_crosses(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        assert not session.scalar(r1.road_geom.crosses(r2.road_geom))
        # TODO Find a suitable geometry for this test
        assert not session.scalar(r2.road_geom.crosses(r3.road_geom))

    def test_within(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==433.44).one()
        p2 = session.query(Spot).filter(Spot.spot_height==407.22).one()
        assert session.scalar(p1.spot_location.within(l.lake_geom))
        assert not session.scalar(p2.spot_location.within(l.lake_geom))

    def test_overlaps(self):
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        l3 = session.query(Lake).filter(Lake.lake_name=='Lake Deep').one()
        assert not session.scalar(l1.lake_geom.overlaps(l2.lake_geom))
        # TODO Find a suitable geometry for this test
        assert not session.scalar(l1.lake_geom.overlaps(l3.lake_geom))

    def test_contains(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==433.44).one()
        p2 = session.query(Spot).filter(Spot.spot_height==407.22).one()
        assert session.scalar(l.lake_geom.contains(p1.spot_location))
        assert not session.scalar(l.lake_geom.contains(p2.spot_location))

    def test_covers(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==433.44).one()
        p2 = session.query(Spot).filter(Spot.spot_height==407.22).one()
        assert session.scalar(l.lake_geom.covers(p1.spot_location))
        assert not session.scalar(l.lake_geom.covers(p2.spot_location))

    def test_covered_by(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==433.44).one()
        p2 = session.query(Spot).filter(Spot.spot_height==407.22).one()
        assert session.scalar(p1.spot_location.covered_by(l.lake_geom))
        assert not session.scalar(p2.spot_location.covered_by(l.lake_geom))

