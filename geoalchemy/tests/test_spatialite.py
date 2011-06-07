from unittest import TestCase
from binascii import b2a_hex
from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        Numeric, func, Table)
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import Select, select

from pysqlite2 import dbapi2 as sqlite
from geoalchemy import (GeometryColumn, Point, Polygon,
		LineString, GeometryDDL, WKTSpatialElement, WKBSpatialElement, 
        DBSpatialElement, GeometryExtensionColumn)
from geoalchemy.functions import functions
from nose.tools import ok_, eq_, assert_almost_equal, raises

from geoalchemy.spatialite import SQLiteComparator, sqlite_functions
from geoalchemy.base import PersistentSpatialElement
from sqlalchemy.orm.query import aliased

engine = create_engine('sqlite://', module=sqlite, echo=True)
connection = engine.raw_connection().connection
connection.enable_load_extension(True)
metadata = MetaData(engine)
session = sessionmaker(bind=engine)()
session.execute("select load_extension('/usr/lib/libspatialite.so')")
session.execute("SELECT InitSpatialMetaData()")
connection.enable_load_extension(False)
session.commit()
Base = declarative_base(metadata=metadata)

class Road(Base):
    __tablename__ = 'roads'

    road_id = Column(Integer, primary_key=True)
    road_name = Column(String)
    road_geom = GeometryColumn(LineString(2, srid=4326), comparator = SQLiteComparator, nullable=False)

class Lake(Base):
    __tablename__ = 'lakes'

    lake_id = Column(Integer, primary_key=True)
    lake_name = Column(String)
    lake_geom = GeometryColumn(Polygon(2, srid=4326, spatial_index=False), comparator = SQLiteComparator)

spots_table = Table('spots', metadata,
                    Column('spot_id', Integer, primary_key=True),
                    Column('spot_height', Numeric()),
                    GeometryExtensionColumn('spot_location', Point(2, srid=4326)))

class Spot(object):
    def __init__(self, spot_id=None, spot_height=None, spot_location=None):
        self.spot_id = spot_id
        self.spot_height = spot_height
        self.spot_location = spot_location

        
mapper(Spot, spots_table, properties={
            'spot_location': GeometryColumn(spots_table.c.spot_location, 
                                            comparator=SQLiteComparator)}) 

# enable the DDL extension, which allows CREATE/DROP operations
# to work correctly.  This is not needed if working with externally
# defined tables.    
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)
GeometryDDL(spots_table)

class TestGeometry(TestCase):
    """Tests for Spatialite
    
    Note that WKT is used for comparisons, because Spatialite may return
    differing WKB values on different systems.
    """

    def setUp(self):
        metadata.drop_all()
        session.execute("DROP VIEW geom_cols_ref_sys")
        session.execute("DROP TABLE geometry_columns")
        session.execute("DROP TABLE spatial_ref_sys")
        session.commit()
        session.execute("SELECT InitSpatialMetaData()")
        session.execute("INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) VALUES (4326, 'epsg', 4326, 'WGS 84', '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')")
        session.execute("INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) VALUES (2249, 'epsg', 2249, 'NAD83 / Massachusetts Mainland (ftUS)', '+proj=lcc +lat_1=42.68333333333333 +lat_2=41.71666666666667 +lat_0=41 +lon_0=-71.5 +x_0=200000.0001016002 +y_0=750000 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs')")
        metadata.create_all()

        # Add objects.  We can use strings...
        session.add_all([
            Road(road_name='Jeff Rd', road_geom='LINESTRING(-88.9139332929936 42.5082802993631,-88.8203027197452 42.5985669235669,-88.7383759681529 42.7239650127389,-88.6113059044586 42.9680732929936,-88.3655256496815 43.1402866687898)'),
            Road(road_name='Peter Rd', road_geom='LINESTRING(-88.9139332929936 42.5082802993631,-88.8203027197452 42.5985669235669,-88.7383759681529 42.7239650127389,-88.6113059044586 42.9680732929936,-88.3655256496815 43.1402866687898)'),
            Road(road_name='Geordie Rd', road_geom='LINESTRING(-89.2232485796178 42.6420382611465,-89.2449842484076 42.9179140573248,-89.2316084522293 43.106847178344,-89.0710987261147 43.243949044586,-89.092834566879 43.2957802993631,-89.092834566879 43.2957802993631,-89.0309715095541 43.3175159681529)'),
            Road(road_name='Paul St', road_geom='LINESTRING(-88.2652071783439 42.5584395350319,-88.1598727834395 42.6269904904459,-88.1013536751592 42.621974566879,-88.0244428471338 42.6437102356688,-88.0110670509554 42.6771497261147)'),
            Road(road_name='Graeme Ave', road_geom='LINESTRING(-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414)'),
            Road(road_name='Phil Tce', road_geom='LINESTRING(-88.9356689617834 42.9363057770701,-88.9824842484076 43.0366242484076,-88.9222931656051 43.1085191528662,-88.8487262866242 43.0449841210191)'),
            Lake(lake_name='My Lake', lake_geom='POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936))'),
            Lake(lake_name='Lake White', lake_geom='POLYGON((-88.1147292993631 42.7540605095542,-88.1548566878981 42.7824840764331,-88.1799363057325 42.7707802547771,-88.188296178344 42.7323248407643,-88.1832802547771 42.6955414012739,-88.1565286624204 42.6771496815287,-88.1448248407643 42.6336783439491,-88.131449044586 42.5718152866242,-88.1013535031847 42.565127388535,-88.1080414012739 42.5868630573248,-88.1164012738854 42.6119426751592,-88.1080414012739 42.6520700636943,-88.0980095541401 42.6838375796178,-88.0846337579618 42.7139331210191,-88.1013535031847 42.7423566878981,-88.1147292993631 42.7540605095542))'),
            Lake(lake_name='Lake Blue', lake_geom='POLYGON((-89.0694267515924 43.1335987261147,-89.1078821656051 43.1135350318471,-89.1329617834395 43.0884554140127,-89.1312898089172 43.0466560509554,-89.112898089172 43.0132165605096,-89.0694267515924 42.9898089171975,-89.0343152866242 42.953025477707,-89.0209394904459 42.9179140127389,-89.0042197452229 42.8961783439491,-88.9774681528663 42.8644108280255,-88.9440286624204 42.8292993630573,-88.9072452229299 42.8142515923567,-88.8687898089172 42.815923566879,-88.8687898089172 42.815923566879,-88.8102707006369 42.8343152866242,-88.7734872611465 42.8710987261147,-88.7517515923567 42.9145700636943,-88.7433917197452 42.9730891719745,-88.7517515923567 43.0299363057325,-88.7734872611465 43.0867834394905,-88.7885352038217 43.158678388535,-88.8738057324841 43.1620222929936,-88.947372611465 43.1937898089172,-89.0042197452229 43.2138535031847,-89.0410031847134 43.2389331210191,-89.0710987261147 43.243949044586,-89.0660828025478 43.2238853503185,-89.0543789808917 43.203821656051,-89.0376592356688 43.175398089172,-89.0292993630573 43.1519904458599,-89.0376592356688 43.1369426751592,-89.0393312101911 43.1386146496815,-89.0393312101911 43.1386146496815,-89.0510350318471 43.1335987261147,-89.0694267515924 43.1335987261147))'),
            Lake(lake_name='Lake Deep', lake_geom='POLYGON((-88.9122611464968 43.038296178344,-88.9222929936306 43.0399681528663,-88.9323248407643 43.0282643312102,-88.9206210191083 43.0182324840764,-88.9105891719745 43.0165605095542,-88.9005573248408 43.0232484076433,-88.9072452229299 43.0282643312102,-88.9122611464968 43.038296178344))'),
            Spot(spot_height=420.40, spot_location='POINT(-88.5945861592357 42.9480095987261)'),
            Spot(spot_height=102.34, spot_location='POINT(-88.9055734203822 43.0048567324841)'),
            Spot(spot_height=388.62, spot_location='POINT(-89.201512910828 43.1051752038217)'),
            Spot(spot_height=454.66, spot_location='POINT(-88.3304141847134 42.6269904904459)'),
        ])

        # or use an explicit WKTSpatialElement (similar to saying func.GeomFromText())
        self.r = Road(road_name='Dave Cres', road_geom=WKTSpatialElement('LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)', 4326))
        session.add(self.r)
        session.commit()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()

    # Test Geometry Functions

    def test_wkt(self):
        eq_(session.scalar(self.r.road_geom.wkt), 'LINESTRING(-88.674841 43.103503, -88.646417 42.998169, -88.607962 42.968073, -88.516003 42.936306, -88.439093 43.003185)')
        centroid_geom = DBSpatialElement(session.scalar(self.r.road_geom.centroid))
        eq_(session.scalar(centroid_geom.wkt), u'POINT(-88.576937 42.991563)')
        ok_(not session.query(Spot).filter(Spot.spot_location.wkt == 'POINT(0,0)').first())
        ok_(session.query(Spot).get(1) is 
            session.query(Spot).filter(Spot.spot_location == 'POINT(-88.5945861592357 42.9480095987261)').first())
        eq_(session.scalar(WKTSpatialElement('POINT(-88.5769371859941 42.9915634871979)').wkt), u'POINT(-88.576937 42.991563)')

    def test_wkb(self):
        eq_(session.scalar(functions.wkt(func.GeomFromWKB(self.r.road_geom.wkb, 4326))), 
            u'LINESTRING(-88.674841 43.103503, -88.646417 42.998169, -88.607962 42.968073, -88.516003 42.936306, -88.439093 43.003185)')
        eq_(session.scalar(self.r.road_geom.wkb), self.r.road_geom.geom_wkb)
        centroid_geom = DBSpatialElement(session.scalar(self.r.road_geom.centroid))
        eq_(session.scalar(functions.wkt(func.GeomFromWKB(centroid_geom.wkb, 4326))), 
            u'POINT(-88.576937 42.991563)')

    def test_persistent(self):
        eq_(session.scalar(functions.wkt(func.GeomFromWKB(self.r.road_geom.wkb, 4326))), 
            u'LINESTRING(-88.674841 43.103503, -88.646417 42.998169, -88.607962 42.968073, -88.516003 42.936306, -88.439093 43.003185)')

        geom = WKTSpatialElement('POINT(30250865.9714116 -610981.481754275)', 2249)
        spot = Spot(spot_height=102.34, spot_location=geom)
        session.add(spot)
        session.commit();
        assert_almost_equal(session.scalar(spot.spot_location.x), 0)
        assert_almost_equal(session.scalar(spot.spot_location.y), 0)
        
        spot.spot_location = PersistentSpatialElement(None)
        ok_(isinstance(spot.spot_location, PersistentSpatialElement))

    def test_svg(self):
        eq_(session.scalar(self.r.road_geom.svg), 'M -88.674841 -43.103503 L -88.646417 -42.998169 -88.607962 -42.968073 -88.516003 -42.936306 -88.439093 -43.003185 ')

    def test_fgf(self):
        eq_(b2a_hex(session.scalar(self.r.road_geom.fgf(1))), '020000000100000005000000d7db0998302b56c0876f04983f8d454000000000000000004250f5e65e2956c068ce11ffc37f45400000000000000000c8ed42d9e82656c0efc45ed3e97b454000000000000000007366f132062156c036c921ded8774540000000000000000078a18c171a1c56c053a5af5b688045400000000000000000')

    def test_dimension(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        eq_(session.scalar(l.lake_geom.dimension), 2)
        eq_(session.scalar(r.road_geom.dimension), 1)
        eq_(session.scalar(s.spot_location.dimension), 0)

    def test_srid(self):
        eq_(session.scalar(self.r.road_geom.srid), 4326)
        ok_(session.query(Spot).filter(Spot.spot_location.srid == 4326).first() is not None)
        eq_(session.scalar(functions.srid('POINT(-88.5945861592357 42.9480095987261)')), 4326)

    def test_geometry_type(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        eq_(session.scalar(l.lake_geom.geometry_type), 'POLYGON')
        eq_(session.scalar(r.road_geom.geometry_type), 'LINESTRING')
        eq_(session.scalar(s.spot_location.geometry_type), 'POINT')

    def test_is_valid(self):
        ok_(not session.scalar(self.r.road_geom.is_valid))

    def test_is_empty(self):
        ok_(not session.scalar(self.r.road_geom.is_empty))

    def test_is_simple(self):
        ok_(session.scalar(self.r.road_geom.is_simple))

    def test_is_valid(self):
        assert session.scalar(self.r.road_geom.is_valid)

    def test_boundary(self):
        eq_(session.scalar(functions.wkt(self.r.road_geom.boundary)), u'MULTIPOINT(-88.674841 43.103503, -88.439093 43.003185)')

    def test_envelope(self):
        eq_(session.scalar(functions.wkt(self.r.road_geom.envelope)), 
            u'POLYGON((-88.674841 42.936306, -88.439093 42.936306, -88.439093 43.103503, -88.674841 43.103503, -88.674841 42.936306))')
        env =  WKBSpatialElement(session.scalar(func.AsBinary(self.r.road_geom.envelope)))
        eq_(env.geom_type(session), 'Polygon')
        
    def test_x(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_( not session.scalar(l.lake_geom.x))
        ok_( not session.scalar(r.road_geom.x))
        eq_(session.scalar(s.spot_location.x), -88.594586159235703)

    def test_y(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.y))
        ok_(not session.scalar(r.road_geom.y))
        eq_(session.scalar(s.spot_location.y), 42.948009598726102)

    def test_start_point(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.start_point))
        assert_almost_equal(session.scalar(func.X(r.road_geom.start_point)), -88.9139332929936)
        assert_almost_equal(session.scalar(func.Y(r.road_geom.start_point)), 42.5082802993631)
        #eq_(b2a_hex(session.scalar(r.road_geom.start_point)), '0001ffffffff850811e27d3a56c0997b2f540f414540850811e27d3a56c0997b2f540f4145407c01000000850811e27d3a56c0997b2f540f414540fe')
        ok_(not session.scalar(s.spot_location.start_point))

    def test_end_point(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.end_point)) 
        assert_almost_equal(session.scalar(func.X(r.road_geom.end_point)), -88.3655256496815)
        assert_almost_equal(session.scalar(func.Y(r.road_geom.end_point)), 43.1402866687898)
        #eq_(b2a_hex(session.scalar(r.road_geom.end_point)), '0001ffffffffccceb1c5641756c02c42dfe9f4914540ccceb1c5641756c02c42dfe9f49145407c01000000ccceb1c5641756c02c42dfe9f4914540fe')
        ok_(not session.scalar(s.spot_location.end_point))
        
    def test_transform(self):
        spot = session.query(Spot).get(1)
        eq_(session.scalar(functions.wkt(spot.spot_location.transform(2249))), 
            u'POINT(-3890517.610956 3627658.674651)')
        ok_(session.query(Spot).filter(sqlite_functions.mbr_contains(
                                                functions.buffer(Spot.spot_location.transform(2249), 10),
                                                WKTSpatialElement('POINT(-3890517.610956 3627658.674651)', 2249))).first() is not None)
        eq_(session.scalar(functions.wkt(functions.transform(WKTSpatialElement('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))', 2249), 4326))), 
            u'POLYGON((-71.177685 42.39029, -71.177684 42.390383, -71.177584 42.390383, -71.177583 42.390289, -71.177685 42.39029))')
        
    def test_length(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        assert_almost_equal(session.scalar(l.lake_geom.length), 0.30157858985653774)
        assert_almost_equal(session.scalar(r.road_geom.length), 0.8551694164147895)
        ok_(not session.scalar(s.spot_location.length))

    def test_is_closed(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.is_closed))
        ok_(not session.scalar(r.road_geom.is_closed))
        ok_(not session.scalar(s.spot_location.is_closed))

    def test_is_ring(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(session.scalar(l.lake_geom.is_ring))
        ok_(not session.scalar(r.road_geom.is_ring))
        ok_(session.scalar(s.spot_location.is_ring))

    def test_num_points(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.num_points))
        eq_(session.scalar(r.road_geom.num_points), 5)
        ok_(not session.scalar(s.spot_location.num_points))

    def test_point_n(self):
        l = session.query(Lake).get(1)  
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.point_n(1)))
        assert_almost_equal(session.scalar(r.road_geom.point_n(5).x), -88.3655256496815)
        assert_almost_equal(session.scalar(r.road_geom.point_n(5).y), 43.1402866687898)
        #eq_(b2a_hex(session.scalar(r.road_geom.point_n(5))), '0001ffffffffccceb1c5641756c02c42dfe9f4914540ccceb1c5641756c02c42dfe9f49145407c01000000ccceb1c5641756c02c42dfe9f4914540fe')
        ok_(not session.scalar(s.spot_location.point_n(1)))

    def test_centroid(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        assert_almost_equal(session.scalar(func.X(l.lake_geom.centroid)), -88.7578400578)
        assert_almost_equal(session.scalar(func.Y(l.lake_geom.centroid)), 43.1937975407)
        #eq_(b2a_hex(session.scalar(l.lake_geom.centroid)), '0001ffffffff81ec9573803056c04bc4995bce98454081ec9573803056c04bc4995bce9845407c0100000081ec9573803056c04bc4995bce984540fe')
        assert_almost_equal(session.scalar(func.X(r.road_geom.centroid)), -88.6569666079)
        assert_almost_equal(session.scalar(func.Y(r.road_geom.centroid)), 42.8422057576)
        #eq_(b2a_hex(session.scalar(r.road_geom.centroid)), '0001ffffffff1cecabbd0b2a56c022b0f465cd6b45401cecabbd0b2a56c022b0f465cd6b45407c010000001cecabbd0b2a56c022b0f465cd6b4540fe')
        assert_almost_equal(session.scalar(func.X(s.spot_location.centroid)), -88.5945861592)
        assert_almost_equal(session.scalar(func.Y(s.spot_location.centroid)), 42.9480095987)
        #ok_(b2a_hex(session.scalar(s.spot_location.centroid)), '0001ffffffff95241bb30d2656c04e69e7605879454095241bb30d2656c04e69e760587945407c0100000095241bb30d2656c04e69e76058794540fe')

    def test_area(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        assert_almost_equal(session.scalar(l.lake_geom.area), 0.0056748625704927669)
        assert_almost_equal(session.scalar(r.road_geom.area), 0.0)
        assert_almost_equal(session.scalar(s.spot_location.area), 0.0)

    def test_equals(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Peter Rd').one()
        r3 = session.query(Road).filter(Road.road_name=='Paul St').one()
        equal_roads = session.query(Road).filter(Road.road_geom.equals(r1.road_geom)).all()
        ok_(r1 in equal_roads)
        ok_(r2 in equal_roads)
        ok_(r3 not in equal_roads)

    def test_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        r3 = session.query(Road).filter(Road.road_name=='Peter Rd').one()
        assert_almost_equal(session.scalar(r1.road_geom.distance(r2.road_geom)), 0.3369972386828412)
        assert_almost_equal(session.scalar(r1.road_geom.distance(r3.road_geom)), 0.0)

    def test_disjoint(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        disjoint_roads = session.query(Road).filter(Road.road_geom.disjoint(r1.road_geom)).all()
        ok_(r2 not in disjoint_roads)
        ok_(r3 in disjoint_roads)

    def test_intersects(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        intersecting_roads = session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()
        ok_(r2 in intersecting_roads)
        ok_(r3 not in intersecting_roads)

    def test_touches(self):
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        r = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        touching_lakes = session.query(Lake).filter(Lake.lake_geom.touches(r.road_geom)).all()
        ok_(not session.scalar(l1.lake_geom.touches(r.road_geom)))
        ok_(session.scalar(l2.lake_geom.touches(r.road_geom)))
        ok_(l1 not in touching_lakes)
        ok_(l2 in touching_lakes)

    def test_crosses(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Paul St').one()
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        crossing_roads = session.query(Road).filter(Road.road_geom.crosses(l.lake_geom)).all()
        ok_(not session.scalar(r1.road_geom.crosses(l.lake_geom)))
        ok_(session.scalar(r2.road_geom.crosses(l.lake_geom)))
        ok_(r1 not in crossing_roads)
        ok_(r2 in crossing_roads)

    def test_within(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        spots_within = session.query(Spot).filter(Spot.spot_location.within(l.lake_geom)).all()
        ok_(session.scalar(p1.spot_location.within(l.lake_geom)))
        ok_(not session.scalar(p2.spot_location.within(l.lake_geom)))
        ok_(p1 in spots_within)
        ok_(p2 not in spots_within)
        envelope_geom = DBSpatialElement(session.scalar(l.lake_geom.envelope))
        spots_within = session.query(Spot).filter(l.lake_geom.within(envelope_geom)).all()
        ok_(p1 in spots_within)
        ok_(p2 in spots_within)

    def test_overlaps(self):
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        l3 = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        overlapping_lakes = session.query(Lake).filter(Lake.lake_geom.overlaps(l3.lake_geom)).all()
        ok_(not session.scalar(l1.lake_geom.overlaps(l3.lake_geom)))
        ok_(session.scalar(l2.lake_geom.overlaps(l3.lake_geom)))
        ok_(l1 not in overlapping_lakes)
        ok_(l2 in overlapping_lakes)

    def test_contains(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        containing_lakes = session.query(Lake).filter(Lake.lake_geom.gcontains(p1.spot_location)).all()
        ok_(session.scalar(l.lake_geom.gcontains(p1.spot_location)))
        ok_(not session.scalar(l.lake_geom.gcontains(p2.spot_location)))
        ok_(l in containing_lakes)
        ok_(l1 not in containing_lakes)
        ok_(session.scalar(l.lake_geom.gcontains(WKTSpatialElement('POINT(-88.9055734203822 43.0048567324841)'))))
        containing_lakes = session.query(Lake).filter(Lake.lake_geom.gcontains('POINT(-88.9055734203822 43.0048567324841)')).all()
        ok_(l in containing_lakes)
        ok_(l1 not in containing_lakes)
        spots_within = session.query(Spot).filter(l.lake_geom.gcontains(Spot.spot_location)).all()
        ok_(session.scalar(l.lake_geom.gcontains(p1.spot_location)))
        ok_(not session.scalar(l.lake_geom.gcontains(p2.spot_location)))
        ok_(p1 in spots_within)
        ok_(p2 not in spots_within)

    # Test Geometry Relations for Minimum Bounding Rectangles (MBRs)

    def test_mbr_equal(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Peter Rd').one()
        r3 = session.query(Road).filter(Road.road_name==u'Paul St').one()
        equal_roads = session.query(Road).filter(Road.road_geom.mbr_equal(r1.road_geom)).all()
        ok_(r1 in equal_roads)
        ok_(r2 in equal_roads)
        ok_(r3 not in equal_roads)
        ok_(session.scalar(r2.road_geom.mbr_equal(r1.road_geom)))
        eq_(session.scalar(sqlite_functions.mbr_equal('POINT(-88.5945861592357 42.9480095987261)', 'POINT(-88.5945861592357 42.9480095987261)')), True)

    def test_mbr_disjoint(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        disjoint_roads = session.query(Road).filter(Road.road_geom.mbr_disjoint(r1.road_geom)).all()
        ok_(r2 not in disjoint_roads)
        ok_(r3 in disjoint_roads)

    def test_mbr_intersects(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        intersecting_roads = session.query(Road).filter(Road.road_geom.mbr_intersects(r1.road_geom)).all()
        ok_(r2 in intersecting_roads)
        ok_(r3 not in intersecting_roads)

    def test_mbr_touches(self):
        l1 = session.query(Lake).filter(Lake.lake_name==u'Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
        r = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        touching_lakes = session.query(Lake).filter(Lake.lake_geom.mbr_touches(r.road_geom)).all()
        ok_(not session.scalar(l1.lake_geom.mbr_touches(r.road_geom)))
        ok_(not session.scalar(l2.lake_geom.mbr_touches(r.road_geom)))
        ok_(l1 not in touching_lakes)
        ok_(l2 not in touching_lakes)

    def test_mbr_within(self):
        l = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        spots_within = session.query(Spot).filter(Spot.spot_location.mbr_within(l.lake_geom)).all()
        ok_(session.scalar(p1.spot_location.mbr_within(l.lake_geom)))
        ok_(not session.scalar(p2.spot_location.mbr_within(l.lake_geom)))
        ok_(p1 in spots_within)
        ok_(p2 not in spots_within)

    def test_mbr_overlaps(self):
        l1 = session.query(Lake).filter(Lake.lake_name==u'Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
        l3 = session.query(Lake).filter(Lake.lake_name==u'My Lake').one()
        overlapping_lakes = session.query(Lake).filter(Lake.lake_geom.mbr_overlaps(l3.lake_geom)).all()
        ok_(not session.scalar(l1.lake_geom.mbr_overlaps(l3.lake_geom)))
        ok_(session.scalar(l2.lake_geom.mbr_overlaps(l3.lake_geom)))
        ok_(l1 not in overlapping_lakes)
        ok_(l2 in overlapping_lakes)

    def test_mbr_contains(self):
        l = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
        l1 = session.query(Lake).filter(Lake.lake_name==u'Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        containing_lakes = session.query(Lake).filter(Lake.lake_geom.mbr_contains(p1.spot_location)).all()
        ok_(session.scalar(l.lake_geom.mbr_contains(p1.spot_location)))
        ok_(not session.scalar(l.lake_geom.mbr_contains(p2.spot_location)))
        ok_(l in containing_lakes)
        ok_(l1 not in containing_lakes)

    def test_within_distance(self):
        ok_(session.scalar(functions._within_distance('POINT(-88.9139332929936 42.5082802993631)', 
                                                      'POINT(-88.9139332929936 35.5082802993631)', 10)))
        ok_(session.scalar(functions._within_distance('Point(0 0)', 'Point(0 0)', 0)))
        ok_(session.scalar(functions._within_distance('Point(0 0)', 
                                                      'Polygon((-5 -5, 5 -5, 5 5, -5 5, -5 -5))', 0)))
        ok_(session.scalar(functions._within_distance('Point(5 5)', 
                                                      'Polygon((-5 -5, 5 -5, 5 5, -5 5, -5 -5))', 0)))
        ok_(session.scalar(functions._within_distance('Point(6 5)', 
                                                      'Polygon((-5 -5, 5 -5, 5 5, -5 5, -5 -5))', 1)))
        ok_(session.scalar(functions._within_distance('Polygon((0 0, 1 0, 1 8, 0 8, 0 0))', 
                                                      'Polygon((-5 -5, 5 -5, 5 5, -5 5, -5 -5))', 0)))  

    @raises(IntegrityError)
    def test_constraint_nullable(self):
        spot_null = Spot(spot_height=420.40, spot_location=None)
        session.add(spot_null)
        session.commit();
        ok_(True)
        road_null = Road(road_name='Jeff Rd', road_geom=None)
        session.add(road_null)
        session.commit();
        
    def test_query_column_name(self):
        # test for bug: http://groups.google.com/group/geoalchemy/browse_thread/thread/6b731dd1673784f9
        from sqlalchemy.orm.query import Query
        query = Query(Road.road_geom).filter(Road.road_geom == '..').__str__()
        ok_('AsBinary(roads.road_geom)' in query, 'table name is part of the column expression (select clause)')
        ok_('WHERE Equals(roads.road_geom' in query, 'table name is part of the column expression (where clause)')
        
        query_wkb = Select([Road.road_geom]).where(Road.road_geom == 'POINT(0 0)').__str__()
        ok_('SELECT AsBinary(roads.road_geom)' in query_wkb, 'AsBinary is added')
        ok_('WHERE Equals(roads.road_geom' in query_wkb, 'AsBinary is not added in where clause')
        
        # test for RAW attribute
        query_wkb = Select([Road.road_geom.RAW]).__str__()
        ok_('SELECT roads.road_geom' in query_wkb, 'AsBinary is not added')
        
        ok_(session.query(Road.road_geom.RAW).first())
        
        query_srid = Query(func.SRID(Road.road_geom.RAW))
        ok_('SRID(roads.road_geom)' in query_srid.__str__(), 'AsBinary is not added')
        ok_(session.scalar(query_srid))
        
        eq_(session.scalar(Select([func.SRID(Spot.spot_location)]).where(Spot.spot_id == 1)), 
                None,
                'AsBinary is added and the SRID is not returned')
        eq_(str(session.scalar(Select([func.SRID(Spot.spot_location.RAW)]).where(Spot.spot_id == 1))), 
                '4326',
                'AsBinary is not added and the SRID is returned')
        
        spot_alias = aliased(Spot)
        query_wkt = Select([func.wkt(spot_alias.spot_location.RAW)]).__str__()
        ok_('SELECT wkt(spots_1.spot_location' in query_wkt, 'Table alias is used in select clause')
        ok_('FROM spots AS spots_1' in query_wkt, 'Table alias is used in from clause')
        
        
