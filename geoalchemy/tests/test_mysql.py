from unittest import TestCase
from binascii import b2a_hex
from sqlalchemy import (create_engine, MetaData, Column, Integer,
        Unicode, Numeric, func, Table)
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy import (GeometryColumn, Point, Polygon, LineString,
        GeometryDDL, WKTSpatialElement, DBSpatialElement, GeometryExtensionColumn,
        WKBSpatialElement)
from geoalchemy.functions import functions
from nose.tools import ok_, eq_, raises, assert_almost_equal
from sqlalchemy import and_
from sqlalchemy.exc import OperationalError

from geoalchemy.mysql import MySQLComparator, mysql_functions

engine = create_engine('mysql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
session = sessionmaker(bind=engine)()
Base = declarative_base(metadata=metadata)

class Road(Base):
    __tablename__ = 'roads'

    road_id = Column(Integer, primary_key=True)
    road_name = Column(Unicode(40))
    road_geom = GeometryColumn(LineString(2, srid=4326, spatial_index=False), comparator=MySQLComparator, nullable=False)

class Lake(Base):
    __tablename__ = 'lakes'

    lake_id = Column(Integer, primary_key=True)
    lake_name = Column(Unicode(40))
    lake_geom = GeometryColumn(Polygon(2, srid=4326), comparator=MySQLComparator)
    
spots_table = Table('spots', metadata,
                    Column('spot_id', Integer, primary_key=True),
                    Column('spot_height', Numeric(precision=10, scale=2)),
                    GeometryExtensionColumn('spot_location', Point(2, srid=4326)))

class Spot(object):
    def __init__(self, spot_id=None, spot_height=None, spot_location=None):
        self.spot_id = spot_id
        self.spot_height = spot_height
        self.spot_location = spot_location

        
mapper(Spot, spots_table, properties={
            'spot_location': GeometryColumn(spots_table.c.spot_location, 
                                            comparator=MySQLComparator)}) 

# enable the DDL extension, which allows CREATE/DROP operations
# to work correctly.  This is not needed if working with externally
# defined tables.    
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)
GeometryDDL(spots_table)

class TestGeometry(TestCase):

    def setUp(self):

        metadata.drop_all()
        metadata.create_all()

        # Add objects.  We can use strings...
        session.add_all([
            Road(road_name=u'Jeff Rd', road_geom='LINESTRING(-88.9139332929936 42.5082802993631,-88.8203027197452 42.5985669235669,-88.7383759681529 42.7239650127389,-88.6113059044586 42.9680732929936,-88.3655256496815 43.1402866687898)'),
            Road(road_name=u'Peter Rd', road_geom='LINESTRING(-88.9139332929936 42.5082802993631,-88.8203027197452 42.5985669235669,-88.7383759681529 42.7239650127389,-88.6113059044586 42.9680732929936,-88.3655256496815 43.1402866687898)'),
            Road(road_name=u'Geordie Rd', road_geom='LINESTRING(-89.2232485796178 42.6420382611465,-89.2449842484076 42.9179140573248,-89.2316084522293 43.106847178344,-89.0710987261147 43.243949044586,-89.092834566879 43.2957802993631,-89.092834566879 43.2957802993631,-89.0309715095541 43.3175159681529)'),
            Road(road_name=u'Paul St', road_geom='LINESTRING(-88.2652071783439 42.5584395350319,-88.1598727834395 42.6269904904459,-88.1013536751592 42.621974566879,-88.0244428471338 42.6437102356688,-88.0110670509554 42.6771497261147)'),
            Road(road_name=u'Graeme Ave', road_geom='LINESTRING(-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414)'),
            Road(road_name=u'Phil Tce', road_geom='LINESTRING(-88.9356689617834 42.9363057770701,-88.9824842484076 43.0366242484076,-88.9222931656051 43.1085191528662,-88.8487262866242 43.0449841210191)'),
            Lake(lake_name=u'My Lake', lake_geom='POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936))'),
            Lake(lake_name=u'Lake White', lake_geom='POLYGON((-88.1147292993631 42.7540605095542,-88.1548566878981 42.7824840764331,-88.1799363057325 42.7707802547771,-88.188296178344 42.7323248407643,-88.1832802547771 42.6955414012739,-88.1565286624204 42.6771496815287,-88.1448248407643 42.6336783439491,-88.131449044586 42.5718152866242,-88.1013535031847 42.565127388535,-88.1080414012739 42.5868630573248,-88.1164012738854 42.6119426751592,-88.1080414012739 42.6520700636943,-88.0980095541401 42.6838375796178,-88.0846337579618 42.7139331210191,-88.1013535031847 42.7423566878981,-88.1147292993631 42.7540605095542))'),
            Lake(lake_name=u'Lake Blue', lake_geom='POLYGON((-89.0694267515924 43.1335987261147,-89.1078821656051 43.1135350318471,-89.1329617834395 43.0884554140127,-89.1312898089172 43.0466560509554,-89.112898089172 43.0132165605096,-89.0694267515924 42.9898089171975,-89.0343152866242 42.953025477707,-89.0209394904459 42.9179140127389,-89.0042197452229 42.8961783439491,-88.9774681528663 42.8644108280255,-88.9440286624204 42.8292993630573,-88.9072452229299 42.8142515923567,-88.8687898089172 42.815923566879,-88.8687898089172 42.815923566879,-88.8102707006369 42.8343152866242,-88.7734872611465 42.8710987261147,-88.7517515923567 42.9145700636943,-88.7433917197452 42.9730891719745,-88.7517515923567 43.0299363057325,-88.7734872611465 43.0867834394905,-88.7885352038217 43.158678388535,-88.8738057324841 43.1620222929936,-88.947372611465 43.1937898089172,-89.0042197452229 43.2138535031847,-89.0410031847134 43.2389331210191,-89.0710987261147 43.243949044586,-89.0660828025478 43.2238853503185,-89.0543789808917 43.203821656051,-89.0376592356688 43.175398089172,-89.0292993630573 43.1519904458599,-89.0376592356688 43.1369426751592,-89.0393312101911 43.1386146496815,-89.0393312101911 43.1386146496815,-89.0510350318471 43.1335987261147,-89.0694267515924 43.1335987261147))'),
            Lake(lake_name=u'Lake Deep', lake_geom='POLYGON((-88.9122611464968 43.038296178344,-88.9222929936306 43.0399681528663,-88.9323248407643 43.0282643312102,-88.9206210191083 43.0182324840764,-88.9105891719745 43.0165605095542,-88.9005573248408 43.0232484076433,-88.9072452229299 43.0282643312102,-88.9122611464968 43.038296178344))'),
            Spot(spot_height=420.40, spot_location='POINT(-88.5945861592357 42.9480095987261)'),
            Spot(spot_height=102.34, spot_location='POINT(-88.9055734203822 43.0048567324841)'),
            Spot(spot_height=388.62, spot_location='POINT(-89.201512910828 43.1051752038217)'),
            Spot(spot_height=454.66, spot_location='POINT(-88.3304141847134 42.6269904904459)'),
        ])

        # or use an explicit WKTSpatialElement (similar to saying func.GeomFromText())
        self.r = Road(road_name=u'Dave Cres', road_geom=WKTSpatialElement('LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)', 4326))
        session.add(self.r)
        session.commit()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()

    # Test Geometry Functions

    def test_wkt(self):
        l = session.query(Lake).get(1)
        eq_(session.scalar(self.r.road_geom.wkt), 'LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)')
        eq_(session.scalar(l.lake_geom.wkt),'POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936))')
        ok_(not session.query(Spot).filter(Spot.spot_location.wkt == 'POINT(0,0)').first())
        ok_(session.query(Spot).get(1) is 
            session.query(Spot).filter(Spot.spot_location == 'POINT(-88.5945861592357 42.9480095987261)').first())
        envelope_geom = DBSpatialElement(session.scalar(self.r.road_geom.envelope))
        eq_(session.scalar(envelope_geom.wkt), 'POLYGON((-88.6748409363057 42.9363057770701,-88.4390925286624 42.9363057770701,-88.4390925286624 43.1035032292994,-88.6748409363057 43.1035032292994,-88.6748409363057 42.9363057770701))')
        eq_(session.scalar(WKTSpatialElement('POINT(-88.5769371859941 42.9915634871979)').wkt), 'POINT(-88.5769371859941 42.9915634871979)')

    def test_wkb(self):
        eq_(b2a_hex(session.scalar(self.r.road_geom.wkb)).upper(), '010200000005000000D7DB0998302B56C0876F04983F8D45404250F5E65E2956C068CE11FFC37F4540C8ED42D9E82656C0EFC45ED3E97B45407366F132062156C036C921DED877454078A18C171A1C56C053A5AF5B68804540')
        eq_(session.scalar(self.r.road_geom.wkb), self.r.road_geom.geom_wkb)
        envelope_geom = DBSpatialElement(session.scalar(self.r.road_geom.envelope))
        eq_(b2a_hex(session.scalar(envelope_geom.wkb)).upper(), '01030000000100000005000000D7DB0998302B56C036C921DED877454078A18C171A1C56C036C921DED877454078A18C171A1C56C0876F04983F8D4540D7DB0998302B56C0876F04983F8D4540D7DB0998302B56C036C921DED8774540')
        
    def test_coords(self):
        eq_(self.r.road_geom.coords(session), [[-88.674840936305699, 43.103503229299399], [-88.6464173694267, 42.998168834394903], [-88.607961955413998, 42.968073292993601], [-88.516003356687904, 42.936305777070103], [-88.4390925286624, 43.003184757961797]])
        s = session.query(Spot).filter(Spot.spot_height==102.34).one()
        eq_(s.spot_location.coords(session), [-88.905573420382197, 43.0048567324841])
        l = session.query(Lake).filter(Lake.lake_name==u"Lake Deep").one()
        eq_(l.lake_geom.coords(session), [[[-88.912261146496803, 43.038296178343998], [-88.922292993630606, 43.039968152866301], [-88.932324840764295, 43.028264331210202], [-88.920621019108296, 43.0182324840764], [-88.910589171974493, 43.016560509554203], [-88.900557324840804, 43.023248407643301], [-88.907245222929902, 43.028264331210202], [-88.912261146496803, 43.038296178343998]]])

    def test_persistent(self):
        eq_(b2a_hex(session.scalar(self.r.road_geom.wkb)).upper(), '010200000005000000D7DB0998302B56C0876F04983F8D45404250F5E65E2956C068CE11FFC37F4540C8ED42D9E82656C0EFC45ED3E97B45407366F132062156C036C921DED877454078A18C171A1C56C053A5AF5B68804540')

    def test_dimension(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        eq_(session.scalar(l.lake_geom.dimension), 2)
        eq_(session.scalar(r.road_geom.dimension), 1)
        eq_(session.scalar(s.spot_location.dimension), 0)
        ok_(session.query(Spot).filter(Spot.spot_location.dimension == 0).first() is not None)
        eq_(session.scalar(functions.dimension('POINT(-88.5945861592357 42.9480095987261)')), 0)

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
        eq_(session.scalar(functions.geometry_type(r.road_geom)), 'LINESTRING')
        ok_(session.query(Road).filter(Road.road_geom.geometry_type == 'LINESTRING').first())
        
    @raises(NotImplementedError)
    def test_is_valid(self):
        session.scalar(self.r.road_geom.is_valid)

    def test_is_empty(self):
        ok_(not session.scalar(self.r.road_geom.is_empty))
        ok_(session.query(Spot).filter(Spot.spot_location.is_empty == False).first() is not None)
        eq_(session.scalar(functions.is_empty('POINT(-88.5945861592357 42.9480095987261)')), False)

    @raises(NotImplementedError)
    def test_is_simple(self):
        session.scalar(self.r.road_geom.is_simple)

    @raises(NotImplementedError)
    def test_boundary(self):
        b2a_hex(session.scalar(self.r.road_geom.boundary))

    def test_envelope(self):
        eq_(b2a_hex(session.scalar(self.r.road_geom.envelope)), 'e610000001030000000100000005000000d7db0998302b56c036c921ded877454078a18c171a1c56c036c921ded877454078a18c171a1c56c0876f04983f8d4540d7db0998302b56c0876f04983f8d4540d7db0998302b56c036c921ded8774540')
        env =  WKBSpatialElement(session.scalar(func.AsBinary(self.r.road_geom.envelope)))
        eq_(env.geom_type(session), 'Polygon')
        eq_(session.scalar(functions.wkt(functions.envelope('POINT(-88.5945861592357 42.9480095987261)'))), 
            'POLYGON((-88.5945861592357 42.9480095987261,-88.5945861592357 42.9480095987261,-88.5945861592357 42.9480095987261,-88.5945861592357 42.9480095987261,-88.5945861592357 42.9480095987261))')
        
    def test_x(self):
        import math
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_( not session.scalar(l.lake_geom.x))
        ok_( not session.scalar(r.road_geom.x))
        print session.scalar(s.spot_location.x)
        eq_(math.ceil(session.scalar(s.spot_location.x)), -88.0)
        s = session.query(Spot).filter(and_(Spot.spot_location.x < 0, Spot.spot_location.y > 42)).all()
        ok_(s is not None)
        eq_(math.ceil(session.scalar(functions.x('POINT(-88.3655256496815 43.1402866687898)'))), -88.0)

    def test_y(self):
        import math
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.y))
        ok_(not session.scalar(r.road_geom.y))
        eq_(math.floor(session.scalar(s.spot_location.y)), 42.0)
        s = session.query(Spot).filter(and_(Spot.spot_location.y < 0, Spot.spot_location.y > 42)).all()
        ok_(s is not None)
        eq_(math.floor(session.scalar(functions.y('POINT(-88.3655256496815 43.1402866687898)'))), 43.0)

    def test_start_point(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.start_point))
        eq_(b2a_hex(session.scalar(r.road_geom.start_point)), 'e61000000101000000850811e27d3a56c0997b2f540f414540')
        ok_(not session.scalar(s.spot_location.start_point))

    def test_end_point(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.end_point))
        eq_(b2a_hex(session.scalar(r.road_geom.end_point)), 'e61000000101000000ccceb1c5641756c02c42dfe9f4914540')
        ok_(not session.scalar(s.spot_location.end_point))
    
    @raises(NotImplementedError)
    def test_transform(self):
        spot = session.query(Spot).get(1)
        session.scalar(spot.spot_location.transform(2249))

    def test_length(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        assert_almost_equal(session.scalar(r.road_geom.length), 0.85516941641479005)
        ok_(not session.scalar(l.lake_geom.length))
        ok_(not session.scalar(s.spot_location.length))

    def test_is_closed(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.is_closed))
        ok_(not session.scalar(r.road_geom.is_closed))
        ok_(not session.scalar(s.spot_location.is_closed))

    @raises(NotImplementedError)
    def test_is_ring(self):
        l = session.query(Lake).get(1)
        session.scalar(l.lake_geom.is_ring)

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
        eq_(b2a_hex(session.scalar(r.road_geom.point_n(5))), 'e61000000101000000ccceb1c5641756c02c42dfe9f4914540')
        ok_(not session.scalar(s.spot_location.point_n(1)))

    @raises(NotImplementedError)
    def test_centroid(self):
        l = session.query(Lake).get(1)
        session.scalar(l.lake_geom.centroid)

    def test_area(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        assert_almost_equal(session.scalar(l.lake_geom.area), 0.0056748625704923002)
        ok_(not session.scalar(r.road_geom.area))
        ok_(not session.scalar(s.spot_location.area))

    # Test Geometry Relations for Spatial Elements

    def test_equals(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Peter Rd').one()
        r3 = session.query(Road).filter(Road.road_name==u'Paul St').one()
        equal_roads = session.query(Road).filter(Road.road_geom.equals(r1.road_geom)).all()
        ok_(r1 in equal_roads)
        ok_(r2 in equal_roads)
        ok_(r3 not in equal_roads)

    @raises(NotImplementedError)
    def test_distance(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        value1 = session.scalar(r1.road_geom.distance(r2.road_geom))
        value2 = session.scalar(r2.road_geom.distance(r1.road_geom))
        eq_(value1, value2)

    def test_disjoint(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        disjoint_roads = session.query(Road).filter(Road.road_geom.disjoint(r1.road_geom)).all()
        ok_(r2 not in disjoint_roads)
        ok_(r3 in disjoint_roads)

    def test_intersects(self):
        r1 = session.query(Road).filter(Road.road_name==u'Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name==u'Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        intersecting_roads = session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()
        ok_(r2 in intersecting_roads)
        ok_(r3 not in intersecting_roads)

    @raises(NotImplementedError)
    def test_touches(self):
        r = session.query(Road).filter(Road.road_name==u'Geordie Rd').one()
        session.query(Lake).filter(Lake.lake_geom.touches(r.road_geom)).all()

    @raises(NotImplementedError)
    def test_crosses(self):
        l = session.query(Lake).filter(Lake.lake_name==u'Lake White').one()
        session.query(Road).filter(Road.road_geom.crosses(l.lake_geom)).all()
        
    def test_within(self):
        l = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
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
        l1 = session.query(Lake).filter(Lake.lake_name==u'Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
        l3 = session.query(Lake).filter(Lake.lake_name==u'My Lake').one()
        overlapping_lakes = session.query(Lake).filter(Lake.lake_geom.overlaps(l3.lake_geom)).all()
        ok_(not session.scalar(l1.lake_geom.overlaps(l3.lake_geom)))
        ok_(session.scalar(l2.lake_geom.overlaps(l3.lake_geom)))
        ok_(l1 not in overlapping_lakes)
        ok_(l2 in overlapping_lakes)

    def test_contains(self):
        l = session.query(Lake).filter(Lake.lake_name==u'Lake Blue').one()
        l1 = session.query(Lake).filter(Lake.lake_name==u'Lake White').one()
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
        eq_(session.scalar(mysql_functions.mbr_equal('POINT(-88.5945861592357 42.9480095987261)', 'POINT(-88.5945861592357 42.9480095987261)')), True)


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
        ok_(l1 not in touching_lakes)
        ok_(l2 not in touching_lakes)
        eq_(session.scalar(l1.lake_geom.mbr_touches(r.road_geom)), 0L)
        eq_(session.scalar(l2.lake_geom.mbr_touches(r.road_geom)), 0L)

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

    @raises(OperationalError)
    def test_constraint_nullable(self):
        road_null = Road(road_name=u'Jeff Rd', road_geom=None)
        session.add(road_null)
        session.commit();

