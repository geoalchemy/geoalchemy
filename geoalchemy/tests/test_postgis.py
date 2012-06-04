from unittest import TestCase
from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        Numeric, func, Table)
from sqlalchemy.orm import sessionmaker, mapper, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, ForeignKey
from sqlalchemy.exc import IntegrityError

from geoalchemy import (Geometry, GeometryCollection, GeometryColumn,
        GeometryDDL, WKTSpatialElement, DBSpatialElement, GeometryExtensionColumn)
from geoalchemy.functions import functions
from geoalchemy.postgis import PGComparator, pg_functions

from nose.tools import eq_, ok_, raises, assert_almost_equal



engine = create_engine('postgresql://gis:gis@localhost/gis')
metadata = MetaData(engine)
session = sessionmaker(bind=engine)()
Base = declarative_base(metadata=metadata)

class Road(Base):
    __tablename__ = 'ROADS'

    road_id = Column(Integer, primary_key=True)
    road_name = Column(String)
    road_geom = GeometryColumn(Geometry(2), comparator=PGComparator, nullable=False)

    lakes = relationship('Lake', backref="road")

class Lake(Base):
    __tablename__ = 'lakes'

    lake_id = Column(Integer, primary_key=True)
    lake_name = Column(String)
    lake_geom = GeometryColumn(Geometry(2, wkt_internal=True), comparator=PGComparator)
    road_id = Column(Integer, ForeignKey('ROADS.road_id'))

spots_table = Table('spots', metadata,
                    Column('spot_id', Integer, primary_key=True),
                    Column('spot_height', Numeric),
                    GeometryExtensionColumn('spot_location', Geometry(2)))

class Spot(object):
    def __init__(self, spot_id=None, spot_height=None, spot_location=None):
        self.spot_id = spot_id
        self.spot_height = spot_height
        self.spot_location = spot_location

        
mapper(Spot, spots_table, properties={
            'spot_location': GeometryColumn(spots_table.c.spot_location, 
                                            comparator=PGComparator)}) 
                         
class Shape(Base):
    __tablename__ = 'shapes'

    shape_id = Column(Integer, primary_key=True)
    shape_name = Column(String)
    shape_geom = GeometryColumn(GeometryCollection(2))

# enable the DDL extension, which allows CREATE/DROP operations
# to work correctly.  This is not needed if working with externally
# defined tables.    
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)
GeometryDDL(spots_table)
GeometryDDL(Shape.__table__)

class TestGeometry(TestCase):
    """Tests for PostGIS
    
    Note that WKT is used for comparisons, because PostGIS returns
    differing WKB values on different systems.
    """

    def setUp(self):

        metadata.drop_all()
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
            Shape(shape_name='Bus Stop', shape_geom='GEOMETRYCOLLECTION(POINT(-88.3304141847134 42.6269904904459))'),
            Shape(shape_name='Jogging Track', shape_geom='GEOMETRYCOLLECTION(LINESTRING(-88.2652071783439 42.5584395350319,-88.1598727834395 42.6269904904459,-88.1013536751592 42.621974566879,-88.0244428471338 42.6437102356688,-88.0110670509554 42.6771497261147))'),
            Shape(shape_name='Play Ground', shape_geom='GEOMETRYCOLLECTION(POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936)))'),
        ])
 
        # or use an explicit WKTSpatialElement (similar to saying func.GeomFromText())
        self.r = Road(road_name='Dave Cres', road_geom=WKTSpatialElement('LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)', 4326))
        session.add(self.r)
        session.commit()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()

    # Test Geometry Functions

    def test_geometry_type(self):
        r = session.query(Road).get(1)
        l = session.query(Lake).get(1)
        s = session.query(Spot).get(1)        
        eq_(session.scalar(r.road_geom.geometry_type), 'ST_LineString')
        eq_(session.scalar(l.lake_geom.geometry_type), 'ST_Polygon')
        eq_(session.scalar(s.spot_location.geometry_type), 'ST_Point')
        eq_(session.scalar(functions.geometry_type(r.road_geom)), 'ST_LineString')
        ok_(session.query(Road).filter(Road.road_geom.geometry_type == 'ST_LineString').first())

    def test_geom_type(self):
        r = session.query(Road).get(1)
        l = session.query(Lake).get(1)
        s = session.query(Spot).get(1)
        eq_(r.road_geom.geom_type(session), 'LineString')
        eq_(l.lake_geom.geom_type(session), 'Polygon')
        eq_(s.spot_location.geom_type(session), 'Point')

    def test_wkt(self):
        l = session.query(Lake).get(1)
        assert session.scalar(self.r.road_geom.wkt) == 'LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)'
        eq_(session.scalar(l.lake_geom.wkt),'POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936))')
        eq_(session.scalar(l.lake_geom.wkt), l.lake_geom.geom_wkt)
        ok_(not session.query(Spot).filter(Spot.spot_location.wkt == 'POINT(0,0)').first())
        ok_(session.query(Spot).get(1) is 
            session.query(Spot).filter(Spot.spot_location == 'POINT(-88.5945861592357 42.9480095987261)').first())
        centroid_geom = DBSpatialElement(session.scalar(self.r.road_geom.centroid))
        eq_(session.scalar(centroid_geom.wkt), u'POINT(-88.5769371859941 42.9915634871979)')
        eq_(session.scalar(WKTSpatialElement('POINT(-88.5769371859941 42.9915634871979)').wkt), u'POINT(-88.5769371859941 42.9915634871979)')
        eq_(session.query(Spot.spot_location.wkt).filter(Spot.spot_id == 1).first(), (u'POINT(-88.5945861592357 42.9480095987261)',))

    def test_coords(self):
        eq_(self.r.road_geom.coords(session), [[-88.6748409363057,43.1035032292994],[-88.6464173694267,42.9981688343949],[-88.607961955414,42.9680732929936],[-88.5160033566879,42.9363057770701],[-88.4390925286624,43.0031847579618]])
        l = session.query(Lake).filter(Lake.lake_name=="Lake Deep").one()
        eq_(l.lake_geom.coords(session), [[[-88.9122611464968,43.038296178344],[-88.9222929936306,43.0399681528663],[-88.9323248407643,43.0282643312102],[-88.9206210191083,43.0182324840764],[-88.9105891719745,43.0165605095542],[-88.9005573248408,43.0232484076433],[-88.9072452229299,43.0282643312102],[-88.9122611464968,43.038296178344]]])
        s = session.query(Spot).filter(Spot.spot_height==102.34).one()
        eq_(s.spot_location.coords(session), [-88.905573420382197, 43.0048567324841])

    def test_wkb(self):
        print session.scalar(functions.wkt(self.r.road_geom.wkb))
        eq_(session.scalar(functions.wkt(func.GeomFromWKB(self.r.road_geom.wkb, 4326))), 
            u'LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)')
        eq_(session.scalar(self.r.road_geom.wkb), self.r.road_geom.geom_wkb)
        ok_(not session.query(Spot).filter(Spot.spot_location.wkb == '101').first())
        centroid_geom = DBSpatialElement(session.scalar(self.r.road_geom.centroid))
        eq_(session.scalar(functions.wkt(func.GeomFromWKB(centroid_geom.wkb, 4326))), u'POINT(-88.5769371859941 42.9915634871979)')

    def test_svg(self):
        eq_(session.scalar(self.r.road_geom.svg), u'M -88.674840936305699 -43.103503229299399 L -88.6464173694267 -42.998168834394903 -88.607961955413998 -42.968073292993601 -88.516003356687904 -42.936305777070103 -88.4390925286624 -43.003184757961797')
        ok_(self.r is session.query(Road).filter(Road.road_geom.svg == u'M -88.674840936305699 -43.103503229299399 L -88.6464173694267 -42.998168834394903 -88.607961955413998 -42.968073292993601 -88.516003356687904 -42.936305777070103 -88.4390925286624 -43.003184757961797').first())
        eq_(session.scalar(pg_functions.svg('POINT(-88.9055734203822 43.0048567324841)')), u'cx="-88.905573420382197" cy="-43.0048567324841"')
        ok_(session.query(Spot).filter(Spot.spot_location.svg == 'cx="-88.905573420382197" cy="-43.0048567324841"').first())

    def test_gml(self):
        eq_(session.scalar(self.r.road_geom.gml), '<gml:LineString srsName="EPSG:4326"><gml:coordinates>-88.674840936305699,43.103503229299399 -88.6464173694267,42.998168834394903 -88.607961955413998,42.968073292993601 -88.516003356687904,42.936305777070103 -88.4390925286624,43.003184757961797</gml:coordinates></gml:LineString>')

    def test_kml(self):
        s = session.query(Spot).filter(Spot.spot_height==420.40).one()
        eq_(session.scalar(s.spot_location.kml), u'<Point><coordinates>-88.594586159235703,42.948009598726102</coordinates></Point>')

    def test_geojson(self):
        s = session.query(Spot).filter(Spot.spot_height==420.40).one()
        ok_(True) # todo: test with version 1.3.4
        
    def test_dimension(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        eq_(session.scalar(r.road_geom.dimension), 1)
        eq_(session.scalar(l.lake_geom.dimension), 2)
        ok_(session.query(Spot).filter(Spot.spot_location.dimension == 0).first() is not None)
        eq_(session.scalar(functions.dimension('POINT(-88.5945861592357 42.9480095987261)')), 0)
        
    def test_srid(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.srid), 4326)
        ok_(session.query(Spot).filter(Spot.spot_location.srid == 4326).first() is not None)
        eq_(session.scalar(functions.srid('POINT(-88.5945861592357 42.9480095987261)')), 4326)

    def test_is_valid(self):
        ok_(session.query(Spot).filter(Spot.spot_location.is_valid == True).first() is not None)
        eq_(session.scalar(functions.is_valid('POINT(-88.5945861592357 42.9480095987261)')), True)

    def test_is_empty(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert not session.scalar(r.road_geom.is_empty)
        assert not session.scalar(l.lake_geom.is_empty)
        ok_(session.query(Spot).filter(Spot.spot_location.is_empty == False).first() is not None)
        eq_(session.scalar(functions.is_empty('POINT(-88.5945861592357 42.9480095987261)')), False)

    def test_is_simple(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.is_simple)
        assert session.scalar(l.lake_geom.is_simple)
        ok_(session.query(Spot).filter(Spot.spot_location.is_simple == True).first() is not None)
        eq_(session.scalar(functions.is_simple('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)')), False)

    def test_is_closed(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert not session.scalar(r.road_geom.is_closed)
        assert session.scalar(l.lake_geom.is_closed)
        ok_(session.query(Lake).filter(Lake.lake_geom.is_closed == True).first() is not None)
        eq_(session.scalar(functions.is_closed('LINESTRING(0 0, 1 1)')), False)

    def test_is_ring(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert not session.scalar(r.road_geom.is_ring)
        ok_(session.query(Road).filter(Road.road_geom.is_ring == True).first() is None)
        eq_(session.scalar(functions.is_ring('LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)')), False)

    def test_num_points(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        s = session.query(Spot).get(1)
        ok_(not session.scalar(l.lake_geom.num_points))
        eq_(session.scalar(r.road_geom.num_points), 5)
        ok_(not session.scalar(s.spot_location.num_points))
        ok_(session.query(Road).filter(Road.road_geom.num_points == 5).first() is not None)
        eq_(session.scalar(functions.num_points('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')), 4)

    def test_point_n(self):
        l = session.query(Lake).get(1)
        r = session.query(Road).get(1)
        ok_(not session.scalar(l.lake_geom.point_n(1)))
        ok_(session.query(Road).filter(Road.road_geom.point_n(5) == WKTSpatialElement('POINT(-88.3655256496815 43.1402866687898)')).first() is not None)
        eq_(session.scalar(r.road_geom.point_n(5).wkt), u'POINT(-88.3655256496815 43.1402866687898)')
        eq_(session.scalar(functions.wkt(functions.point_n('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)', 1)))
                           , u'POINT(77.29 29.07)')

    def test_persistent(self):
        eq_(session.scalar(functions.wkt(func.GeomFromWKB(self.r.road_geom.wkb, 4326))), 
            u'LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)')
        
        geom = WKTSpatialElement('POINT(30250865.9714116 -610981.481754275)', 2249)
        spot = Spot(spot_height=102.34, spot_location=geom)
        session.add(spot)
        session.commit();
        assert_almost_equal(session.scalar(spot.spot_location.x), 0)
        assert_almost_equal(session.scalar(spot.spot_location.y), 0)
        
    def test_eq(self):
        r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414)').one()
        r3 = session.query(Road).filter(Road.road_geom == r1.road_geom).one()
        ok_(r1 is r2 is r3)

    def test_ne(self):
        r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414)').one()
        ok_(r2 not in session.query(Road).filter(Road.road_geom != r2.road_geom).all())

    def test_length(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert_almost_equal(session.scalar(r.road_geom.length), 0.496071476676014)
        ok_(session.query(Road).filter(Road.road_geom.length > 0).first() is not None) 
        assert_almost_equal(session.scalar(functions.length('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')), 0.62916306324869398)

    def test_area(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        assert_almost_equal(session.scalar(l.lake_geom.area), 0.10475991566721)
        ok_(session.query(Lake).filter(Lake.lake_geom.area > 0).first() is not None)
        assert_almost_equal(session.scalar(functions.area(WKTSpatialElement('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))',2249))), 
                            928.625)

    def test_x(self):
        s = session.query(Spot).filter(Spot.spot_height==420.40).one()
        eq_(session.scalar(s.spot_location.x), -88.5945861592357)
        s = session.query(Spot).filter(and_(Spot.spot_location.x < 0, Spot.spot_location.y > 42)).all()
        ok_(s is not None)
        eq_(session.scalar(functions.x('POINT(-88.3655256496815 43.1402866687898)')), -88.3655256496815)

    def test_y(self):
        s = session.query(Spot).filter(Spot.spot_height==420.40).one()
        eq_(session.scalar(s.spot_location.y), 42.9480095987261)
        s = session.query(Spot).filter(and_(Spot.spot_location.y < 0, Spot.spot_location.y > 42)).all()
        ok_(s is not None)
        eq_(session.scalar(functions.y('POINT(-88.3655256496815 43.1402866687898)')), 43.1402866687898)

    def test_centroid(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        eq_(session.scalar(functions.wkt(r.road_geom.centroid)), u'POINT(-88.5889975373709 42.941769988935)')
        eq_(session.scalar(functions.wkt(l.lake_geom.centroid)), u'POINT(-88.9214538261088 43.0191497691548)')
        ok_(session.query(Spot).filter(Spot.spot_location.centroid == WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)')).first() is not None)
        eq_(session.scalar(functions.wkt(functions.centroid('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'))), 
            u'POINT(2.30769230769231 3.30769230769231)')

    def test_boundary(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.boundary)), u'MULTIPOINT(-88.5477708726115 42.6988853949045,-88.5912422101911 43.187101955414)')
        ok_(session.query(Road).filter(Road.road_geom.boundary == WKTSpatialElement('MULTIPOINT(-88.9139332929936 42.5082802993631,-88.3655256496815 43.1402866687898)')).first() is not None)
        eq_(session.scalar(functions.wkt(functions.boundary('POLYGON((1 1,0 0, -1 1, 1 1))'))), 
            u'LINESTRING(1 1,0 0,-1 1,1 1)')
       
    def test_buffer(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert_almost_equal(session.scalar(functions.area(r.road_geom.buffer(10.0, 8))), 321.93380659099699)
        ok_(session.query(Spot).filter(functions.within('POINT(-88.5945861592357 42.9480095987261)', Spot.spot_location.buffer(10))).first() is not None)
        assert_almost_equal(session.scalar(functions.area(functions.buffer('POINT(-88.5945861592357 42.9480095987261)', 10, 2))), 282.84271247461902)
        
    def test_convex_hull(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.convex_hull)), 
            u'POLYGON((-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414,-88.5477708726115 42.6988853949045))')
        ok_(session.query(Spot).filter(Spot.spot_location.convex_hull == WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)')).first() is not None)
        eq_(session.scalar(functions.wkt(functions.convex_hull('POINT(-88.5945861592357 42.9480095987261)'))), 
            u'POINT(-88.5945861592357 42.9480095987261)')

    def test_envelope(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.envelope)), 
            u'POLYGON((-88.6096339299363 42.6988853949045,-88.6096339299363 43.187101955414,-88.5477708726115 43.187101955414,-88.5477708726115 42.6988853949045,-88.6096339299363 42.6988853949045))')
        eq_(session.scalar(functions.geometry_type(self.r.road_geom.envelope)), 'ST_Polygon')
        ok_(session.query(Spot).filter(Spot.spot_location.envelope == WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)')).first() is not None)
        eq_(session.scalar(functions.wkt(functions.envelope('POINT(-88.5945861592357 42.9480095987261)'))), 
            u'POINT(-88.5945861592357 42.9480095987261)')
        
    def test_start_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.start_point)), 
            u'POINT(-88.5477708726115 42.6988853949045)')
        ok_(session.query(Road).filter(Road.road_geom.start_point == WKTSpatialElement('POINT(-88.9139332929936 42.5082802993631)')).first() is not None)
        eq_(session.scalar(functions.wkt(functions.start_point('LINESTRING(0 1, 0 2)'))), 
            u'POINT(0 1)')
        
    def test_end_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.end_point)), 
            u'POINT(-88.5912422101911 43.187101955414)')
        ok_(session.query(Road).filter(Road.road_geom.end_point == WKTSpatialElement('POINT(-88.3655256496815 43.1402866687898)')).first() is not None)
        eq_(session.scalar(functions.wkt(functions.end_point('LINESTRING(0 1, 0 2)'))), 
            u'POINT(0 2)')
        
    def test_transform(self):
        spot = session.query(Spot).get(1)
        # compare the coordinates using a tolerance, because they may vary on different systems
        assert_almost_equal(session.scalar(functions.x(spot.spot_location.transform(2249))), -3890517.6109559298)
        assert_almost_equal(session.scalar(functions.y(spot.spot_location.transform(2249))), 3627658.6746507999)
        ok_(session.query(Spot).filter(Spot.spot_location.transform(2249).wkt == 'POINT(-3890517.61095593 3627658.6746508)').first() is not None)
        eq_(session.scalar(functions.wkt(functions.transform(WKTSpatialElement('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))', 2249), 4326))), 
            u'POLYGON((-71.1776848522251 42.3902896512902,-71.1776843766326 42.3903829478009,-71.1775844305465 42.3903826677917,-71.1775825927231 42.3902893647987,-71.1776848522251 42.3902896512902))')

    # Test Geometry Relationships

    def test_equals(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Peter Rd').one()
        r3 = session.query(Road).filter(Road.road_name=='Paul St').one()
        equal_roads = session.query(Road).filter(Road.road_geom.equals(r1.road_geom)).all()
        ok_(r1 in equal_roads)
        ok_(r2 in equal_roads)
        ok_(r3 not in equal_roads)
        ok_(session.query(Spot).filter(Spot.spot_location.equals(WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)'))).first() is not None)
        eq_(session.scalar(functions.equals('POINT(-88.5945861592357 42.9480095987261)', 'POINT(-88.5945861592357 42.9480095987261)')), True)

    def test_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        r3 = session.query(Road).filter(Road.road_name=='Peter Rd').one()
        assert_almost_equal(session.scalar(r1.road_geom.distance(r2.road_geom)), 0.336997238682841)
        eq_(session.scalar(r1.road_geom.distance(r3.road_geom)), 0.0)
        ok_(session.query(Spot).filter(Spot.spot_location.distance(WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)')) < 10).first() is not None)
        assert_almost_equal(session.scalar(functions.distance('POINT(-88.5945861592357 42.9480095987261)', 'POINT(-88.5945861592357 42.9480095987261)')), 0)

    def test_within_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        roads_within_distance = session.query(Road).filter(
            Road.road_geom.within_distance(r1.road_geom, 0.20)).all()
        ok_(r2 in roads_within_distance)
        ok_(r3 not in roads_within_distance)
        eq_(session.scalar(functions.within_distance('POINT(-88.9139332929936 42.5082802993631)', 'POINT(-88.9139332929936 35.5082802993631)', 10)), True)

    def test_disjoint(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        disjoint_roads = session.query(Road).filter(Road.road_geom.disjoint(r1.road_geom)).all()
        ok_(r2 not in disjoint_roads)
        ok_(r3 in disjoint_roads)
        eq_(session.scalar(functions.disjoint('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )')), True)


    def test_intersects(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        intersecting_roads = session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()
        ok_(r2 in intersecting_roads)
        ok_(r3 not in intersecting_roads)
        eq_(session.scalar(functions.intersects('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )')), False)

    def test_touches(self):
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        r = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        touching_lakes = session.query(Lake).filter(Lake.lake_geom.touches(r.road_geom)).all()
        ok_(not session.scalar(l1.lake_geom.touches(r.road_geom)))
        ok_(session.scalar(l2.lake_geom.touches(r.road_geom)))
        ok_(l1 not in touching_lakes)
        ok_(l2 in touching_lakes)
        eq_(session.scalar(functions.touches('POINT(1 1)', 'LINESTRING (0 0, 1 1, 0 2)')), False)

    def test_crosses(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Paul St').one()
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        crossing_roads = session.query(Road).filter(Road.road_geom.crosses(l.lake_geom)).all()
        ok_(not session.scalar(r1.road_geom.crosses(l.lake_geom)))
        ok_(session.scalar(r2.road_geom.crosses(l.lake_geom)))
        ok_(r1 not in crossing_roads)
        ok_(r2 in crossing_roads)
        eq_(session.scalar(functions.crosses('LINESTRING(0 1, 2 1)', 'LINESTRING (0 0, 1 2, 0 2)')), True)

    def test_within(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        spots_within = session.query(Spot).filter(Spot.spot_location.within(l.lake_geom)).all()
        ok_(session.scalar(p1.spot_location.within(l.lake_geom)))
        ok_(not session.scalar(p2.spot_location.within(l.lake_geom)))
        ok_(p1 in spots_within)
        ok_(p2 not in spots_within)
        eq_(session.scalar(functions.within('LINESTRING(0 1, 2 1)', 'POLYGON((-1 -1, 3 -1, 3 2, -1 2, -1 -1))')), True)
        buffer_geom = DBSpatialElement(session.scalar(l.lake_geom.buffer(10.0)))
        spots_within = session.query(Spot).filter(l.lake_geom.within(buffer_geom)).all()
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
        eq_(session.scalar(functions.overlaps('POLYGON((2 1, 4 1, 4 3, 2 3, 2 1))', 'POLYGON((-1 -1, 3 -1, 3 2, -1 2, -1 -1))')), True)

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
        eq_(session.scalar(functions.gcontains('LINESTRING(0 1, 2 1)', 'POLYGON((-1 -1, 3 -1, 3 2, -1 2, -1 -1))')), False)

    def test_covers(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        covering_lakes = session.query(Lake).filter(Lake.lake_geom.covers(p1.spot_location)).all()
        ok_(session.scalar(l.lake_geom.covers(p1.spot_location)))
        ok_(not session.scalar(l.lake_geom.covers(p2.spot_location)))
        ok_(l in covering_lakes)
        ok_(l1 not in covering_lakes)
        eq_(session.scalar(functions.gcontains('LINESTRING(0 1, 2 1)', 'POLYGON((-1 -1, 3 -1, 3 2, -1 2, -1 -1))')), False)

    def test_covered_by(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        covered_spots = session.query(Spot).filter(Spot.spot_location.covered_by(l.lake_geom)).all()
        ok_(session.scalar(p1.spot_location.covered_by(l.lake_geom)))
        ok_(not session.scalar(p2.spot_location.covered_by(l.lake_geom)))
        ok_(p1 in covered_spots)
        ok_(p2 not in covered_spots)
        eq_(session.scalar(functions.covered_by('LINESTRING(0 1, 2 1)', 'POLYGON((-1 -1, 3 -1, 3 2, -1 2, -1 -1))')), True)

    def test_intersection(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        r = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        s = session.query(Spot).filter(Spot.spot_height==454.66).one()
        eq_(session.scalar(func.ST_AsText(l.lake_geom.intersection(s.spot_location))), 'GEOMETRYCOLLECTION EMPTY')
        eq_(session.scalar(func.ST_AsText(session.scalar(l.lake_geom.intersection(r.road_geom)))), 'POINT(-89.0710987261147 43.243949044586)')
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        r = session.query(Road).filter(Road.road_name=='Paul St').one()
        eq_(session.scalar(func.ST_AsText(session.scalar(l.lake_geom.intersection(r.road_geom)))), 'LINESTRING(-88.1430673666454 42.6255500261493,-88.1140839697546 42.6230657349872)')
        ok_(session.query(Lake).filter(Lake.lake_geom.intersection(r.road_geom).wkt == 'LINESTRING(-88.1430673666454 42.6255500261493,-88.1140839697546 42.6230657349872)').first() is not None)

    def test_extent(self):
        l = session.query(functions.extent(Lake.lake_geom)). \
                filter(Lake.lake_geom != None).scalar()
        r = session.query(functions.extent(Road.road_geom)). \
                filter(Road.road_geom != None).scalar()
        s = session.query(functions.extent(Spot.spot_location)). \
                filter(Spot.spot_location != None).scalar()
        sh = session.query(functions.extent(Shape.shape_geom)). \
                filter(Shape.shape_geom != None).scalar()
        
        eq_(l, "BOX(-89.1329617834395 42.565127388535,-88.0846337579618 43.243949044586)")
        eq_(r, "BOX(-89.2449842484076 42.5082802993631,-88.0110670509554 43.3175159681529)")
        eq_(s, "BOX(-89.201512910828 42.6269904904459,-88.3304141847134 43.1051752038217)")
        eq_(sh, "BOX(-88.7968950764331 42.5584395350319,-88.0110670509554 43.2339172420382)")

    def test_union(self):
        l = session.query(functions.geometry_type(functions.union(Lake.lake_geom))). \
                filter(Lake.lake_geom != None).scalar()
        r = session.query(functions.geometry_type(functions.union(Road.road_geom))). \
                filter(Road.road_geom != None).scalar()
        s = session.query(functions.geometry_type(functions.union(Spot.spot_location))). \
                filter(Spot.spot_location != None).scalar()
        sh = session.query(functions.geometry_type(functions.union(Shape.shape_geom))). \
                filter(Shape.shape_geom != None).scalar()
        la = session.query(functions.union(Lake.lake_geom).wkt). \
                filter(Lake.lake_geom != None).scalar()
        
        eq_(l, "ST_MultiPolygon")
        eq_(r, "ST_MultiLineString")
        eq_(s, "ST_MultiPoint")
        eq_(sh, "ST_GeometryCollection")
        eq_(la, 'MULTIPOLYGON(((-88.1147292993631 42.7540605095542,-88.1548566878981 42.7824840764331,-88.1799363057325 42.7707802547771,-88.188296178344 42.7323248407643,-88.1832802547771 42.6955414012739,-88.1565286624204 42.6771496815287,-88.1448248407643 42.6336783439491,-88.131449044586 42.5718152866242,-88.1013535031847 42.565127388535,-88.1080414012739 42.5868630573248,-88.1164012738854 42.6119426751592,-88.1080414012739 42.6520700636943,-88.0980095541401 42.6838375796178,-88.0846337579618 42.7139331210191,-88.1013535031847 42.7423566878981,-88.1147292993631 42.7540605095542)),((-88.7878611897291 43.1554581337873,-88.7734872611465 43.0867834394905,-88.7517515923567 43.0299363057325,-88.7433917197452 42.9730891719745,-88.7517515923567 42.9145700636943,-88.7734872611465 42.8710987261147,-88.8102707006369 42.8343152866242,-88.8687898089172 42.815923566879,-88.9072452229299 42.8142515923567,-88.9440286624204 42.8292993630573,-88.9774681528663 42.8644108280255,-89.0042197452229 42.8961783439491,-89.0209394904459 42.9179140127389,-89.0343152866242 42.953025477707,-89.0694267515924 42.9898089171975,-89.112898089172 43.0132165605096,-89.1312898089172 43.0466560509554,-89.1329617834395 43.0884554140127,-89.1078821656051 43.1135350318471,-89.0694267515924 43.1335987261147,-89.0510350318471 43.1335987261147,-89.0393312101911 43.1386146496815,-89.0376592356688 43.1369426751592,-89.0292993630573 43.1519904458599,-89.0376592356688 43.175398089172,-89.0543789808917 43.203821656051,-89.0660828025478 43.2238853503185,-89.0710987261147 43.243949044586,-89.0410031847134 43.2389331210191,-89.0042197452229 43.2138535031847,-88.947372611465 43.1937898089172,-88.8738057324841 43.1620222929936,-88.7937087643429 43.1588812709654,-88.7968950764331 43.2305732929936,-88.7250001719745 43.2339172420382,-88.716640299363 43.1570064140127,-88.7878611897291 43.1554581337873)))')

    def test_collect(self):
        l = session.query(functions.geometry_type(functions.collect(Lake.lake_geom))). \
                filter(Lake.lake_geom != None).scalar()
        r = session.query(functions.geometry_type(functions.collect(Road.road_geom))). \
                filter(Road.road_geom != None).scalar()
        s = session.query(functions.geometry_type(functions.collect(Spot.spot_location))). \
                filter(Spot.spot_location != None).scalar()
        sh = session.query(functions.geometry_type(functions.collect(Shape.shape_geom))). \
                filter(Shape.shape_geom != None).scalar()
        la = session.query(functions.collect(Lake.lake_geom).wkt). \
                filter(Lake.lake_geom != None).scalar()

        eq_(l, "ST_MultiPolygon")
        eq_(r, "ST_MultiLineString")
        eq_(s, "ST_MultiPoint")
        eq_(sh, "ST_GeometryCollection")
        eq_(la, "MULTIPOLYGON(((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936)),((-88.1147292993631 42.7540605095542,-88.1548566878981 42.7824840764331,-88.1799363057325 42.7707802547771,-88.188296178344 42.7323248407643,-88.1832802547771 42.6955414012739,-88.1565286624204 42.6771496815287,-88.1448248407643 42.6336783439491,-88.131449044586 42.5718152866242,-88.1013535031847 42.565127388535,-88.1080414012739 42.5868630573248,-88.1164012738854 42.6119426751592,-88.1080414012739 42.6520700636943,-88.0980095541401 42.6838375796178,-88.0846337579618 42.7139331210191,-88.1013535031847 42.7423566878981,-88.1147292993631 42.7540605095542)),((-89.0694267515924 43.1335987261147,-89.1078821656051 43.1135350318471,-89.1329617834395 43.0884554140127,-89.1312898089172 43.0466560509554,-89.112898089172 43.0132165605096,-89.0694267515924 42.9898089171975,-89.0343152866242 42.953025477707,-89.0209394904459 42.9179140127389,-89.0042197452229 42.8961783439491,-88.9774681528663 42.8644108280255,-88.9440286624204 42.8292993630573,-88.9072452229299 42.8142515923567,-88.8687898089172 42.815923566879,-88.8687898089172 42.815923566879,-88.8102707006369 42.8343152866242,-88.7734872611465 42.8710987261147,-88.7517515923567 42.9145700636943,-88.7433917197452 42.9730891719745,-88.7517515923567 43.0299363057325,-88.7734872611465 43.0867834394905,-88.7885352038217 43.158678388535,-88.8738057324841 43.1620222929936,-88.947372611465 43.1937898089172,-89.0042197452229 43.2138535031847,-89.0410031847134 43.2389331210191,-89.0710987261147 43.243949044586,-89.0660828025478 43.2238853503185,-89.0543789808917 43.203821656051,-89.0376592356688 43.175398089172,-89.0292993630573 43.1519904458599,-89.0376592356688 43.1369426751592,-89.0393312101911 43.1386146496815,-89.0393312101911 43.1386146496815,-89.0510350318471 43.1335987261147,-89.0694267515924 43.1335987261147)),((-88.9122611464968 43.038296178344,-88.9222929936306 43.0399681528663,-88.9323248407643 43.0282643312102,-88.9206210191083 43.0182324840764,-88.9105891719745 43.0165605095542,-88.9005573248408 43.0232484076433,-88.9072452229299 43.0282643312102,-88.9122611464968 43.038296178344)))")

    def test_within_distance(self):
        ok_(session.scalar(functions._within_distance('POINT(-88.9139332929936 42.5082802993631)', 'POINT(-88.9139332929936 35.5082802993631)', 10)))
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

    def test_null(self):
        spot_null = Spot(spot_height=420.40, spot_location=None)
        session.add(spot_null)
        session.commit();
        spot_null = session.query(Spot).filter(Spot.spot_location == None).one()
        eq_(spot_null.spot_location, None)
        ok_(session.query(Spot).filter(Spot.spot_location != None).first() is not None)

    def test_issue17(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        lakes_within_distance = session.query(Lake).filter(
            Lake.road.has(functions._within_distance(Road.road_geom,r1.road_geom, 0.20))).all()
        lakes_within_distance = session.query(Lake).filter(
            Lake.road.has(functions.within_distance(Road.road_geom,r1.road_geom, 0.20))).all()
