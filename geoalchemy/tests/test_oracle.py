from unittest import TestCase
from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        Numeric, func, Table, and_, not_)
from sqlalchemy.orm import sessionmaker, mapper, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from geoalchemy.geometry import LineString, Point, Polygon
from sqlalchemy.schema import Sequence
from sqlalchemy.sql.expression import text, select

from geoalchemy import (GeometryCollection, GeometryColumn,
        GeometryDDL, WKTSpatialElement, DBSpatialElement, GeometryExtensionColumn)
from geoalchemy.functions import functions
from geoalchemy.oracle import OracleComparator, oracle_functions, ORACLE_NULL_GEOMETRY,\
    OracleSpatialDialect

from nose.tools import eq_, ok_, raises, assert_almost_equal

import os
os.environ['ORACLE_HOME'] = '/usr/lib/oracle/xe/app/oracle/product/10.2.0/server'
os.environ['LD_LIBRARY'] = '/usr/lib/oracle/xe/app/oracle/product/10.2.0/server/lib'


import cx_Oracle

#engine = create_engine('oracle://gis:gis@localhost:1521/gis', echo=True)
engine = create_engine('oracle://system:system@172.16.53.129:1521/gis', echo=False)

metadata = MetaData(engine)
session = sessionmaker(bind=engine)()
Base = declarative_base(metadata=metadata)

diminfo = "MDSYS.SDO_DIM_ARRAY("\
            "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.000000005),"\
            "MDSYS.SDO_DIM_ELEMENT('LATITUDE', -90, 90, 0.000000005)"\
            ")"
diminfo_ = text(diminfo)
tolerance = 0.000000005

class Road(Base):
    __tablename__ = 'roads'

    road_id = Column(Integer, Sequence('roads_id_seq'), primary_key=True)
    road_name = Column(String(40))
    road_geom = GeometryColumn(LineString(2, diminfo=diminfo), comparator=OracleComparator, nullable=False)

road_alias = aliased(Road)

class Lake(Base):
    __tablename__ = 'lakes'

    lake_id = Column(Integer, Sequence('lakes_id_seq'), primary_key=True)
    lake_name = Column(String(40))
    lake_geom = GeometryColumn(Polygon(2, diminfo=diminfo), comparator=OracleComparator)

lake_alias = aliased(Lake)

spots_table = Table('spots', metadata,
                    Column('spot_id', Integer, Sequence('spots_id_seq'), primary_key=True),
                    Column('spot_height', Numeric),
                    GeometryExtensionColumn('spot_location', Point(2, diminfo=diminfo)))

class Spot(object):
    def __init__(self, spot_id=None, spot_height=None, spot_location=None):
        self.spot_id = spot_id
        self.spot_height = spot_height
        self.spot_location = spot_location

mapper(Spot, spots_table, properties={
            'spot_location': GeometryColumn(spots_table.c.spot_location, 
                                            comparator=OracleComparator)}) 
spot_alias = aliased(Spot)        
                         
class Shape(Base):
    __tablename__ = 'shapes'

    shape_id = Column(Integer, Sequence('shapes_id_seq'), primary_key=True)
    shape_name = Column(String(40))
    shape_geom = GeometryColumn(GeometryCollection(2, diminfo=diminfo))

# enable the DDL extension, which allows CREATE/DROP operations
# to work correctly.  This is not needed if working with externally
# defined tables.    
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)
GeometryDDL(spots_table)
GeometryDDL(Shape.__table__)


class TestGeometry(TestCase):
    """Tests for Oracle
    """

    SLOW_SYSTEM = True
    database_is_set_up = False

    def __init__(self, methodName):
        TestCase.__init__(self, methodName)

    def setUp(self):
        if not session.is_active:
            session.rollback()
        
        if not TestGeometry.SLOW_SYSTEM or not TestGeometry.database_is_set_up:
            
            metadata.drop_all()
            metadata.create_all()
    
            # Add objects.  We can use strings...
            session.add_all([
                Road(road_name='Jeff Rd', road_geom='LINESTRING(-88.9139332929936 42.5082802993631,-88.8203027197452 42.5985669235669,-88.7383759681529 42.7239650127389,-88.6113059044586 42.9680732929936,-88.3655256496815 43.1402866687898)'),
                Road(road_name='Peter Rd', road_geom='LINESTRING(-88.9139332929936 42.5082802993631,-88.8203027197452 42.5985669235669,-88.7383759681529 42.7239650127389,-88.6113059044586 42.9680732929936,-88.3655256496815 43.1402866687898)'),
                Road(road_name='Geordie Rd', road_geom='LINESTRING(-89.2232485796178 42.6420382611465,-89.2449842484076 42.9179140573248,-89.2316084522293 43.106847178344,-89.0710987261147 43.243949044586,-89.092834566879 43.2957802993631,-89.0309715095541 43.3175159681529)'),
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
                # the following spots are for test_within_distance
                Spot(spot_height=420.40, spot_location='POINT(0 0)'),
                Spot(spot_height=102.34, spot_location='POINT(10 10)'),
                Spot(spot_height=388.62, spot_location='POINT(10 11)'),
                Spot(spot_height=1454.66, spot_location='POINT(40 34)'),
                Spot(spot_height=54.66, spot_location='POINT(5 5)'),
                Spot(spot_height=333.12, spot_location='POINT(2 3)'),
                Spot(spot_height=783.55, spot_location='POINT(38 34)'),
                Spot(spot_height=3454.67, spot_location='POINT(-134 45)'),

                Shape(shape_name='Bus Stop', shape_geom='GEOMETRYCOLLECTION(POINT(-88.3304141847134 42.6269904904459))'),
                Shape(shape_name='Jogging Track', shape_geom='GEOMETRYCOLLECTION(LINESTRING(-88.2652071783439 42.5584395350319,-88.1598727834395 42.6269904904459,-88.1013536751592 42.621974566879,-88.0244428471338 42.6437102356688,-88.0110670509554 42.6771497261147))'),
                Shape(shape_name='Play Ground', shape_geom='GEOMETRYCOLLECTION(POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936)))'),
            ])
     
            # or use an explicit WKTSpatialElement (similar to saying func.GeomFromText())
            self.r = Road(road_name='Dave Cres', road_geom=WKTSpatialElement('LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)', 4326))
            session.add(self.r)
            session.commit()
            
            TestGeometry.database_is_set_up = True
        
        self.r = session.query(Road).filter(Road.road_name=='Dave Cres').one()

    def tearDown(self):
        if not TestGeometry.SLOW_SYSTEM:
            session.rollback()
            metadata.drop_all()

    def test_WKBSpatialElement(self):
        s = session.query(Spot).get(1)     
        ok_(isinstance(s.spot_location.geom_wkb, buffer))
        ok_(session.scalar(func.SDO_UTIL.VALIDATE_WKBGEOMETRY(s.spot_location.geom_wkb)))
        ok_(session.scalar(func.SDO_GEOM.SDO_MBR(s.spot_location.desc)))
        
    # Test Geometry Functions

    def test_geometry_type(self):
        r = session.query(Road).get(1)
        l = session.query(Lake).get(1)
        s = session.query(Spot).get(1) 
        
        eq_(session.scalar(r.road_geom.geometry_type), u'ST_LINESTRING')
        eq_(session.scalar(l.lake_geom.geometry_type), u'ST_POLYGON')
        eq_(session.scalar(s.spot_location.geometry_type), u'ST_POINT')
        eq_(session.scalar(functions.geometry_type(r.road_geom)), u'ST_LINESTRING')
        ok_(session.query(Road).filter(Road.road_geom.geometry_type == u'ST_LINESTRING').first())
        
    def test_gtype(self):
        r = session.query(Road).get(1)
        l = session.query(Lake).get(1)
        s = session.query(Spot).get(1) 
        
        # see 'Valid SDO_GTYPE Values': http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objrelschema.htm#g1013735
        eq_(session.scalar(r.road_geom.gtype), 2)
        eq_(session.scalar(l.lake_geom.gtype), 3)
        eq_(session.scalar(s.spot_location.gtype), 1)
        eq_(session.scalar(oracle_functions.gtype(r.road_geom)), 2)
        
        # note we are using a table alias!
        road = session.query(road_alias).filter(road_alias.road_geom.gtype == 2).first()
        ok_(road)

    def test_wkt(self):
        l = session.query(Lake).get(1)
        eq_(session.scalar(self.r.road_geom.wkt), 'LINESTRING (-88.6748409363057 43.1035032292994, -88.6464173694267 42.9981688343949, -88.607961955414 42.9680732929936, -88.5160033566879 42.9363057770701, -88.4390925286624 43.0031847579618)')
        eq_(session.scalar(l.lake_geom.wkt), 'POLYGON ((-88.7968950764331 43.2305732929936, -88.7935511273885 43.1553344394904, -88.716640299363 43.1570064140127, -88.7250001719745 43.2339172420382, -88.7968950764331 43.2305732929936))')
        ok_(session.query(Spot).filter(Spot.spot_location.wkt == 'POINT (-88.5945861592357 42.9480095987261)').first())

        ok_(session.query(Spot).get(1) is 
            session.query(Spot).filter(Spot.spot_location == 'POINT(-88.5945861592357 42.9480095987261)').first())
        
        """At the moment, cx_Oracle does not support cx_Oracle.Object as parameter, so DBSpatialElement
        can not be used for Oracle, see also:
        http://sourceforge.net/mailarchive/message.php?msg_name=AANLkTilkBwWsIy0yEFQpPOvQiF-k9RxvlYlKf2KyaOfw%40mail.gmail.com
        """
#        centroid_geom = DBSpatialElement(session.scalar(l.lake_geom.centroid(0.000000005)))
#        eq_(session.scalar(centroid_geom.wkt), u'POINT(-88.5769371859941 42.9915634871979)')

        eq_(session.scalar(WKTSpatialElement('POINT(-88.5769371859941 42.9915634871979)').wkt), 'POINT (-88.5769371859941 42.9915634871979)')

    def test_coords(self):
        eq_(self.r.road_geom.coords(session), [[-88.6748409363057,43.1035032292994],[-88.6464173694267,42.9981688343949],[-88.607961955414,42.9680732929936],[-88.5160033566879,42.9363057770701],[-88.4390925286624,43.0031847579618]])
        l = session.query(Lake).filter(Lake.lake_name=="Lake Deep").one()
        eq_(l.lake_geom.coords(session), [[[-88.9122611464968,43.038296178344],[-88.9222929936306,43.0399681528663],[-88.9323248407643,43.0282643312102],[-88.9206210191083,43.0182324840764],[-88.9105891719745,43.0165605095542],[-88.9005573248408,43.0232484076433],[-88.9072452229299,43.0282643312102],[-88.9122611464968,43.038296178344]]])
        s = session.query(Spot).get(2)
        eq_(s.spot_location.coords(session), [-88.905573420382197, 43.0048567324841])

    def test_wkb(self):
        eq_(session.scalar(functions.wkt(func.SDO_GEOMETRY(self.r.road_geom.wkb, 4326))), 
            u'LINESTRING (-88.6748409363057 43.1035032292994, -88.6464173694267 42.9981688343949, -88.607961955414 42.9680732929936, -88.5160033566879 42.9363057770701, -88.4390925286624 43.0031847579618)')
        eq_(session.scalar(self.r.road_geom.wkb), self.r.road_geom.geom_wkb)
        ok_(not session.query(Spot).filter(func.DBMS_LOB.Compare(Spot.spot_location.wkb, func.to_blob('101')) == 0).first())
#        centroid_geom = DBSpatialElement(session.scalar(self.r.road_geom.centroid))
#        eq_(session.scalar(functions.wkt(func.GeomFromWKB(centroid_geom.wkb, 4326))), u'POINT(-88.5769371859941 42.9915634871979)')


    def test_gml(self):
        eq_(session.scalar(self.r.road_geom.gml), u'<gml:LineString srsName="SDO:4326" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates decimal="." cs="," ts=" ">-88.6748409363057,43.1035032292994 -88.6464173694267,42.9981688343949 -88.607961955414,42.9680732929936 -88.5160033566879,42.9363057770701 -88.4390925286624,43.0031847579618 </gml:coordinates></gml:LineString>')

    def test_gml311(self):
        eq_(session.scalar(self.r.road_geom.gml311), u'<gml:Curve srsName="SDO:4326" xmlns:gml="http://www.opengis.net/gml"><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">-88.6748409363057 43.1035032292994 -88.6464173694267 42.9981688343949 -88.607961955414 42.9680732929936 -88.5160033566879 42.9363057770701 -88.4390925286624 43.0031847579618 </gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>')
    
    def test_kml(self):
        s = session.query(Spot).get(1)
        eq_(session.scalar(s.spot_location.kml),  u'<Point><extrude>0</extrude><tessellate>0</tessellate><altitudeMode>relativeToGround</altitudeMode><coordinates>-88.5945861592357,42.9480095987261 </coordinates></Point>')
  
    def test_dims(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        eq_(session.scalar(r.road_geom.dims), 2)
        eq_(session.scalar(l.lake_geom.dims), 2)
        ok_(session.query(spot_alias).filter(spot_alias.spot_location.dims == 2).first() is not None)
        eq_(session.scalar(oracle_functions.dims('POINT(-88.5945861592357 42.9480095987261)')), 2)
        
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
        
    @raises(NotImplementedError)
    def test_is_valid(self):
        session.scalar(self.r.road_geom.is_valid)

    def test_is_empty(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert not session.scalar(r.road_geom.is_empty)
        assert not session.scalar(l.lake_geom.is_empty)
        ok_(session.query(Spot).filter(not_(Spot.spot_location.is_empty)).first() is not None)
        eq_(session.scalar(functions.is_empty('POINT(-88.5945861592357 42.9480095987261)')), False)

    def test_is_simple(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.is_simple)
        assert session.scalar(l.lake_geom.is_simple)
        ok_(session.query(Spot).filter(Spot.spot_location.is_simple).first() is not None)
        eq_(session.scalar(functions.is_simple('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)')), False)

    def test_is_closed(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert not session.scalar(r.road_geom.is_closed)
        # note Oracle always returns FALSE for polygons
        assert not session.scalar(l.lake_geom.is_closed) 
        ok_(session.query(Lake).filter(Lake.lake_geom.is_closed).first() is None)
        # note that we manually have to set a geometry type for WKTSpatialElement 
        eq_(session.scalar(functions.is_closed(WKTSpatialElement('LINESTRING(0 0, 1 1)', geometry_type=LineString.name))), False)

    def test_is_ring(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert not session.scalar(r.road_geom.is_ring)
        ok_(session.query(Road).filter(Road.road_geom.is_ring).first() is None)
        eq_(session.scalar(functions.is_ring(WKTSpatialElement('LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)', geometry_type=LineString.name))), False)

    def test_num_points(self):
        r = session.query(Road).get(1)
        eq_(session.scalar(r.road_geom.num_points), 5)
        ok_(session.query(Road).filter(Road.road_geom.num_points == 5).first() is not None)
        eq_(session.scalar(functions.num_points(WKTSpatialElement('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)', geometry_type=LineString.name))), 
            4)

    def test_point_n(self):
        r = session.query(Road).get(1)
        ok_(session.query(Road).filter(and_(Road.road_geom.point_n(5) <> None,  
                                            functions.wkt(Road.road_geom.point_n(5)) == 'POINT (-88.3655256496815 43.1402866687898)')).first() 
                                            is not None)
        eq_(session.scalar(r.road_geom.point_n(5).wkt), u'POINT (-88.3655256496815 43.1402866687898)')
        eq_(session.scalar(functions.wkt(functions.point_n(
                                                WKTSpatialElement('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)', geometry_type=LineString.name), 1))), 
            u'POINT (77.29 29.07)')

    def test_persistent(self):
        eq_(session.scalar(functions.wkt(func.SDO_GEOMETRY(self.r.road_geom.wkb, 4326))), 
            u'LINESTRING (-88.6748409363057 43.1035032292994, -88.6464173694267 42.9981688343949, -88.607961955414 42.9680732929936, -88.5160033566879 42.9363057770701, -88.4390925286624 43.0031847579618)')
        
        geom = WKTSpatialElement('POINT(30250865.9714116 -610981.481754275)', 2249)
        spot = Spot(spot_height=102.34, spot_location=geom)
        session.add(spot)
        session.commit();
        assert_almost_equal(float(session.scalar(spot.spot_location.x)), 0)
        assert_almost_equal(float(session.scalar(spot.spot_location.y)), 0)
        session.delete(spot)
        session.commit()
        
    def test_eq(self):
        r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414)').one()
        r3 = session.query(Road).filter(Road.road_geom == r1.road_geom).one()
        ok_(r1 is r2 is r3)

    def test_length(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()                                      
        assert_almost_equal(float(session.scalar(r.road_geom.length)), 54711.459044609008)
        ok_(session.query(Road).filter(Road.road_geom.length > 0).first() is not None) 
        assert_almost_equal(float(session.scalar(functions.length('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)', diminfo_))),
                            66830.9630972249)

    def test_area(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        assert_almost_equal(float(session.scalar(l.lake_geom.area(tolerance, auto_diminfo=False))), 104854567.261647)
        assert_almost_equal(float(session.scalar(l.lake_geom.area(
                                                 OracleSpatialDialect.get_diminfo_select(Lake.lake_geom), 
                                                 auto_diminfo=False))), 104854567.261647)
        assert_almost_equal(float(session.scalar(l.lake_geom.area(
                                                 OracleSpatialDialect.get_diminfo_select(Lake.__table__.c.lake_geom), 
                                                 auto_diminfo=False))), 104854567.261647)
        ok_(session.query(Lake).filter(Lake.lake_geom.area > 0).first() is not None)
        assert_almost_equal(float(session.scalar(functions.area(WKTSpatialElement('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))',2249), diminfo_))), 
                            86.272430609366495)

    def test_x(self):
        s = session.query(Spot).get(1)
        assert_almost_equal(float(session.scalar(s.spot_location.x)), -88.594586159235689)
        s = session.query(Spot).filter(and_(Spot.spot_location.x < 0, Spot.spot_location.y > 42)).all()
        ok_(s is not None)
        assert_almost_equal(float(session.scalar(functions.x(WKTSpatialElement('POINT(-88.3655256496815 43.1402866687898)', geometry_type=Point.name)))), 
                            -88.3655256496815)

    def test_y(self):
        s = session.query(Spot).get(1)
        assert_almost_equal(float(session.scalar(s.spot_location.y)), 42.9480095987261)
        s = session.query(Spot).filter(and_(Spot.spot_location.y < 0, Spot.spot_location.y > 42)).all()
        ok_(s is not None)
        assert_almost_equal(float(session.scalar(functions.y(WKTSpatialElement('POINT(-88.3655256496815 43.1402866687898)', geometry_type=Point.name)))), 
                            43.1402866687898)

    def test_centroid(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        # Oracle does not support centroid for LineString
        eq_(session.scalar(functions.wkt(r.road_geom.centroid)), None)
        eq_(session.scalar(functions.wkt(l.lake_geom.centroid)), u'POINT (-88.9213649409212 43.0190392609092)')
        ok_(session.query(Spot).filter(functions.wkt(Spot.spot_location.centroid) == 'POINT (-88.5945861592357 42.9480095987261)').first() is not None)
        eq_(session.scalar(functions.wkt(functions.centroid('MULTIPOINT((-1 0), (-1 2), (-1 3), (-1 4), (-1 7), (0 1), (0 3), (1 1), (2 0), (6 0), (7 8), (9 8), (10 6) )', diminfo_))), 
            u'POINT (2.30460703912783 3.31788085910597)')

    def test_boundary(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.boundary.wkt), u'MULTIPOINT ((-88.5477708726115 42.6988853949045), (-88.5912422101911 43.187101955414))')
        ok_(session.query(Road).filter(Road.road_geom.boundary.wkt == 'MULTIPOINT ((-88.9139332929936 42.5082802993631), (-88.3655256496815 43.1402866687898))').first() is not None)
        eq_(session.scalar(functions.wkt(functions.boundary(WKTSpatialElement('POLYGON((1 1,0 0, -1 1, 1 1))', geometry_type=Polygon.name)))), 
            u'LINESTRING (1.0 1.0, 0.0 0.0, -1.0 1.0, 1.0 1.0)')
       
    def test_buffer(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert_almost_equal(float(session.scalar(functions.area(r.road_geom.buffer(10.0), diminfo_))), 1094509.76889366)
        ok_(session.query(Spot).filter(functions.within(
                                                'POINT(-88.5945861592357 42.9480095987261)', 
                                                Spot.spot_location.buffer(10))).first() is not None)
        assert_almost_equal(float(session.scalar(functions.area(functions.buffer(
                                        'POINT(-88.5945861592357 42.9480095987261)', diminfo_, 10, 
                                        'unit=km arc_tolerance=0.05'), diminfo_))), 
                            312144711.50297302)
        
    def test_convex_hull(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.convex_hull)), 
            u'POLYGON ((-88.5477708728477 42.6988853969538, -88.5912422100661 43.1871019533972, -88.6029460317718 43.0884554581896, -88.6096339299412 42.9697452675198, -88.5477708728477 42.6988853969538))')
        # Oracle does not support ConvexHull for points
        ok_(session.query(Spot).filter(functions.equals(Spot.spot_location.convex_hull,
                                                        WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)'))).first() is None)
        eq_(session.scalar(functions.wkt(functions.convex_hull('LINESTRING(0 0, 1 1, 1 0)', diminfo_))), 
            u'POLYGON ((0.999999915732742 8.48487373222706E-8, 0.999999915755202 0.999999915201249, 8.4273559349594E-8 8.48422467917753E-8, 0.999999915732742 8.48487373222706E-8))')

    def test_envelope(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.envelope)), 
            u'POLYGON ((-88.6096339299363 42.6988853949045, -88.5477708726115 42.6988853949045, -88.5477708726115 43.187101955414, -88.6096339299363 43.187101955414, -88.6096339299363 42.6988853949045))')
        eq_(session.scalar(functions.geometry_type(self.r.road_geom.envelope)), u'ST_POLYGON')
        ok_(session.query(Spot).filter(functions.wkt(Spot.spot_location.envelope) == 'POINT (-88.5945861592357 42.9480095987261)').first() is not None)
        eq_(session.scalar(functions.wkt(functions.envelope(WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)', geometry_type=Point.name)))), 
            u'POINT (-88.5945861592357 42.9480095987261)')
        
    def test_start_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.start_point)), 
            u'POINT (-88.5477708726115 42.6988853949045)')
        ok_(session.query(Road).filter(functions.wkt(Road.road_geom.start_point) ==
                                                        'POINT (-88.9139332929936 42.5082802993631)').first() is not None)
        eq_(session.scalar(functions.wkt(functions.start_point(WKTSpatialElement('LINESTRING(0 1, 0 2)', geometry_type=LineString.name)))), 
            u'POINT (0.0 1.0)')
        
    def test_end_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.end_point)), 
            u'POINT (-88.5912422101911 43.187101955414)')
        ok_(session.query(Road).filter(functions.wkt(Road.road_geom.end_point) ==
                                                    'POINT (-88.3655256496815 43.1402866687898)').first() is not None)
        eq_(session.scalar(functions.wkt(functions.end_point(WKTSpatialElement('LINESTRING(0 1, 0 2)', geometry_type=LineString.name)))), 
            u'POINT (0.0 2.0)')
        
    def test_transform(self):
        spot = session.query(Spot).get(1)
        # note that we have to cast to 'ST_POINT', because 'functions.x' only works for Points in Oracle
        assert_almost_equal(float(session.scalar(functions.x(func.ST_POINT(spot.spot_location.transform(2249))))), -3890517.61088792)
        assert_almost_equal(float(session.scalar(functions.y(func.ST_POINT(spot.spot_location.transform(2249))))), 3627658.6749871401)
        ok_(session.query(Spot).filter(functions.wkt(Spot.spot_location.transform(2249)) == 'POINT (-3890517.61088792 3627658.67498714)').first() is not None)
        eq_(session.scalar(functions.wkt(functions.transform(WKTSpatialElement('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))', 2249), 4326))), 
            u'POLYGON ((-71.1776848522252 42.3902896503503, -71.1776843766327 42.390382946861, -71.1775844305466 42.3903826668518, -71.1775825927231 42.3902893638588, -71.1776848522252 42.3902896503503))')

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
        assert_almost_equal(float(session.scalar(r1.road_geom.distance(r2.road_geom))), 29371.776054049602)
        eq_(session.scalar(r1.road_geom.distance(r3.road_geom)), 0.0)
        ok_(session.query(Spot).filter(Spot.spot_location.distance(
                                                            WKTSpatialElement('POINT(-88.5945861592357 42.9480095987261)'), 
                                                            tolerance, auto_diminfo=False) < 10).first() is not None)
        eq_(session.scalar(functions.distance('POINT(-88.5945861592357 42.9480095987261)', 'POINT(-88.5945861592357 42.9480095987261)', tolerance)), 
            0)

    def test_within_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        roads_within_distance = session.query(Road).filter(
            Road.road_geom.within_distance(0.20, r1.road_geom)).all()
        ok_(r2 in roads_within_distance)
        ok_(r3 not in roads_within_distance)

    def test_disjoint(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        disjoint_roads = session.query(Road).filter(Road.road_geom.disjoint(r1.road_geom)).all()
        ok_(r2 not in disjoint_roads)
        ok_(r3 in disjoint_roads)
        eq_(session.scalar(functions.disjoint(
                                WKTSpatialElement('POINT(0 0)'), 
                                WKTSpatialElement('LINESTRING ( 2 0, 0 2 )'))), True)

    def test_intersects(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        intersecting_roads = session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()
        ok_(r2 in intersecting_roads)
        ok_(r3 not in intersecting_roads)
        eq_(session.scalar(functions.intersects(
                                WKTSpatialElement('POINT(0 0)'), 
                                WKTSpatialElement('LINESTRING ( 2 0, 0 2 )'))), False)

    def test_touches(self):
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        l2 = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        r = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        touching_lakes = session.query(Lake).filter(Lake.lake_geom.touches(r.road_geom)).all()
        ok_(session.scalar(l2.lake_geom.touches(r.road_geom)))
        ok_(l1 not in touching_lakes)
        ok_(l2 in touching_lakes)
        eq_(session.scalar(functions.touches(
                                WKTSpatialElement('POINT(0 0)'), 
                                WKTSpatialElement('LINESTRING ( 2 0, 0 2 )'))), False)

    def test_crosses(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Paul St').one()
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        crossing_roads = session.query(Road).filter(Road.road_geom.crosses(l.lake_geom)).all()
        ok_(not session.scalar(r1.road_geom.crosses(l.lake_geom)))
        ok_(session.scalar(r2.road_geom.crosses(l.lake_geom)))
        ok_(r1 not in crossing_roads)
        ok_(r2 in crossing_roads)
        # note that Oracle returns False instead of True (like the other databases do..)
        eq_(session.scalar(functions.crosses('LINESTRING(0 1, 2 1)', 'LINESTRING (0 0, 1 2, 0 2)')), False)

    def test_within(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        p1 = session.query(Spot).get(2)
        p2 = session.query(Spot).get(3)
        spots_within = session.query(Spot).filter(Spot.spot_location.within(l.lake_geom)).all()
        ok_(session.scalar(p1.spot_location.within(l.lake_geom)))
        ok_(not session.scalar(p2.spot_location.within(l.lake_geom)))
        ok_(p1 in spots_within)
        ok_(p2 not in spots_within)
        eq_(session.scalar(functions.within('LINESTRING(0 1, 2 1)', 'POLYGON((-1 -1, 3 -1, 3 2, -1 2, -1 -1))')), True)

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
        p1 = session.query(Spot).get(2)
        p2 = session.query(Spot).get(3)
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

    def test_sdo_filter(self):
        ok_(session.query(Spot).filter(Spot.spot_location.sdo_filter('POINT(-88.9055734203822 43.0048567324841)')).all())
    
    def test_sdo_nn(self):
        ok_(session.query(Spot).filter(Spot.spot_location.sdo_nn('POINT(-88.9055734203822 43.0048567324841)'
                                                                     'distance=2')).first())
    
    def test_sdo_nn_distance(self):
        ok_(session.query(Spot, oracle_functions.sdo_nn_distance(text('42'))).
                        filter(Spot.spot_location.sdo_nn('POINT(-88.9055734203822 43.0048567324841)'
                                                                     'distance=2', text('42'))).first())
    
    def test_sdo_relate(self):
        ok_(session.query(Spot).
                        filter(Spot.spot_location.sdo_relate('POINT(-88.9055734203822 43.0048567324841)',
                                                                     'mask=equal')).first())
    
    def test_sdo_within_distance(self):
        ok_(session.query(Spot).
                        filter(Spot.spot_location.sdo_within_distance('POINT(-88.9055734203822 43.0048567324841)',
                                                                     'distance=2')).first())
    
    def test_sdo_anyinteract(self):
        ok_(session.query(Spot).filter(
                Spot.spot_location.sdo_anyinteract('POINT(-88.9055734203822 43.0048567324841)')).first())
    
    def test_sdo_contains(self):
        ok_(session.query(Lake).filter(
                Lake.lake_geom.sdo_contains('POINT(-88.9055734203822 43.0048567324841)')).first())

    def test_sdo_coveredby(self):
        test_lake = Lake(lake_name='Test Lake', lake_geom=WKTSpatialElement('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 4326))
        session.add(test_lake)
        session.commit()
        
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        covering_lakes = session.query(Lake).filter(Lake.lake_geom.sdo_coveredby('POLYGON((0 0, 20 0, 20 20, 0 20, 0 0))')).all()
        ok_(test_lake in covering_lakes)
        ok_(l1 not in covering_lakes)
    
    def test_sdo_covers(self):
        # see http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_intro.htm#i880253
        # Covers/CoveredBy work different in Oracle
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        l1 = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        covering_lakes = session.query(Lake).filter(Lake.lake_geom.sdo_covers('LINESTRING(-88.7968950764331 43.2305732929936, -88.7935511273885 43.1553344394904)')).all()
        ok_(l in covering_lakes)
        ok_(l1 not in covering_lakes)
    
    def test_sdo_equal(self):
        ok_(session.query(Spot).filter(
                        Spot.spot_location.sdo_equal('POINT(-88.9055734203822 43.0048567324841)')).first())
    
    def test_sdo_inside(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        ok_(session.query(Spot).filter(
                        Spot.spot_location.sdo_inside(l.lake_geom)).first())
    
    def test_sdo_on(self):
        test_road = Road(road_name='Test Road', road_geom=WKTSpatialElement('LINESTRING(0 0, 10 0)', 4326))
        session.add(test_road)
        session.commit()
        
        ok_(session.query(Road).filter(
                        Road.road_geom.sdo_on('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')).first())
    
    def test_sdo_overlapbydisjoint(self):
        r2 = session.query(Road).filter(Road.road_name=='Paul St').one()
        ok_(session.query(Lake).filter(
                        Lake.lake_geom.sdo_overlapbdydisjoint(r2.road_geom)).first())
    
    def test_sdo_overlapbyintersect(self):
        test_lake = Lake(lake_name='Test Lake Overlap', lake_geom=WKTSpatialElement('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 4326))
        session.add(test_lake)
        session.commit()
        ok_(session.query(Lake).filter(Lake.lake_geom.sdo_overlapbdyintersect(
                    'POLYGON((-10 1, 5 1, 5 8, -10 8, -10 1))')).first())
    
    def test_sdo_overlaps(self):
        r2 = session.query(Road).filter(Road.road_name=='Paul St').one()
        ok_(session.query(Lake).filter(
                        Lake.lake_geom.sdo_overlaps(r2.road_geom)).first())
    
    def test_sdo_touch(self):
        test_lake = Lake(lake_name='Test Lake Touch', lake_geom=WKTSpatialElement('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 4326))
        session.add(test_lake)
        session.commit()
        ok_(session.query(Lake).filter(Lake.lake_geom.sdo_touch(
                    'POLYGON((-10 1, 0 1, 0 8, -10 8, -10 1))')).first())
        

    def test_intersection(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        s = session.query(Spot).get(4)
        eq_(session.scalar(functions.wkt(l.lake_geom.intersection(s.spot_location, tolerance, auto_diminfo=False))), None)
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        r = session.query(Road).filter(Road.road_name=='Paul St').one()
        eq_(session.scalar(functions.wkt(l.lake_geom.intersection(r.road_geom))), u'LINESTRING (-88.1430664921296 42.6255530821991, -88.114084510211 42.6230683849207)')
        ok_(session.query(Lake).filter(functions.equals(Lake.lake_geom.intersection(r.road_geom), WKTSpatialElement('LINESTRING(-88.1430664921296 42.6255530821991, -88.114084510211 42.6230683849207)'))).first() is not None)
 
    def test_sdo_geom_sdo_area(self):
        ok_(self.test_area, 'same as functions.area')
    
    def test_sdo_geom_sdo_buffer(self):
        ok_(self.test_buffer, 'same as functions.buffer')
    
    def test_sdo_geom_sdo_centroid(self):
        ok_(self.test_centroid, 'same as functions.centroid')
    
    def test_sdo_geom_sdo_concavehull(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.sdo_geom_sdo_concavehull(tolerance))), 
            u'POLYGON ((-88.5477708728477 42.6988853969538, -88.5912422100661 43.1871019533972, -88.6029460317718 43.0884554581896, -88.6096339299412 42.9697452675198, -88.5477708728477 42.6988853969538))')
        
    def test_sdo_geom_sdo_concavehull_boundary(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.sdo_geom_sdo_concavehull_boundary(tolerance))), 
            u'POLYGON ((-88.5477708728477 42.6988853969538, -88.5912422100661 43.1871019533972, -88.6029460317718 43.0884554581896, -88.6096339299412 42.9697452675198, -88.5477708728477 42.6988853969538))')
        
    def test_sdo_geom_sdo_convexhull(self):
        ok_(self.test_convex_hull, 'same as functions.convex_hull')
    
    def test_sdo_geom_sdo_difference(self):
        eq_(session.scalar(functions.wkt(oracle_functions.sdo_geom_sdo_difference(
                                    'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))',
                                    'POLYGON((-10 1, 5 1, 5 8, -10 8, -10 1))',                                                 
                                    tolerance))), 
            u'POLYGON ((5.0 1.0, 5.0 8.0, 0.0 8.06054845190832, 0.0 1.00766739404158, 5.0 1.0))')
        
    
    def test_sdo_geom_sdo_distance(self):
        ok_(self.test_distance, 'same as functions.distance')
    
    def test_sdo_geom_sdo_intersection(self):
        ok_(self.test_intersection, 'same as functions.intersection')
    
    def test_sdo_geom_sdo_length(self):
        ok_(self.test_length, 'same as functions.length')
    
    def test_sdo_geom_sdo_mbr(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(functions.wkt(r.road_geom.sdo_geom_sdo_mbr)), 
            u'POLYGON ((-88.6096339299363 42.6988853949045, -88.5477708726115 42.6988853949045, -88.5477708726115 43.187101955414, -88.6096339299363 43.187101955414, -88.6096339299363 42.6988853949045))')
    
    def test_sdo_geom_sdo_pointonsurface(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        eq_(session.scalar(functions.wkt(l.lake_geom.sdo_geom_sdo_pointonsurface)), u'POINT (-89.0694267515924 43.1335987261147)')
    
    def test_sdo_geom_sdo_union(self):
        eq_(session.scalar(functions.wkt(oracle_functions.sdo_geom_sdo_union(
                                    'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))',
                                    'POLYGON((-10 1, 5 1, 5 8, -10 8, -10 1))',                                                 
                                    tolerance))), 
            u'POLYGON ((0.0 8.06054845190832, -10.0 8.0, -10.0 1.0, 0.0 1.00766739404158, 0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 8.06054845190832))')
    
    def test_sdo_geom_sdo_xor(self):
        eq_(session.scalar(functions.wkt(oracle_functions.sdo_geom_sdo_xor(
                                    'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))',
                                    'POLYGON((-10 1, 5 1, 5 8, -10 8, -10 1))',                                                 
                                    tolerance))), 
            u'MULTIPOLYGON (((-10.0 8.0, -10.0 1.0, 0.0 1.00766739404158, 0.0 8.06054845190832, -10.0 8.0)), ((10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 8.06054845190832, 5.0 8.0, 5.0 1.0, 0.0 1.00766739404158, 0.0 0.0, 10.0 0.0)))')
    
    def test_sdo_geom_sdo_within_distance(self):
        ok_(self.test_within_distance, 'same as functions.within_distance')

    def test_within_distance(self):
        """
        Because SDO_WITHIN_DISTANCE requires a spatial index for the geometry used
        as first parameter, we have to insert out test geometries into tables,
        unlike to the other databases.
        
        Note that Oracle uses meter as unit for the tolerance value for geodetic coordinate
        systems (like 4326)!
        """
        # test if SDO_functions._within_distance is called correctly
        eq_(session.query(Spot).filter(functions._within_distance(Spot.spot_location, 'POINT(0 0)', 0)).count(), 1)
        eq_(session.query(Spot).filter(functions._within_distance(Spot.spot_location, 'POINT(0 0)', 0.1)).count(), 1)
        eq_(session.query(Spot).filter(functions._within_distance(Spot.spot_location, 'POINT(9 9)', 100000)).count(), 0)
        
        eq_(session.query(Spot).filter(functions._within_distance(Spot.spot_location, 
                                                       'Polygon((-5 -5, 5 -5, 5 5, -5 5, -5 -5))', 0)).count(), 3)
        
        eq_(session.query(Spot).filter(functions._within_distance(Spot.spot_location, 
                                                       'Polygon((-10 -10, 10 -10, 10 10, -10 10, -10 -10))', 0)).count(), 4)
        
        eq_(session.query(Spot).filter(functions._within_distance(Spot.spot_location, 
                                                       'Polygon((-10 -10, 10 -10, 10 10, -10 10, -10 -10))', 200000)).count(), 5)

        # test if SDO_GEOM.functions._within_distance is called correctly
        eq_(session.scalar(select([text('1')], from_obj=['dual']).where(
                                                    functions._within_distance('POINT(0 0)', 'POINT(0 0)', 0, 
                                                                    {'tol' : 0.00000005}))), 1)
        eq_(session.scalar(select([text('1')], from_obj=['dual']).where(
                                                    functions._within_distance('POINT(0 0)', 'POINT(0 0)', 0, 
                                                                    {'dim1' : text(diminfo),
                                                                     'dim2' : text(diminfo)}))), 1)

#todo: insert None
    @raises(IntegrityError)
    def test_constraint_nullable(self):
        spot_null = Spot(spot_height=None, spot_location=ORACLE_NULL_GEOMETRY)
#        spot_null = Spot(spot_height=None, spot_location=None)
        session.add(spot_null)
        session.commit();
        session.delete(spot_null)
        session.commit();
        ok_(True)
        road_null = Road(road_name='Jeff Rd', road_geom=ORACLE_NULL_GEOMETRY)
#        road_null = Road(road_name='Jeff Rd', road_geom=None)
        session.add(road_null)
        session.commit();
