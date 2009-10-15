from unittest import TestCase
from binascii import b2a_hex
from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        Numeric, func, literal, select)
from sqlalchemy.orm import sessionmaker, column_property
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy import (Geometry, GeometryCollection, GeometryColumn,
        GeometryDDL, WKTSpatialElement, WKBSpatialElement)
from nose.tools import eq_, ok_

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
GeometryDDL(Spot.__table__)
GeometryDDL(Shape.__table__)

class TestGeometry(TestCase):

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

    def test_wkt(self):
        l = session.query(Lake).get(1)
        assert session.scalar(self.r.road_geom.wkt) == 'LINESTRING(-88.6748409363057 43.1035032292994,-88.6464173694267 42.9981688343949,-88.607961955414 42.9680732929936,-88.5160033566879 42.9363057770701,-88.4390925286624 43.0031847579618)'
        eq_(session.scalar(l.lake_geom.wkt),'POLYGON((-88.7968950764331 43.2305732929936,-88.7935511273885 43.1553344394904,-88.716640299363 43.1570064140127,-88.7250001719745 43.2339172420382,-88.7968950764331 43.2305732929936))')

    def test_coords(self):
        eq_(self.r.road_geom.coords(session), [[-88.6748409363057,43.1035032292994],[-88.6464173694267,42.9981688343949],[-88.607961955414,42.9680732929936],[-88.5160033566879,42.9363057770701],[-88.4390925286624,43.0031847579618]])
        l = session.query(Lake).filter(Lake.lake_name=="Lake Deep").one()
        eq_(l.lake_geom.coords(session), [[[-88.9122611464968,43.038296178344],[-88.9222929936306,43.0399681528663],[-88.9323248407643,43.0282643312102],[-88.9206210191083,43.0182324840764],[-88.9105891719745,43.0165605095542],[-88.9005573248408,43.0232484076433],[-88.9072452229299,43.0282643312102],[-88.9122611464968,43.038296178344]]])
        s = session.query(Spot).filter(Spot.spot_height==102.34).one()
        eq_(s.spot_location.coords(session), [-88.905573420382197, 43.0048567324841])

    def test_wkb(self):
        eq_(b2a_hex(session.scalar(self.r.road_geom.wkb)).upper(), '010200000005000000D7DB0998302B56C0876F04983F8D45404250F5E65E2956C068CE11FFC37F4540C8ED42D9E82656C0EFC45ED3E97B45407366F132062156C036C921DED877454078A18C171A1C56C053A5AF5B68804540')

    def test_svg(self):
        eq_(session.scalar(self.r.road_geom.svg), 'M -88.674840936305699 -43.103503229299399 -88.6464173694267 -42.998168834394903 -88.607961955413998 -42.968073292993601 -88.516003356687904 -42.936305777070103 -88.4390925286624 -43.003184757961797')

    def test_gml(self):
        eq_(session.scalar(self.r.road_geom.gml), '<gml:LineString srsName="EPSG:4326"><gml:coordinates>-88.6748409363057,43.1035032292994 -88.6464173694267,42.9981688343949 -88.607961955414,42.9680732929936 -88.5160033566879,42.9363057770701 -88.4390925286624,43.0031847579618</gml:coordinates></gml:LineString>')

    def test_dimension(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        eq_(session.scalar(r.road_geom.dimension), 1)
        eq_(session.scalar(l.lake_geom.dimension), 2)

    def test_geometry_type(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.geometry_type) == 'ST_LineString'
        assert session.scalar(l.lake_geom.geometry_type) == 'ST_Polygon'

    def test_is_empty(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert not session.scalar(r.road_geom.is_empty)
        assert not session.scalar(l.lake_geom.is_empty)

    def test_is_simple(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert session.scalar(r.road_geom.is_simple)
        assert session.scalar(l.lake_geom.is_simple)

    def test_is_closed(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='My Lake').one()
        assert not session.scalar(r.road_geom.is_closed)
        assert session.scalar(l.lake_geom.is_closed)

    def test_is_ring(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        assert not session.scalar(r.road_geom.is_ring)

    def test_persistent(self):
        assert b2a_hex(session.scalar(self.r.road_geom.wkb)).upper() == '010200000005000000D7DB0998302B56C0876F04983F8D45404250F5E65E2956C068CE11FFC37F4540C8ED42D9E82656C0EFC45ED3E97B45407366F132062156C036C921DED877454078A18C171A1C56C053A5AF5B68804540'

    def test_eq(self):
        r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(-88.5477708726115 42.6988853949045,-88.6096339299363 42.9697452675159,-88.6029460318471 43.0884554585987,-88.5912422101911 43.187101955414)').one()
        r3 = session.query(Road).filter(Road.road_geom == r1.road_geom).one()
        ok_(r1 is r2 is r3)

    def test_length(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.length), 0.496071476676014)

    def test_area(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        eq_(session.scalar(l.lake_geom.area), 0.10475991566721)

    def test_x(self):
        s = session.query(Spot).filter(Spot.spot_height==420.40).one()
        eq_(session.scalar(s.spot_location.x), -88.5945861592357)

    def test_y(self):
        s = session.query(Spot).filter(Spot.spot_height==420.40).one()
        eq_(session.scalar(s.spot_location.y), 42.9480095987261)

    def test_centroid(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        eq_(session.scalar(r.road_geom.centroid), '0101000020E6100000AF1BBA22B22556C0476A43EB8B784540')
        eq_(session.scalar(l.lake_geom.centroid), '0101000020E610000057FA7719F93A56C0751FE87F73824540')

    def test_boundary(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.boundary), '0104000020E610000002000000010100000056E48FAD0E2356C029629D13755945400101000000463291E9D62556C0A9C2F5F4F2974540')

    def test_buffer(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        (session.scalar(r.road_geom.buffer(length=10.0, num_segs=8)), '0103000020E61000000100000026000000C8C5532DF39658C0C5FF3AED1E5F44403E9604D900A658C005FBFD3A20C44540A1B1F04593A558C08C2E402052D345405941958F21A258C0313C8506212246404D7132CE61A158C016145079C12E464058A282B6758658C075D18E9ED52347402D796C0D265458C0D694C711B309484072F437C8610C58C0DC3F2E6D84D74840E35C1FF0EAB157C0CC3D37DE60854940CD355F803B4857C08294AAF5990C4A406FAAA13263D356C0851BC962FD674A40FE4F3E8ADF5756C06576DB1208944A408F0964A76FDA55C0B64038BE088F4A40E1AAC494E55F55C0CEA2D88E30594A40304928DBF6EC54C07850AC3C91F44940AEF3742D0E8654C0034743B208654940B574F0F41F2F54C0E61712021BB048405AC9366983EB53C0BAC99122BCDC4740D885A7B2D1BD53C0FEB216870BF3464068589E5ACCA753C01CFE4F3604FC454077E6A37247A853C08DD49353BDCB45407E38C4CA4FA353C066435C778A7D45403C322F9E15AC53C02B50932E3B83444094343AEA1ACD53C0DA41DD70279143408183E5D31A0554C0E7B146C89CB042403AB1646FEE5154C0BC468D3B3CEA4140C8D60BECA1B054C03AFBDC69A545414035D4F89F911D55C090D1E18A2BC940409A08B9D58D9455C077A4053597794040B773B0FB031156C07A0A064EF759404054ED14A02B8E56C0F4FDDBF4826B40407DF4EF7E350757C0A402F28C8DAD4040763C3BD37A7757C07EA048608D1D41400CF29719ABDA57C0F0FB469734B74140CD177082F62C58C00B4795909B744240A2ACED71336B58C0E5416FF87A4E43402FC3DB9CFD9258C0B1AAEF63733C4440C8C5532DF39658C0C5FF3AED1E5F4440')

    def test_convex_hull(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.convex_hull), '0103000020E6100000010000000500000056E48FAD0E2356C029629D1375594540EFE6073E042756C03DB7E89C207C45405202F4AA962656C0C4EA2A82528B4540463291E9D62556C0A9C2F5F4F297454056E48FAD0E2356C029629D1375594540')

    def test_envelope(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.envelope), '0103000020E6100000010000000500000000000040042756C0000000007559454000000040042756C000000000F3974540000000A00E2356C000000000F3974540000000A00E2356C0000000007559454000000040042756C00000000075594540')

    def test_start_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.start_point), '0101000020E610000056E48FAD0E2356C029629D1375594540')

    def test_end_point(self):
        r = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        eq_(session.scalar(r.road_geom.end_point), '0101000020E6100000463291E9D62556C0A9C2F5F4F2974540')

    # Test Geometry Relationships

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
        eq_(session.scalar(r1.road_geom.distance(r2.road_geom)), 0.336997238682841)
        eq_(session.scalar(r1.road_geom.distance(r3.road_geom)), 0.0)

    def test_within_distance(self):
        r1 = session.query(Road).filter(Road.road_name=='Jeff Rd').one()
        r2 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()
        r3 = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        roads_within_distance = session.query(Road).filter(
            Road.road_geom.within_distance(r1.road_geom, 0.20)).all()
        ok_(r2 in roads_within_distance)
        ok_(r3 not in roads_within_distance)

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

    def test_covered_by(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        p1 = session.query(Spot).filter(Spot.spot_height==102.34).one()
        p2 = session.query(Spot).filter(Spot.spot_height==388.62).one()
        covered_spots = session.query(Spot).filter(Spot.spot_location.covered_by(l.lake_geom)).all()
        ok_(session.scalar(p1.spot_location.covered_by(l.lake_geom)))
        ok_(not session.scalar(p2.spot_location.covered_by(l.lake_geom)))
        ok_(p1 in covered_spots)
        ok_(p2 not in covered_spots)

    def test_intersection(self):
        l = session.query(Lake).filter(Lake.lake_name=='Lake Blue').one()
        r = session.query(Road).filter(Road.road_name=='Geordie Rd').one()
        s = session.query(Spot).filter(Spot.spot_height==454.66).one()
        eq_(session.scalar(WKBSpatialElement(session.scalar(l.lake_geom.intersection(s.spot_location))).wkt), 'GEOMETRYCOLLECTION EMPTY')
        eq_(session.scalar(WKBSpatialElement(session.scalar(l.lake_geom.intersection(r.road_geom))).wkt), 'POINT(-89.0710987261147 43.243949044586)')
        l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
        r = session.query(Road).filter(Road.road_name=='Paul St').one()
        eq_(session.scalar(WKBSpatialElement(session.scalar(l.lake_geom.intersection(r.road_geom))).wkt), 'LINESTRING(-88.1430673666454 42.6255500261493,-88.1140839697546 42.6230657349872)')
