from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from geoalchemy import *
from geoalchemy.postgis import PGComparator


# Setup the database engine and session
engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
session = sessionmaker(bind=engine)()

# Setup the metadata and declarative extension
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)


# Define the model classes
class Spot(Base):
    __tablename__ = 'spots'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    height = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(Point(2), comparator=PGComparator)

class Road(Base):
    __tablename__ = 'roads'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    width = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(LineString(2), comparator=PGComparator)

class Lake(Base):
    __tablename__ = 'lakes'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    depth = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(Polygon(2), comparator=PGComparator)

# DDL Extensions for geometry specific DDL
GeometryDDL(Spot.__table__)
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)

# Drop and create database objects
metadata.drop_all()
metadata.create_all()

# Create a few gis objects in the database
wkt_spot1 = "POINT(-81.40 38.08)"
spot1 = Spot(name="Gas Station", height=240.8, geom=WKTSpatialElement(wkt_spot1))
wkt_spot2 = "POINT(-81.42 37.65)"
spot2 = Spot(name="Restaurant", height=233.6, geom=WKTSpatialElement(wkt_spot2))

wkt_road1 = "LINESTRING(-80.3 38.2, -81.03 38.04, -81.2 37.89)"
road1 = Road(name="Peter St", width=6.0, geom=WKTSpatialElement(wkt_road1))
wkt_road2 = "LINESTRING(-79.8 38.5, -80.03 38.2, -80.2 37.89)"
road2 = Road(name="George Ave", width=8.0, geom=WKTSpatialElement(wkt_road2))

wkt_lake1 = "POLYGON((-81.3 37.2, -80.63 38.04, -80.02 37.49, -81.3 37.2))"
lake1 = Lake(name="Lake Juliet", depth=36.0, geom=WKTSpatialElement(wkt_lake1))
wkt_lake2 = "POLYGON((-79.8 38.5, -80.03 38.2, -80.02 37.89, -79.92 37.75, -79.8 38.5))"
lake2 = Lake(name="Lake Blue", depth=58.0, geom=WKTSpatialElement(wkt_lake2))

session.add_all([spot1, spot2, road1, road2, lake1, lake2])
session.commit()

