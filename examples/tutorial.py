from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from geoalchemy import *

# Setup the database engine and session
engine = create_engine('postgres://gis:gis@localhost/gis', echo=True)
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
    geom = GeometryColumn(Point(2))

class Road(Base):
    __tablename__ = 'roads'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    width = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(LineString(2))

class Lake(Base):
    __tablename__ = 'lakes'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    depth = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(Polygon(2))

# DDL Extensions for geometry specific DDL
GeometryDDL(Spot.__table__)
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)

# Drop and create database objects
metadata.drop_all()
metadata.create_all()

# Create a few gis objects in the database
wkt = "POINT(-81.40 38.08)"
wkt = "POINT(-81.42 37.65)"
spot1 = Spot(name="Gas Station", height=240.8, geom=WKTSpatialElement(wkt))
spot2 = Spot(name="Restaurant", height=233.6, geom=WKTSpatialElement(wkt))

wkt = "LINESTRING(-80.3 38.2, -81.03 38.04, -81.2 37.89)"
road1 = Road(name="Peter St", width=6.0, geom=WKTSpatialElement(wkt))
wkt = "LINESTRING(-79.8 38.5, -80.03 38.2, -80.2 37.89)"
road2 = Road(name="George Ave", width=8.0, geom=WKTSpatialElement(wkt))

wkt = "POLYGON((-81.3 37.2, -80.63 38.04, -80.02 37.49, -81.3 37.2))"
lake1 = Lake(name="Lake Juliet", depth=36.0, geom=WKTSpatialElement(wkt))
wkt = "POLYGON((-79.8 38.5, -80.03 38.2, -80.02 37.89, -79.92 37.75, -79.8 38.5))"
lake2 = Lake(name="Lake Blue", depth=58.0, geom=WKTSpatialElement(wkt))

session.add_all([spot1, spot2, road1, road2, lake1, lake2])
session.commit()

