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
    __table_args__ = {'autoload': True}
    geom = GeometryColumn(Point(2))

class Road(Base):
    __tablename__ = 'roads'
    __table_args__ = {'autoload': True}
    geom = GeometryColumn(LineString(2))

class Lake(Base):
    __tablename__ = 'lakes'
    __table_args__ = {'autoload': True}
    geom = GeometryColumn(Polygon(2))


