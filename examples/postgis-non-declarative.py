from sqlalchemy import *
from sqlalchemy.orm import *

from geoalchemy import *
from geoalchemy.postgis import PGComparator

engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
session = sessionmaker(bind=engine)()

# define table
spots_table = Table('spots', metadata,
                    Column('spot_id', Integer, primary_key=True),
                    Column('spot_height', Numeric),
                    GeometryExtensionColumn('spot_location', Geometry(2)))

# define class
class Spot(object):
    def __init__(self, spot_id=None, spot_height=None, spot_location=None):
        self.spot_id = spot_id
        self.spot_height = spot_height
        self.spot_location = spot_location

# set up the mapping between table and class       
mapper(Spot, spots_table, properties={
            'spot_location': GeometryColumn(spots_table.c.spot_location, 
                                            comparator=PGComparator)}) 

# enable the DDL extension, which allows CREATE/DROP operations
# to work correctly.  This is not needed if working with externally
# defined tables.    
GeometryDDL(spots_table)


metadata.drop_all()
metadata.create_all()

# Add objects.  We can use strings...
session.add_all([
            Spot(spot_height=420.40, spot_location='POINT(-88.5945861592357 42.9480095987261)'),
            Spot(spot_height=102.34, spot_location='POINT(-88.9055734203822 43.0048567324841)'),
            Spot(spot_height=388.62, spot_location='POINT(-89.201512910828 43.1051752038217)'),
            Spot(spot_height=454.66, spot_location='POINT(-88.3304141847134 42.6269904904459)'),
])

session.commit()

s = session.query(Spot).get(1)
print session.scalar(s.spot_location.wkt)

s = session.query(Spot).filter(Spot.spot_location == 'POINT(-88.5945861592357 42.9480095987261)').first()
print session.scalar(s.spot_location.wkt)
print session.scalar(functions.geometry_type(s.spot_location))