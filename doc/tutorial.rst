GeoAlchemy Tutorial
===================

This is a short tutorial to illustrate the use of GeoAlchemy and to get
you started quickly. In this tutorial we will create a simple model using
three vector layers in PostGIS. The same tutorial will be applicable for
MySQL and Spatialte with minor changes.

Initializing SQLAlchemy
-----------------------

First of all we need some code to initialize sqlalchemy and to create an
engine with a database specific dialect. We use the standard sqlalchemy
dburi format to create the engine. For more details refer to the sqlalchemy
documentation. We use this enginr to create a session. A session could be
either bound or unbound. In this example we will create an bound session.::

    from sqlalchemy import *
    from sqlalchemy.orm import *

    engine = create_engine(postgres://gis:password@localhost/gisdb, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

Model Definition
----------------

Let us assume we have to model the following GIS layers in the database::

    * spots - represented as point geometry
    * roads - represented as line geometry, and
    * lakes - represented as polygon geometry

In SQLAlchemy, models can be defined either by declaraing the model classes
and database tables seperately and then mapping the classes to the tables
using mapper configuration, or they can be defined declaratively using the
new declarative extenstion. In this example we use the SQLAlchemy deaclarative
extension to define the model. We also create a metadata object that holds
the schema information of all database objects and will thus be useful for
creating the objects in the database::

    from sqlalchemy.ext.declarative import declarative_base
    from datetime import datetime
    from geoalchemy import 

    metadata = Metadata(engine)
    Base = declarative_base(metadata)

    class Spot(Base):
        __tablename__ = 'spots'
        id = Column(Integer, primary_key=True)
        name = Column(Unicode, nullable=False)
        height = Column(Integer)
        created = Column(Datetime, default=datetime.now())
        geom = GeometryColumn(Point(2))

    class Road(Base):
        __tablename__ = 'roads'
        id = Column(Integer, primary_key=True)
        name = Column(Unicode, nullable=False)
        width = Column(Integer)
        created = Column(Datetime, default=datetime.now())
        geom = GeometryColumn(LineString(2))

    class Lake(Base):
        __tablename__ = 'lakes'
        id = Column(Integer, primary_key=True)
        name = Column(Unicode, nullable=False)
        depth = Column(Integer)
        created = Column(Datetime, default=datetime.now())
        geom = GeometryColumn(Polygon(2))

    GeometryDDL(Spot.__table__)
    GeometryDDL(Road.__table__)
    GeometryDDL(Lake.__table__)

In the above model definition we have defined an `id` field for each class
which is also the primary key in the database. We have defined a set of
standards attributes using datatyes availabe under `sqlalchemy.types`. We
have also created a `geometry attribute` for each class using `GeometryColumn`
and Point, LineString and Polygon datatypes of GeoAlchemy.

Finally we have used `GeometryDDL`, a DDL Extension for geometry data types
that support special DDLs required for creation of geometry fields in the
database.

Creating Database Tables
------------------------

Now we use the metadata object to create our tables. We will also first drop
the tables so that the database is emptied before creating the tables.::

    metadata.drop_all()
    metadata.create_all()


