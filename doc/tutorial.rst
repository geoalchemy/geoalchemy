GeoAlchemy Tutorial
===================

This is a short tutorial to illustrate the use of GeoAlchemy and to get
you started quickly. In this tutorial we will create a simple model using
three vector layers in PostGIS. The same tutorial will be applicable for
MySQL. Minor changes are required for spatialte as the spatialite extension
needs to be loaded. Spatialite specific details are given `here
<#notes-for-spatialite>`_.

Setting up PostGIS
------------------

This tutorial requires a working postgis installation. The PostGIS
Documentation has extensive `installation instructions
<http://postgis.refractions.net/docs/ch02.html#PGInstall>`_. Create a
spatially enabled database called `gis` as per instructions given
`here <http://postgis.refractions.net/docs/ch02.html#id2532099>`_. Also
create a new user and grant it permissions on this database.

Initializing SQLAlchemy
-----------------------

First of all we need some code to initialize sqlalchemy and to create an
engine with a database specific dialect. We use the standard sqlalchemy
dburi format to create the engine. For more details refer to the sqlalchemy
documentation. We use this engine to create a session. A session could be
either bound or unbound. In this example we will create an bound session.

.. code-block:: python

    from sqlalchemy import *
    from sqlalchemy.orm import *

    engine = create_engine('postgres://gis:password@localhost/gis', echo=True)
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
creating the objects in the database.

.. code-block:: python

    from sqlalchemy.ext.declarative import declarative_base
    from datetime import datetime
    from geoalchemy import *

    metadata = MetaData(engine)
    Base = declarative_base(metadata=metadata)

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

    GeometryDDL(Spot.__table__)
    GeometryDDL(Road.__table__)
    GeometryDDL(Lake.__table__)

In the above model definition we have defined an `id` field for each class
which is also the primary key in the database. We have defined a set of
standard attributes using datatyes availabe under `sqlalchemy.types`. We
have also created a `geometry attribute` for each class using `GeometryColumn`
and Point, LineString and Polygon datatypes of GeoAlchemy. Here we pass the
dimension parameter to `GeometryColumn`. We leave out the `srid` parameter which
defaults to `4326`. This means that our geometry values will be in Geographic
Latitude and Longitude coordinate system.

Finally we have used `GeometryDDL`, a DDL Extension for geometry data types
that support special DDLs required for creation of geometry fields in the
database.

Creating Database Tables
------------------------

Now we use the metadata object to create our tables. On subsequent use we will
also first drop the tables so that the database is emptied before creating tables.

.. code-block:: python

    metadata.drop_all()   # comment this on first occassion
    metadata.create_all()

Adding GIS Features
-------------------

Addind GIS features is now as simple as instantiating the model classes and addind them to the SQLAlchemy session object that we created earlier. Geoalchemy enables creation of spatial attributes specified using the Well Known Text (WKT) format using geoalchemy `WKTSpatialElement` class.

.. code-block:: python

    wkt = "POINT(-81.40 38.08)"
    spot1 = Spot(name="Gas Station", height=240.8, geom=WKTSpatialElement(wkt))
    wkt = "POINT(-81.42 37.65)"
    spot2 = Spot(name="Restaurant", height=233.6, geom=WKTSpatialElement(wkt)
    
    wkt = "LINESTRING(-80.3 38.2, -81.03 38.04, -81.2 37.89)"
    road1 = Road(name="Peter St", width=6, geom=WKTSpatialElement(wkt))
    wkt = "LINESTRING(-79.8 38.5, -80.03 38.2, -80.2 37.89)"
    road2 = Road(name="George Ave", width=8, geom=WKTSpatialElement(wkt))
    
    wkt = "POLYGON((-81.3 37.2, -80.63 38.04, -80.02 37.49, -81.3 37.2))"
    lake1 = Lake(name="Lake Juliet", depth=36, geom=WKTSpatialElement(wkt))
    wkt = "POLYGON((-79.8 38.5, -80.03 38.2, -80.02 37.89, -79.92 37.75, -79.8 38.5))"
    lake2 = Lake(name="Lake Blue", depth=58, geom=WKTSpatialElement(wkt))
    
    session.add_all([spot1, spot2, road1, road2, lake1, lake2])
    session.commit()

Scripts for creating sample gis objects as shown above are available in the examples directory. You could run those scripts to create the database tables and the gis objects. Running them with -i option to the interpreter will drop you at the interactive interpreter promt. You can then follow the rest of the tutorial on the interpreter.

.. code-block:: python

    $ python -i examples/tutorial.py
    >>>

Performing Spatial Queries
--------------------------

The GeoAlchemy project intends to cover most of the spatial operations and
spatial relations supported by the underlying spatial database. Some of these
are shown below and the rest are documented in the reference docs.

Functions to obtain geometry value in different formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> s = session.query(Spot).get(1)
    >>> session.scalar(s.geom.wkt)
    'POINT(-81.42 37.65)'
    >>> session.scalar(s.geom.gml)
    '<gml:Point srsName="EPSG:4326"><gml:coordinates>-81.42,37.65</gml:coordinates></gml:Point>'
    >>> session.scalar(s.geom.kml)
    '<Point><coordinates>-81.42,37.65</coordinates></Point>'
    >>> import binascii
    >>> binascii.hexlify(session.scalar(s.geom.wkb))
    '01010000007b14ae47e15a54c03333333333d34240'

Functions to obtain the geometry type, coordinates, etc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


.. code-block:: python

    >>> s = session.query(Spot).filter(Spot.height > 240).first()
    >>>
    >>> session.scalar(s.geom.geometry_type)
    'ST_Point'
    >>> session.scalar(s.geom.x)
    -81.420000000000002
    >>> session.scalar(s.geom.y)
    37.649999999999999
    >>> s.geom.coords(session)
    [-81.420000000000002, 37.649999999999999]

Spatial operations that return new geometries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> r = session.query(Road).first()
    >>> l = session.query(Lake).first()
    >>>
    >>> buffer_geom = WKBSpatialElement(session.scalar(r.geom.buffer(10.0)))
    >>> session.scalar(buffer_geom.wkt)
    'POLYGON((-77.4495270615657 28.6622373442108,-77.9569183543725 28.4304851371862,-79.8646930595254 27.9795532202266, ........ ,28.6622373442108))'
    >>> envelope_geom = WKBSpatialElement(session.scalar(r.geom.envelope))
    >>> session.scalar(envelope_geom.wkt)
    'POLYGON((-81.2000045776367 37.8899993896484,-81.2000045776367 38.2000007629395,-80.2999954223633 38.2000007629395,-80.2999954223633 37.8899993896484,-81.2000045776367 37.8899993896484))'
    >>> cv_geom = WKBSpatialElement(session.scalar(r.geom.convex_hull))
    >>> session.scalar(cv_geom.wkt)
    'POLYGON((-81.2 37.89,-81.03 38.04,-80.3 38.2,-81.2 37.89))'

Spatial relations for filtering features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> r = session.query(Road).first()
    >>> l = session.query(Lake).first()

    >>> session.query(Road).filter(Road.geom.intersects(r.geom)).count()
    1L
    >>> session.query(Lake).filter(Lake.geom.touches(r.geom)).count()
    0L
    >>> session.query(Spot).filter(Spot.geom.covered_by(l.geom)).count()
    0L


Notes for Spatialite
--------------------

Although Python2.5 and its higher versions include sqlite support, while using
spatialite in python we have to use the db-api module provided by pysqlite2.
So we have to install pysqlite2 separately. Also, by default the pysqlite2
disables extension loading. In order to enable extension loading, we have
to build it ourselves. Download the pysqlite tarball, open the file setup.cfg
and comment out the line that reads:

.. code-block:: python

    define=SQLITE_OMIT_LOAD_EXTENSION

Now save the file and then build and install pysqlite2:

.. code-block:: bash

    $ python setup.py install

Now, we are ready to use spatialte in our code. While importing pysqlite
in our code we must ensure that we are importing from the newly installed
pysqlite2 and not from the pysqlite library included in python. Also pass
the imported module as a parameter to sqlalchemy create_engine function
so that sqlalchemy uses this module instead of the default module:
to be used:

.. code-block:: python

    from pysqlite2 import dbapi2 as sqlite

    engine = create_engine('sqlite:////tmp/devdata.db', module=sqlite, echo=True)

Enable sqlite extension loading and load the spatialite extension:

.. code-block:: python

    connection = engine.raw_connection().connection
    connection.enable_load_extension(True)
    metadata = MetaData(engine)
    session = sessionmaker(bind=engine)()
    session.execute("select load_extension('/usr/local/lib/libspatialite.so')")

When using for the database for the first time we have to initialize the
database. Details are given in `spatialite documentation
<http://www.gaia-gis.it/spatialite/spatialite-tutorial-2.3.1.html#t2>`_.

.. code-block:: sql

    sqlite3> SELECT InitSpatialMetaData();
    sqlite3> INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) VALUES (4326, 'epsg', 4326, 'WGS 84', '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs');

