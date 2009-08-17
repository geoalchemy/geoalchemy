GeoAlchemy Tutorial
===================

This is a short tutorial to illustrate the use of GeoAlchemy and to get
you started quickly. In this tutorial we will create a simple model using
three vector layers in PostGIS. The same tutorial will be applicable for
MySQL and Spatialte with minor changes.

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
standards attributes using datatyes availabe under `sqlalchemy.types`. We
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

Addind GIS features is now as simple as instantiating the model classes and addind them to the SQLAlchemy session object that we created earlier. Geoalchemy enables creation of spatial attributes Well Known Text (WKT) format using the `WKTSpatialElement class`.

.. code-block:: python

    wkt = "POINT(-81.40 38.08)"
    wkt = "POINT(-81.42 37.65)"
    spot1 = Spot(name="Gas Station", height=240.8, geom=WKTSpatialElement(wkt))
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


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
    'POLYGON((-77.4495270615657 28.6622373442108,-77.9569183543725 28.4304851371862,-79.8646930595254 27.9795532202266,-81.8237828615519 27.9094742151711,-83.7589010380305 28.2229412188832,-85.5956820826657 28.9079078702047,-87.2635395299911 29.9380512848588,-88.698378553651 31.2737836291316,-89.8450590946332 32.86377345812,-90.6595148628138 34.6469183543725,-91.1104467797734 36.5546930595254,-91.1805257848289 38.5137828615519,-90.8670587811168 40.4489010380304,-90.1820921297954 42.2856820826657,-89.1519487151412 43.953539529991,-87.8162163708684 45.388378553651,-87.6462163708684 45.538378553651,-86.2901515097915 46.5447519713416,-84.7840829416022 47.3085954312167,-83.1709592140701 47.8081264141947,-82.4409592140701 47.9681264141947,-80.4941543554641 48.1981150266565,-78.5398882538607 48.0438816856514,-76.6532622595793 47.5113534919791,-74.9067783515483 46.6209952054773,-73.3675529269137 45.4070227957775,-72.0947375555367 43.916088541781,-71.1372458183362 42.2054882107431,-70.5318735858053 40.3409592140701,-70.3018849733435 38.3941543554641,-70.4561183143486 36.4398882538607,-70.9886465080209 34.5532622595794,-71.8790047945227 32.8067783515483,-73.0929772042225 31.2675529269137,-74.583911458219 29.9947375555367,-76.2945117892569 29.0372458183362,-77.4495270615657 28.6622373442108))'
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
    >>> session.query(Spot).filter(Spot.geom.contained_by(l.geom)).count()
    0L


