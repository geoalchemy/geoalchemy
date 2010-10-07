GeoAlchemy Tutorial
===================

This is a short tutorial to illustrate the use of GeoAlchemy and to get
you started quickly. In this tutorial we will create a simple model using
three vector layers in PostGIS. The same tutorial will be applicable for
MySQL. Minor changes are required for spatialte as the spatialite extension
needs to be loaded. Spatialite specific details are given `here
<usagenotes.html#notes-for-spatialite>`_.

Setting up PostGIS
------------------

This tutorial requires a working PostGIS installation. The PostGIS
Documentation has extensive `installation instructions
<http://postgis.refractions.net/docs/ch02.html#PGInstall>`_. Create a
spatially enabled database called `gis` as per instructions given in the
`PostGIS documentation <http://postgis.refractions.net/docs/ch02.html#id2532099>`_. Also
create a new user and grant it permissions on this database.

On Ubuntu the following steps have to executed to create the database.

.. code-block:: bash

	sudo su postgres
	createdb -E UNICODE gis
	createlang plpgsql gis
	psql -d gis -f /usr/share/postgresql-8.3-postgis/lwpostgis.sql
	psql -d gis -f /usr/share/postgresql-8.3-postgis/spatial_ref_sys.sql
	
	#Create a new user (if a user named 'gis' does not exist already)
	createuser -P gis
	
	#Grant permissions to user 'gis' on the new database
	psql gis
	grant all on database gis to "gis";
	grant all on spatial_ref_sys to "gis";
	grant all on geometry_columns to "gis";
	\q
	

Initializing SQLAlchemy
-----------------------

First of all we need some code to initialize sqlalchemy and to create an
engine with a database specific dialect. We use the standard sqlalchemy
dburi format to create the engine. For more details refer to the sqlalchemy
documentation. We use this engine to create a session. A session could be
either bound or unbound. In this example we will create a bound session.

.. code-block:: python

    from sqlalchemy import *
    from sqlalchemy.orm import *

    engine = create_engine('postgresql://gis:password@localhost/gis', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

Model Definition
----------------

Let us assume we have to model the following GIS layers in the database:

* spots - represented as point geometry
* roads - represented as line geometry, and
* lakes - represented as polygon geometry

In SQLAlchemy, models can be defined either by declaring the model
classes and database tables separately and then mapping the classes to
the tables using mapper configuration, or they can be defined
declaratively using the new declarative extension. In this example we
use the SQLAlchemy declarative extension to define the model. Notes on how to use
GeoAlchemy with a non-declarative mapping are stated in the `Usage Notes
<usagenotes.html#notes-on-non-declarative-mapping>`_. We also
create a metadata object that holds the schema information of all
database objects and will thus be useful for creating the objects in
the database.

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
standard attributes using datatypes availabe under `sqlalchemy.types`. We
have also created a `geometry attribute` for each class using `GeometryColumn`
and Point, LineString and Polygon datatypes of GeoAlchemy. Here we pass the
dimension parameter to `GeometryColumn`. We leave out the `srid` parameter which
defaults to `4326`. This means that our geometry values will be in Geographic
Latitude and Longitude coordinate system.

Finally we have used `GeometryDDL`, a DDL Extension for geometry data types
that support special DDLs required for creation of geometry fields in the
database.

The above declaration is completely database independent, it could also be used for MySQL or Spatialite.
Queries written with GeoAlchemy are generic too. GeoAlchemy translates these generic expressions into 
the function names that are known by the database, that is currently in use.
If you want to use a database specific function on a geometry column, like `AsKML` in PostGIS, you will have to set a comparator
when defining your mapping. For the above example the mapping for `Spot` then would look like this:

.. code-block:: python

    from geoalchemy.postgis import PGComparator

    class Spot(Base):
        __tablename__ = 'spots'
        id = Column(Integer, primary_key=True)
        name = Column(Unicode, nullable=False)
        height = Column(Integer)
        created = Column(DateTime, default=datetime.now())
        geom = GeometryColumn(Point(2), comparator=PGComparator)
        
	# [..]



Now you can also use PostGIS specific functions on geometry columns. 

.. code-block:: python

	>>> s = session.query(Spot).filter(Spot.geom.kml == '<Point><coordinates>-81.4,38.08</coordinates></Point>').first()
	>>> session.scalar(s.geom.wkt)
	'POINT(-81.4 38.08)'

Note that you do not have to set a comparator, when you want to execute a database specific function 
on a geometry attribute of an object (*s.geom.kml*) or when you are directly using a function (*pg_functions.kml('POINT(..)')*).
You only have to set a comparator, when you are using a function on a geometry column (*Spot.geom.kml*).

The following comparators and database specific function declarations are available:

* PostGIS: *geoalchemy.postgis.PGComparator* and *geoalchemy.postgis.pg_functions*
* MySQL: *geoalchemy.mysql.MySQLComparator* and *geoalchemy.mysql.mysql_functions*
* Spatialite: *geoalchemy.spatialite.SQLiteComparator* and *geoalchemy.spatialite.sqlite_functions*
* Oracle: *geoalchemy.oracle.OracleComparator* and *geoalchemy.oracle.oracle_functions*

Creating Database Tables
------------------------

Now we use the metadata object to create our tables. On subsequent use
we will also first drop the tables so that the database is emptied
before creating tables.

.. code-block:: python

    metadata.drop_all()   # comment this on first occassion
    metadata.create_all()

Adding GIS Features
-------------------

Adding GIS features is now as simple as instantiating the model
classes and adding them to the SQLAlchemy session object that we
created earlier. GeoAlchemy enables creation of spatial attributes
specified using the Well Known Text (WKT) format using GeoAlchemy
`WKTSpatialElement` class.

.. code-block:: python

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

If you want to insert a geometry that has a different spatial reference system than your
geometry column, a transformation is added automatically.

.. code-block:: python

    geom_spot3 = WKTSpatialElement('POINT(30250865 -610981)', 2249)
    spot3 = Spot(name="Park", height=53.2, geom=geom_spot3)
    session.add(spot3)
    session.commit()

Scripts for creating sample gis objects as shown above are available
in the `examples directory
<http://bitbucket.org/sanjiv/geoalchemy/src/tip/examples/>`_. You could run those scripts to create the
database tables and the gis objects. Running them with -i option to
the interpreter will drop you at the interactive interpreter
promt. You can then follow the rest of the tutorial on the
interpreter.

.. code-block:: python

    $ python -i examples/tutorial.py
    >>>

Performing Spatial Queries
--------------------------

The GeoAlchemy project intends to cover most of the spatial operations
and spatial relations supported by the underlying spatial
database. Some of these are shown below and the rest are documented in
the reference docs.

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
    
Note that for all commands above a new query had to be made to the database. Internally
GeoAlchemy uses Well-Known-Binary (WKB) to fetch the geometry, that belongs to an object of a mapped class. 
All the time an object is queried, the geometry for this object is loaded in WKB.

You can also access this internal WKB geometry directly and use it for example to create a
`Shapely <http://trac.gispython.org/lab/wiki/Shapely>`_ geometry. In this case, no new query has to be made to 
the database.

.. code-block:: python

    >>> binascii.hexlify(s.geom.geom_wkb)
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
    >>> buffer_geom = DBSpatialElement(session.scalar(r.geom.buffer(10.0)))
    >>> session.scalar(buffer_geom.wkt)
    'POLYGON((-77.4495270615657 28.6622373442108,-77.9569183543725 28.4304851371862,-79.8646930595254 27.9795532202266, ........ ,28.6622373442108))'
    >>> envelope_geom = DBSpatialElement(session.scalar(r.geom.envelope))
    >>> session.scalar(envelope_geom.wkt)
    'POLYGON((-81.2000045776367 37.8899993896484,-81.2000045776367 38.2000007629395,-80.2999954223633 38.2000007629395,-80.2999954223633 37.8899993896484,-81.2000045776367 37.8899993896484))'
    >>> cv_geom = DBSpatialElement(session.scalar(r.geom.convex_hull))
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
    >>> session.scalar(r.geom.touches(l.geom))
    False
    >>> box = 'POLYGON((-82 38, -80 38, -80 39, -82 39, -82 38))'
    >>> session.query(Spot).filter(Spot.geom.within(box)).count()
    1L

Using the generic functions from *geoalchemy.functions* or the database specific functions from 
*geoalchemy.postgis.pg_functions*, *geoalchemy.mysql.mysql_functions* and *geoalchemy.spatialite.sqlite_functions*,
more complex queries can be made.

.. code-block:: python
	
	>>> from geoalchemy.functions import functions
	>>> session.query(Spot).filter(Spot.geom.within(functions.buffer(functions.centroid(box), 10, 2))).count()
	2L
	>>> from geoalchemy.postgis import pg_functions
	>>> point = 'POINT(-82 38)'
	>>> session.scalar(pg_functions.gml(functions.transform(point, 2249)))
	'<gml:Point srsName="EPSG:2249"><gml:coordinates>-2369733.76351267,1553066.7062767</gml:coordinates></gml:Point>'
	