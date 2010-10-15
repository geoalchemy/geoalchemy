Usage notes
============

Notes for Spatialite
--------------------

Although Python2.5 and its higher versions include sqlite support,
while using spatialite in python we have to use the db-api module
provided by `pysqlite2 <http://code.google.com/p/pysqlite/>`_.  So we have to 
install pysqlite2 separately. Also, by default the pysqlite2 disables extension
loading. In order to enable extension loading, we have to build it
ourselves. Download the pysqlite tarball, open the file setup.cfg and
comment out the line that reads:

.. code-block:: python

    define=SQLITE_OMIT_LOAD_EXTENSION

Now save the file and then build and install pysqlite2:

.. code-block:: bash

    $ python setup.py install

Now, we are ready to use spatialite in our code. While importing pysqlite
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

Notes for Oracle
------------------

Oracle support was tested using Oracle Standard Edition 11g Release 2 with Oracle Locator. GeoAlchemy does
not work with Oracle 10g Express Edition, because it does not contain an internal JVM which is required
for various geometry functions.

cx_Oracle
~~~~~~~~~

GeoAlchemy was tested using the Python driver `cx_Oracle <http://cx-oracle.sourceforge.net/>`_. cx_Oracle
requires the environment variables ``ORACLE_HOME`` and ``LD_LIBRARY_PATH``. You can set them in Python before importing
the module.

.. code-block:: python

    import os
    os.environ['ORACLE_HOME'] = '/usr/lib/oracle/xe/app/oracle/product/10.2.0/server'
    os.environ['LD_LIBRARY'] = '/usr/lib/oracle/xe/app/oracle/product/10.2.0/server/lib'

    import cx_Oracle

Dimension Information Array (DIMINFO)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using a spatial index for a geometry column, Oracle requires a dimension information array 
(`Geometry Metadata Views: DIMINFO <http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_objrelschema.htm#sthref300>`_)
for this column. If you are using the DDL extension of GeoAlchemy to create your tables, you will
have to set a DIMINFO parameter in your table mapping.

.. code-block:: python

    diminfo = "MDSYS.SDO_DIM_ARRAY("\
                "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.000000005),"\
                "MDSYS.SDO_DIM_ELEMENT('LATITUDE', -90, 90, 0.000000005)"\
                ")"
    
    class Lake(Base):
        __tablename__ = 'lakes'
    
        lake_id = Column(Integer, Sequence('lakes_id_seq'), primary_key=True)
        lake_name = Column(String(40))
        lake_geom = GeometryColumn(Polygon(2, diminfo=diminfo), comparator=OracleComparator)
    
Some geometry functions also expect a DIMINFO array as parameter for every geometry that is passed in
as parameter. For parameters that are geometry columns or that were queried from the database, GeoAlchemy 
automatically will insert a subquery that selects the DIMINFO array from the metadata view ``ALL_SDO_GEOM_METADATA``
connected to the geometry.

Following functions expect a DIMINFO array:

- functions.length
- functions.area
- functions.centroid
- functions.buffer
- functions.convex_hull
- functions.distance
- functions.within_distance
- functions.intersection
- all functions in oracle_functions.sdo_geom_sdo_*

Example:

.. code-block:: python

    session.query(Lake).filter(Lake.lake_geom.area > 0).first()
    
    l = session.query(Lake).filter(Lake.lake_name=='Lake White').one()
    session.scalar(l.lake_geom.area)

For geometries that do not have an entry in ``ALL_SDO_GEOM_METADATA``, you manually have to 
set a DIMINFO array.

.. code-block:: python
    
    from sqlalchemy.sql.expression import text
    diminfo = text("MDSYS.SDO_DIM_ARRAY("\
            "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.000000005),"\
            "MDSYS.SDO_DIM_ELEMENT('LATITUDE', -90, 90, 0.000000005)"\
            ")")
            
    session.scalar(functions.area(WKTSpatialElement('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 4326), diminfo))

If you want to use a different DIMINFO array or a tolerance value as parameter, you have to set the
flag ``auto_diminfo``, so that GeoAlchemy is not trying to insert the DIMINFO array for you.

.. code-block:: python

    session.scalar(l.lake_geom.area(0.000000005, auto_diminfo=False))


Using Oracle functions
~~~~~~~~~~~~~~~~~~~~~~

ST_GEOMETRY functions
"""""""""""""""""""""""

Oracle supports the SQL/MM geometry type ``ST_GEOMETRY`` (see `ST_GEOMETRY and SDO_GEOMETRY Interoperability
<http://download.oracle.com/docs/cd/E11882_01/appdev.112/e11830/sdo_sql_mm.htm#CHDCADHC>`_). 
The functions inside the package ``MDSYS.OGC_*`` implement the specification `OGC Simple Feature Access`
and the functions inside the package ``MDSYS.ST_GEOMETRY.ST_*`` the specification `SQL/MM`.

GeoAlchemy uses the functions from these two packages for all functions in  ``functions.*`` except for the
following:

- functions.wkt
- functions.wkb
- functions.length
- functions.area
- functions.centroid
- functions.buffer
- functions.convex_hull
- functions.distance
- functions.within_distance
- functions.intersection
- functions.transform

The functions in ``MDSYS.OGC_*`` and ``MDSYS.ST_GEOMETRY.ST_*`` expect the geometry type ``ST_GEOMETRY``.
GeoAlchemy automatically adds a cast from ``SDO_GEOMETRY``. 

Some functions, like ``OGC_X`` or ``OGC_IsClosed``, also only work for subtypes of ``ST_GEOMETRY``, so that 
a cast to the subtype has to be added. If you are executing a function on a geometry column or on a geometry 
attribute of a mapped instance, GeoAlchemy will insert the cast to the subtype. Otherwise you will have 
to specify the geometry type:

.. code-block:: python

    session.scalar(functions.is_ring(WKTSpatialElement('LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)', geometry_type=LineString.name)))

Functions that return a Boolean value
""""""""""""""""""""""""""""""""""""""

Functions in Oracle return 'TRUE' or 1/0 instead of a Boolean value. When those functions
are used inside a where clause, GeoAlchemy adds a comparision, for example:

.. code-block:: python

    session.query(Lake).filter(
                Lake.lake_geom.sdo_contains('POINT(-88.9055734203822 43.0048567324841)'))
                        
    # is compiled to:
    SELECT .. 
    FROM lakes 
    WHERE SDO_CONTAINS(lakes.lake_geom, MDSYS.SDO_GEOMETRY(:SDO_GEOMETRY_1, :SDO_GEOMETRY_2)) = 'TRUE'    

Measurement functions
"""""""""""""""""""""

Measurement functions like ``area``, ``length``, ``distance`` and ``within_distance`` by default use meter as unit 
for geodetic data (like WGS 84) and otherwise the unit 'associated with the data'. If you want to use a different
unit, you can set it as parameter.

.. code-block:: python
    
    session.scalar(l.lake_geom.area('unit=SQ_KM'))
    
Member functions
""""""""""""""""

For member functions of ``SDO_Geometry`` (currently only ``oracle_functions.gtype`` and ``oracle_functions.dims``)
a table alias has to be used, when the function is called on a geometry column.

.. code-block:: python

    from sqlalchemy.orm import aliased
    spot_alias = aliased(Spot) 

    session.query(spot_alias).filter(spot_alias.spot_location.dims == 2).first()

Inserting NULL in geometry columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When synchronizing mapped instances with the database, SQLAlchemy uses bind parameters for insert/update
statements. Unfortunately cx_Oracle currently does not support the insertion of ``None`` into geometry columns when
using bind parameters. 

If you want to insert ``NULL`` you have to use the constant ``oracle.ORACLE_NULL_GEOMETRY``:

.. code-block:: python

    from geoalchemy.oracle import ORACLE_NULL_GEOMETRY
    
    spot_null = Spot(spot_height=None, spot_location=ORACLE_NULL_GEOMETRY)
    session.add(spot_null)
    session.commit();

Notes on DBSpatialElement
~~~~~~~~~~~~~~~~~~~~~~~~~~

For the other databases the result of a function call, that returned a new geometry, could be wrapped into a
`DBSpatialElement <reference/base.html#geoalchemy.base.DBSpatialElement>`_, so that new queries could be executed 
on that instance.

For Oracle the returned geometry is an object of ``SDO_Geometry``. cx_Oracle currently does not support
Oracle objects as argument in queries, so ``DBSpatialElement`` can not be used for Oracle.


Notes for MS Sql Server
-----------------------

The MS Sql Server spatial support has been tested using MS SQL Server 2008, connecting to it via pyODBC on Windows.

There is one important difference between SQL Server 2008 spatial support and PostGIS in that it is **not** possible
to restrict the spatial column to a specific type of geometry. All columns will be :class:`geoalchemy.geometry.Geometry`.

Supported functions
~~~~~~~~~~~~~~~~~~~

Most of the standard functions defined in GeoAlchemy are available and work as expected, but there are a few exceptions:

* g.centroid -- Only returns results for :class:`~geoalchemy.geometry.Polygon`
  and :class:`~geoalchemy.geometry.MultiPolygon`. Returns 'NULL' for all
  other :class:`~geoalchemy.geometry.Geometry`
* g.envelope -- Will always return a :class:`~geoalchemy.geometry.Polygon`
  regardless of the type of :class:`~geoalchemy.geometry.Geometry` it
  was called on
* g.buffer -- Only supports the buffer distance as a parameter
* g.transform -- Not defined
* g.within_distance -- Not defined
* g.covers -- Not defined
* g.covers_by -- Not defined
* g.intersection -- Not defined

MS Sql Server specific functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sql Server provides a number of additional spatial functions, details of which can be found in the documentation of
:class:`geoalchemy.mssql.ms_functions`. These additional functions can be used like any other function, or via
`ms_functions.function_name`.

.. code-block:: python

    session.query(Road).filter(Road.road_geom.instance_of('LINESTRING'))
    
    from geoalchemy.mssql import ms_functions
    ms_functions.buffer_with_tolerance('POINT(-88.5945861592357 42.9480095987261)', 10, 2, 0)
    
* :class:`~geoalchemy.mssql.ms_functions.text_zm`
* :class:`~geoalchemy.mssql.ms_functions.buffer_with_tolerance`
* :class:`~geoalchemy.mssql.ms_functions.filter`
* :class:`~geoalchemy.mssql.ms_functions.instance_of`
* :class:`~geoalchemy.mssql.ms_functions.m`
* :class:`~geoalchemy.mssql.ms_functions.make_valid`
* :class:`~geoalchemy.mssql.ms_functions.reduce`
* :class:`~geoalchemy.mssql.ms_functions.to_string`
* :class:`~geoalchemy.mssql.ms_functions.z`

Creating a spatial index
~~~~~~~~~~~~~~~~~~~~~~~~

Sql Server requires a bounding box that is equal to the extent of the data in the indexed spatial column.
As the necessary information is not available when the DDL statements are executed no spatial indexes
are created by default. To create a spatial index the bounding box must be specified explicitly when the
:class:`~geoalchemy.geometry.GeometryColumn` is defined:

.. code-block:: python

    class Road(Base):
       __tablename__ = 'ROADS'
   
       road_id = Column(Integer, primary_key=True)
       road_name = Column(String(255))
       road_geom = GeometryColumn(Geometry(2, bounding_box='(xmin=-180, ymin=-90, xmax=180, ymax=90)'), nullable=False)

Inserting NULL geometries
~~~~~~~~~~~~~~~~~~~~~~~~~

Due to a bug in the underlying libraries there is currently no support for inserting NULL geometries that have a `None`
geometry. The following code will not work:

.. code-block:: python

    session.add(Road(road_name=u'Destroyed road', road_geom=None))

To insert NULL you must use :data:`geoalchemy.mssql.MS_SPATIAL_NULL` to explicitly specify the NULL geometry.

.. code-block:: python

    session.add(Road(road_name=u'Destroyed road', road_geom=MS_SPATIAL_NULL))

This is an issue with pyODBC and can be tracked via `<http://code.google.com/p/pyodbc/issues/detail?id=103>`_.


Notes on non-declarative mapping
--------------------------------

In some cases it may be favored to define the database tables and the model classes separately. GeoAlchemy also
supports this way of non-declarative mapping. The following example demonstrates how a mapping can
be set up.

.. code-block:: python

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
	
	# enable the DDL extension   
	GeometryDDL(spots_table)
	
	# create table
	metadata.create_all()

	# add object
	session.add(Spot(spot_height=420.40, spot_location='POINT(-88.5945861592357 42.9480095987261)'))
	session.commit()
	
	s = session.query(Spot).get(1)
	print session.scalar(s.spot_location.wkt)
