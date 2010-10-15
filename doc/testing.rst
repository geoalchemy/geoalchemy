Testing GeoAlchemy
==================

Requirements
------------

In order to test GeoAlchemy you must have nose tools installed. Also you should
have installed SQLAlchemy, GeoAlchemy and all the DB-APIs. Refer the
`installation docs`_ for details. In case you are interested in
testing against only one DB-API (say Postgres), you need not have the other
APIs installed. However, you will be able to run those specific tests only.

If you intend to run the tests for Spatialite, you must also have Spatialite
extension installed. Also make you have enabled extension loading in your
pysqlite build. Refer the `Spatialite specific notes`_ for details.


Preparation
-----------

You must prepare the databases to run the tests on. Spatial feature is enabled
by default in MySQL. You must setup a spatial database in PostgreSQL using
PostGIS. Then modify the test_mysql.py and test_postgis.py files in
geoalchemy/tests to use the correct dburi (host, username and password)
combination. No database preparation is required for Spatialite, as we
would use an in-memory database.

Running the Tests
-----------------

The tests can be run using nosetests as:

.. code-block:: bash

    $ nosetests

For running the tests against a single database at a time:

.. code-block:: bash

    $ nosetests geoalchemy/tests/test_postgis.py
    $ nosetests geoalchemy/tests/test_mysql.py
    $ nosetests geoalchemy/tests/test_spatialite.py

.. _`installation docs`: install.html
.. _`Spatialite specific notes`: tutorial.html#notes-for-spatialite
