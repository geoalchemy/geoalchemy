Introduction
============

GeoAlchemy provides extensions to SQLAlchemy to work with spatial databases.

The current supported spatial database systems are `PostGIS
<http://postgis.refractions.net/>`_, `Spatialite
<http://www.gaia-gis.it/spatialite/>`_, `MySQL <http://www.mysql.com/>`_,
`Oracle
<http://www.oracle.com/technology/software/products/database/index.html>`_, and
`MS SQL Server 2008
<http://www.microsoft.com/sqlserver/2008/en/us/spatial-data.aspx?pf=true>`_.

GeoAlchemy is an extension of SQLAlchemy. It provides support for
Geospatial data types at the ORM layer using SQLAlchemy. It aims to
support spatial operations and relations specified by the Open Geospatial
Consortium (OGC). 
Installation
------------

To install GeoAlchemy use EasyInstall as follows::

    $ easy_install GeoAlchemy

Or, download the package, change into the geoalchemy dir and type::

    $ python setup.py install

GeoAlchemy is known to work with Python 2.5 and Python 2.6, but it should also
work with Python 2.4 and 2.7.

Documentation
-------------

The documentation is available online at http://geoalchemy.org.

Community
---------

* Mailing list: http://groups.google.com/group/geoalchemy
* Code repository: http://bitbucket.org/geoalchemy/geoalchemy/
* Issue tracket: http://bitbucket.org/geoalchemy/geoalchemy/

Package Contents
----------------

  geoalchemy/
      Source code of the project.

  geoalchemy/tests/
      Unittests for GeoAlchemy.

  doc/
      Documentation source.

  examples/
      A few examples demonstrating usage.

License
-------

GeoAlchemy is released under the MIT License.

Authors
-------

Éric Lemoine (maintainer)
Sanjiv Singh
Tobias Sauerwein

History
-------

GeoAlchemy was initiated by Sanjiv Singh, during Google Summer of Code 2009,
under the mentorship of `Mark Ramm-Christensen
<http://compoundthinking.com/blog/>`_.  The initial code was based on example
code written by Michael Bayer, the author of SQLAlchemy project.
