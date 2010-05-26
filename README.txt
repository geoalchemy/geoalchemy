==========
GeoAlchemy
==========

GIS Support for `SQLAlchemy <http://www.sqlalchemy.org/>`_.

Introduction
------------
GeoAlchemy is an extension of SQLAlchemy. It provides support for
Geospatial data types at the ORM layer using SQLAlchemy. It aims to
support spatial operations and relations specified by the Open Geospatial
Consortium (OGC). The project started under Google Summer of Code Program
under the mentorship of `Mark Ramm-Christensen <http://compoundthinking.com/blog/>`_.

Requirements
------------
Requires SQLAlchemy > 0.6. Supported on Python 2.5 and Python 2.6.
Should also work with Python 2.4 but has not been tested. It also
requires a supported spatial database.


Supported Spatial Databases
---------------------------
At present `PostGIS <http://postgis.refractions.net/>`_, `Spatialite
<http://www.gaia-gis.it/spatialite/>`_, `MySQL <http://www.mysql.com/>`_
and `Oracle <http://www.oracle.com/technology/software/products/database/index.html>`_
are supported.

Support
-------
GeoAlchemy is at an early stage of development. Its mailing list is available on
`Google Groups <http://groups.google.com/group/geoalchemy>`_. The source code can be 
found on `BitBucket <http://bitbucket.org/geoalchemy/geoalchemy/>`_. Also, feel free to email 
the author directly to send bugreports, feature requests, patches, etc.


Installation
------------
To install type as usual::

    $ easy_install GeoAlchemy

Or, download the package, change into geoalchemy dir and type::

    $ python setup.py install


Documentation
-------------
Documentation is available online at http://geoalchemy.org.
You can also generate full documentation using sphinx by doing `make html`
in the `doc` dir and pointing the browser to `doc/_build/index.html`.


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

Contributors
------------

The contributors to this project (in alphabetical order are):

    Eric Lemoine
    Frank Broniewski
    Michael Bayer
    Mike Gilligan
    Sanjiv Singh
    Stefano Costa
    Tobias Sauerwein

