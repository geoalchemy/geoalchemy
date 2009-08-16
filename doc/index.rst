.. GeoAlchemy documentation master file, created by
   sphinx-quickstart on Tue Aug  4 21:27:34 2009.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GeoAlchemy: Using SQLAlchemy with Spatial Databases
===================================================

GIS Support for `SQLAlchemy <http://www.sqlalchemy.org/>`_.

Introduction
------------
GeoAlchemy is an extension of SQLAlchemy. It provides support for
Geospatial data types at the ORM layer using SQLAlchemy. It aims to
support spatial operations and relations specified by the Open Geospatial
Consortium (OGC).

Requirements
------------
Requires SQLAlchemy >= 0.5. Supported on python 2.5 and python 2.6.
Should also work with python 2.4 but has not been tested. It also
requires a supported spatial database.


Supported Spatial Databases
---------------------------
At present `PostGIS <http://postgis.refractions.net/>`_, `Spatialite
<http://www.gaia-gis.it/spatialite/>`_ and `MySQL Spatial
<http://www.mysql.com/>`_  are supported.

Support
-------
GeoAlchemy is at an early stage of development. It does not yet have a
mailing list of its own but support should be available on the SQLAlchemy
Mailing List or IRC Channel. Also, feel free to email the author directly
to send bugreports, feature requests, patches, etc.


Installation
------------
To install type as usual::

    $ easy_install GeoAlchemy

Or, download the package, change into geoalchemy dir and type::

    $ python setup.py install


Documentation
-------------
Documentation is available online at `http://geo.turbogears.org/geoalchemy`.
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


Contents:

.. toctree::
   :maxdepth: 2

   install.rst
   tutorial.rst
   reference.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

