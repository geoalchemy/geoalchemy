Tutorial : FeatureServer Using GeoAlchemy
=========================================


Introduction
------------

FeatureServer is a simple Python-based geographic feature server. It allows
you to store geographic vector features in a number of different backends,
and interact with them -- creating, updating, and deleting -- via a
REST-based API. It is distributed under a BSD-like open source license.

text.geo, the TurboGears2 extension for GIS makes it possible to use
FeatureServer with GeoAlchemy as datasource. It provides two main components:

*  GeoAlchemy Datasource - This allows geographic features to be stored in any of the spatial databases supported by `GeoAlchemy <http://geoalchemy.org>`_.
* FeatureServer Controller - This creates a new controller that reads the config and makes use of the FeatureServer API to dispatch requests to featureserver.

About this Tutorial
-------------------

In this tutorial we will create a TG2 app and use tgext.geo extension to configure and use featureserver to store, manipulate and retreive GIS features in a PostGIS database using GeoAlchemy as the ORM layer.


Installation
------------

It is assumed that a fresh virtualenv has been created and TG2 installed following the `TG2 Installation Guide <http://turbogears.org/2.0/docs/main/DownloadInstall.html#install-turbogears-2>`_. Install tgext.geo using easy_install::

    (tg2env)$ easy_install -i http://www.turbogears.org/2.0/downloads/current/index/ tgext.geo

We assume that a PostgreSQL server is installed and ready for use. Install PostGIS and create a new PostGIS enabled database called `gis`. Refer the docs `here <http://postgis.refractions.net/documentation>`_ to achieve this. We also need to install the python db-api for postgres::

    (tg2env)$ easy_install -i http://www.turbogears.org/2.0/downloads/current/index egenix-mx-base
    (tg2env)$ easy_install -i http://www.turbogears.org/2.0/downloads/current/index psycopg2 


Creating a New TG2 App
----------------------

Create a new TG2 app with gis capability::

    (tg2env)$ paster quickstart TGFeature --geo
    (tg2env)$ cd TGFeature


Model Definition for Features
-----------------------------

We assume that we have to model a layer of roads in our application. We open the tgfeature/model/__init__.py file in the package and add the following model definition::

    class Road(DeclarativeBase):
        __tablename__ = 'roads'
        id = Column(Integer, primary_key=True)
        name = Column(Unicode, nullable=False)
        width = Column(Integer)
        created = Column(DateTime, default=datetime.now())
        geom = GeometryColumn(LineString(2))

    GeometryDDL(Road.__table__)

Apart from the standard attributes, we have defined a spatial attribute called `geom` as a `GeometryColumn`. We will use this attribute to store geometry values of data type `LineString` in the database. GeoAlchemy supports other geometry types such as Point, Polygon and Mutiple Geometries. We also pass the dimension of the geometry as a parameter. The Geometry type takes another parameter for the `SRID`. In this case we leave it to its default value of `4326` which means that our geometry values will have geographic latitude and longitude coordinate system. We finally call the GeometryDDL DDL Extension that enables creation and deletion of geometry columns just after and before table create and drop statements respectively. The GeometryColumn, LineString and GeometryDDL must be imported from the geoalchemy package.

Creating Tables in the Database 
-------------------------------

The database tables can now be created using the setup-app paster command

.. code-block:: bash

    $ (tg2env) paster setup-app development.ini

In case we need sample data to be inserted during application startup, we must add them into the setup script, i.e. tgformat/websetup.py prior ro running the setup command. Let us add some sample data.

.. code-block:: python

    wkt = "LINESTRING(-80.3 38.2, -81.03 38.04, -81.2 37.89)"
    road1 = model.Road(name="Peter St", width=6, geom=WKTSpatialElement(wkt))
    wkt = "LINESTRING(-79.8 38.5, -80.03 38.2, -80.2 37.89)"
    road2 = model.Road(name="George Ave", width=8, geom=WKTSpatialElement(wkt))
    model.DBSesion.add_all([road1, road2])



FeatureServer Config
--------------------

Now we need to configure our app by declaring certain parameters under the [app:main] section of the ini file. In this case we use development.ini as we are in development mode right now.

.. code-block:: python

    geo.roads.model=tgfeature.model
    geo.roads.cls=Road
    geo.roads.table=roads
    geo.roads.fid=id
    geo.roads.geometry=geom

The config parameters use a geo.<layer>.param=value format. This allows additional layers to be defined within the same app as follows:

.. code-block:: python

    geo.lakes.model=tgfeature.model
    geo.lakes.cls=Lake
    geo.lakes.table=lakes
    geo.lakes.fid=id
    geo.lakes.geometry=geom

In this tutorial, however, we will use only the roads layer.

Using the FeatureServerController
---------------------------------

We can now import and mount the FeatureServer Controller inside our root controller.

.. code-block:: python

    from tgfeature.model import DBSession
    from tgext.geo.featureserver import FeatureServerController

    class RootController(BaseController):
        ....
        roads = FeatureServerController("roads", DBSession)

We pass two parameters here. The first one being the layer name. This must be the same as layer name used in development.ini. The second parameter is the sqlalchemy session. In case we were using the lakes layer too, as shown in the sample config, we would create two controller instances as:

.. code-block:: python

    class RootController(BaseController):
        ....
        roads = FeatureServerController("roads", DBSession)
        lakes = FeatureServerController("lakes", DBSession)

Testing the Server using curl
-----------------------------

We are now ready to start and test out new geo-enabled TG2 app. Start the server in development mode by running:

.. code-block:: bash

    $(tg2env) paster serve --reload development.ini

Note the `--reload` option. This tells the server to reload the app whenever there is change in any of the package files that are in its dependency chain. Now we will open up a new command window and test the server using the `curl` utility.

.. code-block:: bash

    # Request the features in GeoJSON format (default)
    $ curl http://localhost:8080/roads/all.json
    or simply
    $ curl http://localhost:8080/roads
    {"crs": null, "type": "FeatureCollection", .... long GeoJSON output

    # Request the features in GML format
    $ curl http://localhost:8080/8080/roads/all.gml
    <wfs:FeatureCollection
   	xmlns:fs="http://example.com/featureserver
        ....   long GML output

    # Request the features in KML format
    $ curl http://localhost:8080/roads/all.kml
    <?xml version="1.0" encoding="UTF-8"?>
        <kml xmlns="http://earth.google.com/kml/2.0"
        ....  long KML output

Now lets create a new feature using curl. Store the following json data in a new file postdata.json:

.. code-block:: javascript

    {"features": [{
        "geometry": {
            "type": "LineString",
            "coordinates": [[-88.913933292993605, 42.508280299363101],
                            [-88.8203027197452, 42.598566923566899],
                            [-88.738375968152894, 42.723965012738901],
                            [-88.611305904458604, 42.968073292993601],
                            [-88.365525649681501, 43.140286668789798]
            ]
        },
        "type": "Feature",
        "id": 10,
        "properties": {"name": "Broad Ave", "width": 10}
    }]}

Create a POST request using this data and send it to the server.

.. code-block:: bash

    $(tg2env) curl -d @postdata.json http://localhost:8080/roads/create.json

This creates a new feature and returns back the features in json format. To modify the feature edit the postdata.json file and change the properties. Lets change the name property from `Broad Ave` to `Narrow St` and the width property from `10` to `4`. The modify url should include the feature id as shows below:

.. code-block:: bash

    $(tg2env)  curl -d @postdata.json http://localhost:8080/roads/3.json

For deleting the feature simly send a DELETE request with the feature id in the url:

.. code-block:: bash

    $(tg2env) curl -X DELETE http://localhost:8080/roads/3.json

An OpenLayers Application Using FeatureServer
---------------------------------------------

The server is now ready to be accessed by client applications for storing, manipulating and deleting featues. `OpenLayers <http://openlayers.org>`_ is an open source javascript web mapping application. It is quite matured and is under active development. To develop an OpenLayers web application using featureserver the developer is strongly recommended to have a look at the demo application available with the featureserver source code. Copy the demo app (index.html in side featureserver source code directory) to the public folder under the different name:

.. code-block:: bash

    $(tg2env) cp /path/to/featureserversource/index.html tgformat/public/demo.html
    $(tg2env) cp /path/to/featureserversource/json.html tgformat/public/
    $(tg2env) cp /path/to/featureserversource/kml.html tgformat/public/

Now modify these files to change the following::

    * change all references to featureserver.cgi to '' (null)
    * change all references to scribble to 'roads' (layer)

Point your browser to http://localhost:8080/demo.html. You should now be able to view, create and modify features using featureserver running inside your TG2 app.

.. todo:: Add authentication and authorization notes

.. todo:: Review this file for todo items.

