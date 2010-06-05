from setuptools import setup, find_packages
import sys, os

version = '0.3.1'

setup(name='GeoAlchemy',
      version=version,
      description="Using SQLAlchemy with Spatial Databases",
      long_description=open('README.txt').read(),
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Environment :: Plugins",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Intended Audience :: Information Technology",
          "License :: OSI Approved :: MIT License",
          "Topic :: Scientific/Engineering :: GIS"
      ],
      keywords='geo gis sqlalchemy orm',
      author='Sanjiv Singh',
      author_email='singhsanjivk@gmail.com',
      maintainer='Eric Lemoine',
      maintainer_email='eric.lemoine@camptocamp.com',
      url='http://geoalchemy.org/',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests', "doc"]),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'SQLAlchemy>=0.6.1',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
