from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='geoalchemy',
      version=version,
      description="Geospatial data types support for SQLAlchemy",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='geo gis sqlalchemy orm',
      author='Sanjiv Singh',
      author_email='singhsanjivk@gmail.com',
      url='http://gsoc.turbogears.org/',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'SQLAlchemy>=0.5',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
