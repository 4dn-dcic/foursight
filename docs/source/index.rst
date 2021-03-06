=========
Foursight
=========

A serverless chalice application to monitor and run tasks on `Fourfront <https://github.com/4dn-dcic/fourfront>`_. Essentially, the app provides an number of endpoints to run checks, fetch results, dynamically create environments to check, and more.


.. image:: https://travis-ci.org/4dn-dcic/foursight.svg?branch=production
   :target: https://travis-ci.org/4dn-dcic/foursight
   :alt: Build Status

.. image:: https://coveralls.io/repos/github/4dn-dcic/foursight/badge.svg?branch=production
   :target: https://coveralls.io/github/4dn-dcic/foursight?branch=production
   :alt: Coverage

.. image:: https://readthedocs.org/projects/foursight/badge/?version=latest
   :target: https://foursight.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

Beta version
------------

Foursight is under active development and features will likely change.


API Documentation
-----------------

Foursight uses autodoc to generate documentation for both the core chalicelib and checks. You can find the autodocs in the ``Check Documentation`` files.


Foursight-core
--------------

For the rest of the documentation, see `Foursight-core documentation <https://foursight-core.readthedocs.io/en/latest/>`_.


*Contents*

 .. toctree::
   :maxdepth: 4

   getting_started
   checks
   actions
   deployment
   environments
   elasticsearch
   development_notes
   development_tips
   check_modules
