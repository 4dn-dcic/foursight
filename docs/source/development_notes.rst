.. role:: raw-html-m2r(raw)
   :format: html


Development Notes
=================

Since pyyaml is a pip-installed package with C-extensions but without an available wheel, I had to package it and include it in the /vendor directory. See `here <http://chalice.readthedocs.io/en/latest/topics/packaging.html>`_ for more information. Note that you MUST use an EC2 using the Amazon linux to create the wheel.

Running Tests Locally
^^^^^^^^^^^^^^^^^^^^^

Tests can be run using pytest like so:
``pytest tests``\ :raw-html-m2r:`<br>`
``pytest tests/test_check_utils``
``pytest tests -k test_action_result_methods``

Local Foursight
^^^^^^^^^^^^^^^

To run a local version of Foursight, run ``chalice local`` at the top level. Navigate to ``localhost:8000`` to see the ``dev`` site.
