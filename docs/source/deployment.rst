
Deploying Foursight
===================

As mentioned in the `getting started <./getting_started.md>`_ documentation, there are two stages supported by Foursight: ``dev`` and ``prod``. Dev is used for developmental work, such as building new checks and testing them on live Fourfront environments. Prod is used production Foursight code and should only be deployed to when you are certain your checks are functioning correctly. The current way to deploy Foursight locally is to use the deploy.py script like this (from the root directory of the project):

.. code-block::

   python -m deploy <stage>

Where ``<stage>`` is either ``dev`` or ``prod``. If there is a packaging issue with your code, Chalice will catch it and log messages accordingly.

You will need a number of environment variables to be present locally to be able to deploy Foursight.

You can also run a local deployment of the dev application by running 'chalice local'. This is the recommended method for testing fixes that do not require a true deployment to test.

Automatic deployments
---------------------

The Github repository for Chalice is outfitted with Travis CI testing and will deploy automatically when code is merged into the production branch. When you merge into production and tests pass, automatic deployment to the prod stage will occur.

Running tests
-------------

As you add checks to Foursight, please take the time to create tests for them. The current test setup is pretty basic: the Python ``unittest`` package can be executed locally from the root directory with the following command:

.. code-block::

   python -m test # run all tests
   # can also run with <optional test class>.<optional test fxn>
   python -m TestCheckUtils.test_fetch_check_group
