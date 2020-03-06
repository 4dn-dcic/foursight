
Development Tips
================

This documentation is meant to help you get up and running writing checks for Foursight. It includes some useful tips and functions outside of the scope of the strictly necessary stuff contained in the `getting started <https://foursight.readthedocs.io/en/latest/getting_started.html>`_ and `checks <https://foursight.readthedocs.io/en/latest/checks.html>`_ documentation. First, we will go over the timeline of writing a new check module containing new checks and scheduling it.

Overall process for adding, testing, and scheduling a brand new check
---------------------------------------------------------------------


#. Create your new check module file in the ``chalicelib/checks`` directory.
#. Write your checks within that file.
#. Add your check to ``check_setup.json``.
#. Do some testing of your new check.
#. Deploy to Foursight ``dev`` and test it live. `See here <https://foursight.readthedocs.io/en/latest/deployment.html>`_.

Testing tips
------------

Manual testing of your check
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's assume that you've already finished steps 1 through 3 in the list above (these are pretty much covered in the getting started and checks documentation). For step 4, it is recommended that you go into a local Python interpreter and run your check directly to ensure that it provides the output you want. Below is some code run from the root directory of this project that will outline manual testing of the ``items_created_in_the_past_day`` check contained within the ``wrangler_checks`` check module. The code below is run from the Python interpreter.

.. code-block:: python

   >>> import app
   # set the stage for foursight - currently 'dev' is the default
   # NOTE: you probably want to change this to 'prod' to get a result posted to s3 (see below)
   >>> app.set_stage('prod')
   # create a Foursight connection to the 'mastertest' environment
   >>> connection = app.init_connection('mastertest')
   # run your check using the run_check_or_action utility
   # args are: FS connection object, string check name, dict of kwargs to run with
   # it could be useful to add a break point within your check function to see what's happening
   >>> app.run_check_or_action(connection, 'wrangler_checks/items_created_in_the_past_day', {})
   # some possible output:
   {'name': 'items_created_in_the_past_day', 'title': 'Items Created In The Past Day',
   'description': 'No items have been created in the past day.', 'status': 'PASS',
   'uuid': '2018-01-16T19:14:34.025445','brief_output': None,'full_output': {},
   'admin_output': None, 'ff_link': None}

   # you can also run with kwargs...
   >>> app.run_check_or_action(connection, 'wrangler_checks/items_created_in_the_past_day', {'item_type': 'File'})

It's important to note that if you return the ``check`` at the end of your function, then the result will always get written to S3. In this case, it is probably best to not overwrite the primary check result when testing (which is the one shown on the Foursight UI). Since running a check will always overwrite the ``latest`` result, you can test safely by omitting the ``primary=True`` key word argument and fetching those results with ``get_latest_result`` method of your check result.

To overwrite the primary check result, you must set the ``primary=True`` key word argument for your check. If you want to, you can pass this dictionary into the ``run_check_or_action`` function:

.. code-block:: python

   # will overwrite the latest result for items_created_in_the_past_day, which won't display on the UI
   app.run_check_or_action(connection, 'wrangler_checks/items_created_in_the_past_day', {})

   # will overwrite the latest + primary results for items_created_in_the_past_day and display it on the UI
   app.run_check_or_action(connection, 'wrangler_checks/items_created_in_the_past_day', {'primary': True})

If writing any results to S3 is not desirable during your testing, either insert a break point before that function or not return the check result.

Testing on the UI
^^^^^^^^^^^^^^^^^

The Foursight UI is also a useful place to test your checks, since it is very easy to adjust the check key word arguments when signed in as admin. In this mode, setting the ``primary`` argument to anything but ``True`` will cause your test to run (and be available on the /history/ page) with overwriting the current primary result.

Just make sure to do your testing on the development stage of Foursight, which you can deploy to locally using ``python -m deploy dev``. Please keep in mind that you will need to have some environment variables locally to make this work.

Automated Check Testing
^^^^^^^^^^^^^^^^^^^^^^^

In the below section we document some code on how to test out a Foursight check in the Python interpreter. We've also provided a script in the top level ``scripts`` directory that will automate this process. If you `cd` into the script directory and run ``python test_check.py`` some help on how to use it will appear.

Manual testing of your action
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Actions function very similarly to checks when run individually. In fact, testing them is completely the same; the only difference is the different output. Below is some code that would test an action called ``patch_file_size`` in the ``wrangler_checks`` module.

**NOTE:** you must add ``check_name`` and ``called_by`` parameters to your ``run_check_or_action`` call to test an action. If your action doesn't leverage these parameters specifically, you can give whatever value you want.

**WARNING:** when manually running an action, be aware that it actually be executed on the given Foursight connection. For that reason, when in testing stages it is best to remove any impactful code within an action or insert a break point to have manual control.

.. code-block:: python

   >>> import app
   # create a Foursight connection to the 'mastertest' environment
   >>> connection = app.init_connection('mastertest')
   >>> app.run_check_or_action(connection, 'wrangler_checks/patch_file_size', {'check_name': None, 'called_by': None})
   # some possible output:
   {'name': 'patch_file_size','description': None, 'status': 'DONE',
   'uuid': '2018-01-16T19:14:34.025445', 'output': [] ...}

   # you can also run with kwargs...
   >>> app.run_check_or_action(connection, 'wrangler_checks/patch_file_size', {'check_name': 'some_check_name', 'called_by': 'some_uuid', 'some_arg': 'some_value'})

Manual testing of your schedule
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's say you want to run a whole schedule and not an individual check. To test this, you can use ``app.queue_scheduled_checks``\ , which causes your checks to run on AWS. This function is the one that is internally used to run checks, but it is difficult to track output. For that reason, it may be easier to test with ``run_check_or_action`` as described above. Below are examples from the Python interpreter with the example schedule named ``morning_checks``.

**NOTE:** if a check setup has kwargs including ``primary=True``\ , then the result will be written live to the Foursight UI. Omitting this argument when testing your check may be desirable.

.. code-block:: python

   >>> import app
   # queue_scheduled_checks takes the environment name directly (not connection)
   # runs async; to see the results, see the Foursight UI, S3, or use Foursight API
   >>> app.queue_scheduled_checks('mastertest', 'morning_checks')

Some other testing notes
^^^^^^^^^^^^^^^^^^^^^^^^


* By default, you will use the ``dev`` stage of Foursight from the Python interpreter and test.py. To change to ``prod`` (USE WITH CARE), use ``app.set_stage('prod')``.
* You can extend the timeout of your checks/actions locally by using ``app.set_timeout(num)``\ , where ``num`` is an integer representing timeout in seconds. Setting it 0 will disable the timeout completely.
* You can get the latest check results using ``app.get_check_results(connection)`` given a Foursight connection.
* Make sure to use dcicutils for lots of handy utility functions to connect with Fourfront!

Scheduling your checks
^^^^^^^^^^^^^^^^^^^^^^

Okay, so you've written a check function and want to make a new schedule for it. To schedule it using a CRON or rate expression, go to the top of app.py and create a new scheduled function (leading with the ``@app.schedule()`` decorator). Two examples are below:

.. code-block:: python

   @app.schedule(Rate(1, unit=Rate.HOURS))
   def one_hour_checks(event):
       # run this schedule for all environments
       queue_scheduled_checks('all', 'one_hour_checks')

Or scheduling with a CRON expression... for more info, `see here <http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html>`_.

.. code-block:: python

   # run at 10 am UTC every day
   @app.schedule(Cron(0, 10, '*', '*', '?', '*'))
   def daily_checks(event):
       queue_scheduled_checks('all', 'daily_checks')
