
Foursight Checks
================

Checks are the fundamental unit of work done in Foursight. They contain the entirety of code needed to make some observation or do some work and then take care of setting the result fields and storing the result. As mentioned in the `getting started <https://foursight.readthedocs.io/en/latest/getting_started.html>`_ documentation, checks are written in files called check modules and organized in the check setup. This document contains information on writing checks, as well as best practices for running them.

It is assumed that you've already read the getting started documentation. If not, head over and check that out before continuing. If you are interested in tips on the check development process, `go here <https://foursight.readthedocs.io/en/latest/development_tips.html>`_.

Quick reference for important check requirements
------------------------------------------------


* Checks must always start with the ``@check_function()`` decorator.
* Checks should always initialize their check results using the constructor, with the first argument as the FS connection and the second argument as the **exact string name of the check**.
* There should NOT be two checks with the same name. The tests will fail if this happens.
* Attributes of the check result, such as status, are simply set like: ``check.status = 'PASS'``.
* Checks should always end by returning the value of the check result: ``return check``.
* Checks have variable parameters through key word arguments (kwargs), two of which are most important: ``uuid`` and ``primary``.
* Due to lambda runtime limitations, checks will timeout and exit after running for a time set by the ``CHECK_TIMEOUT`` variable in ``chalicelib/utils.py``. You must keep your check runtimes under this limit.

Attributes you can set on a check result
----------------------------------------

The check result has a number of important attributes that determine what is stored as output of your check. Below is a list of different fields you can set on your check result within the body of your check. As always, the check function should return the check result object. Any of the following attributes can be set like this:

.. code-block::

   check = CheckResult(connection, 'my_check_name')
   check.status = 'PASS'
   check.summary = 'Test summary'
   check.<other_attr> = <value>
   ...
   return check

Here is a list of attributes that you will routinely use, with brief descriptions:


* **status**\ : string value. Must be one of: 'PASS', 'WARN', 'FAIL', 'ERROR', or 'IGNORE', otherwise it will be set to 'ERROR' automatically. 'IGNORE' by default, which means the check result will not be displayed on the UI.
* **summary**\ : string check summary that should be a short form of the description. If present, it will be displayed on the UI.
* **description**\ : string description of what the check is doing or explaining the output. Will be displayed on the UI.
* **brief_output**\ : Any value that will be displayed on the UI if set. The intended use of this attribute is as any output relevant to a check having a non-PASS status.
* **full_output**\ : same as brief_output, but is intended to hold the entirety of the check data.
* **admin_output**\ : same as brief_output, but is only visible to admins to view on the UI. Use for sensitive data.
* **ff_link**\ : a link to (presumably) Fourfront that will be displayed in the UI if provided. Should be relevant to the check.
* **action**\ : name of a Foursight action function that is linked to this check. See the `action docs <https://foursight.readthedocs.io/en/latest/actions.html>`_ for more information.
* **allow_action**\ : boolean value of whether or not the linked action can be run. Defaults to False. See the `action docs <https://foursight.readthedocs.io/en/latest/actions.html>`_ for more information.

Lastly, there are a number of attributes that are used internally. These do not usually need to be manually set, but can be.


* **kwargs**\ : these are set to the value of the key word parameters used by the check, which is a combination of default arguments and any overriding arguments (defined in the check setup or a manual call to run the check).
* **connections**\ : is set automatically from ``FSConnection``.
* **name**\ : the string name of the check that should be exactly equal to the name of the function you want the result to represent.

Our example check
-----------------

Let's say we want to write a check that will check Fourfront for all items that were released in the past day, which we will do by leveraging the "date_created" field. A reasonable place for this check to live is chalicelib/checks/wrangler_checks.py, since it is a metadata-oriented check. First, let's put down a barebones framework for our check using the ``check_function`` decorator to initialize the result for the check.

.. code-block::

   @check_function()
   def items_created_in_the_past_day(connection, **kwargs):
       check = CheckResult(connection, 'items_created_in_the_past_day')
       return check

At the moment, this check won't do anything but write a result to the ``items_created_in_the_past_day`` check directory, which will have some default values (namely status=ERROR). So, the body of the check can be thought of as doing the computation necessary to fill those fields of the check result. To actually get our check to do something, let's import ff_utils module from the central dcicutils package, which allows us to easily make requests to Fourfront. Imports should generally be done at the top level of your checks file, but they are shown in the function here for completeness. It's important to note that we can always get Fourfront access keys through ``connection.ff_keys``. Likewise, the current Fourfront environment (such as ``foufront-webdev`` or ``fourfront-webprod``\ ) using the ``connection.ff_env`` field. These are leveraged when using the ff_utils package.

.. code-block::

   @check_function()
   def items_created_in_the_past_day(connection, **kwargs):
       from dcicutils import ff_utils
       check = CheckResult(connection, 'items_created_in_the_past_day')
       check.status = 'PASS'
       check.description = 'Working description.'
       return check

Okay, now we are ready to use the ``ff_utils`` module to connect to Fourfront. Next we need to get a search result from Fourfront and use those results within our check. The big idea is that we will iterate through the search results and see which items have a ``date_created`` value of less than a day ago. I'm going to go ahead and add a lot to the check and describe it afterwards.

.. code-block::

   @check_function()
   def items_created_in_the_past_day(connection, **kwargs):
       from dcicutils import ff_utils
       check = CheckResult(connection, 'items_created_in_the_past_day')
       ### let item_type = 'Item for now'
       item_type = 'Item'
       # date string of approx. one day ago in form YYYY-MM-DD
       date_str = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
       search_query = ''.join(['search/?type=', item_type, '&q=date_created:>=', date_str, '&frame=object'])
       # this will return a list of hits from the search
       search_res = ff_utils.search_metadata(search_query, key=connection.ff_keys)
       full_output = {}
       item_output = []
       for res in search_res:
           item_output.append({
               'uuid': res.get('uuid'),
               '@id': res.get('@id'),
               'date_created': res.get('date_created')
           })
       if item_output:
           full_output[item_type] = item_output
       check.full_output = full_output
       if full_output:
           check.status = 'WARN'
           check.description = 'Items have been created in the past day.'
       else:
           check.status = 'PASS'
           check.description = 'No items have been created in the past day.'
       return check

There are a couple funky things happening in the check above. First, the ``search_metadata`` function gets search results for all items of the type ``item_type`` with a date_created field greater than that of the current UTC time minus one day. We then iterate through those results and add them to the ``full_output`` dictionary, keyed by ``item_type``. If any results were found, let's set the status to WARN and give a helpful description. If no items have been created in the past day, let's set the status to PASS to show that this check requires no attention.

This check is fully functional as written above, but it has a couple limitations. For example, it only operates on the ``item_type`` Item, which is the most generalized type of item and may cause a timeout in the lambda running this function if the resulting search result is very large. In the next section, we will use default check arguments and the check_group to further break down the check into different runs for different item types.

Check setup
-----------

Let's start by configuring our check setup (in ``check_setup.json``\ ) so that our check runs on all environment every morning. It will be part of the ``morning_checks`` schedule. It is assumed that you've already read the basics of the check setup in the `getting started <https://foursight.readthedocs.io/en/latest/getting_started.html#adding-checks-to-check_setup>`_ documentation, so we will start with the following.

.. code-block::

   {
       "items_created_in_the_past_day": {
           "title": "Items created in the past day",
           "group": "My example checks",
           "schedule": {
               "morning_checks": {
                   "all": {
                       "kwargs": {}
                   }
               }
           }
       }

Check arguments
---------------

A key word arguments (kwargs) object can be passed into your checks for internal use a couple ways. The first is through the ``check_function`` decorator. Any kwargs used in it's declaration will be available in the check. For example, the ``item_type`` variable in the check above would be better set as a default kwarg for the check as-so:

.. code-block::

   @check_function(item_type='Item')
   def items_created_in_the_past_day(connection, **kwargs):
       ...

These kwargs defined in the check function can be overwritten by those defined in the check setup. Note in the check setup above, the empty ``kwargs`` section means that the default key word arguments will be used when running this check. So if we wanted to run the ``items_created_in_the_past_day`` check with ``item_type = Experiment`` we could add the following key word argument to the check setup:

.. code-block::

   {
       "items_created_in_the_past_day": {
           "title": "Items created in the past day",
           "group": "My example checks",
           "schedule": {
               "morning_checks": {
                   "all": {
                       "kwargs": {"item_type": "Experiment"}
                   }
               }
           }
       }

This will cause the ``item_type`` to be overwritten in the check code. If you wanted to use the default ``item_type`` kwarg, you would just leave an empty dictionary under ``kwargs``. Using this system, it is very easy to specify different kwargs for different schedules and environments. In the example below, we use the default kwargs for the ``data`` environment and some unique kwargs for the ``webdev`` environment.

.. code-block::

   {
       "items_created_in_the_past_day": {
           "title": "Items created in the past day",
           "group": "My example checks",
           "schedule": {
               "morning_checks": {
                   "data": {
                       "kwargs": {}
                   },
                   "webdev": {
                       "kwargs": {"item_type": "Experiment"}
                   }
               }
           }
       }

Lastly, arguments that are not defined in the default kwargs through the ``check_function`` decorator can also be added to the dictionary:

.. code-block::

   {
       "items_created_in_the_past_day": {
           "title": "Items created in the past day",
           "group": "My example checks",
           "schedule": {
               "morning_checks": {
                   "all": {
                       "kwargs": {
                           "item_type": "Experiment",
                           "another_arg": "another_val"
                       }
                   }
               }
           }
       }

This would execute the ``items_created_in_the_past_day`` check with the default kwarg ``item_type=Item`` and the provided ``another_arg=another_val`` kwarg. This system allows checks to have multiple schedules with different parameters.

Using default kwargs can be important if they are required for a check's functionality. When run programmatically or from outside of a schedule these defaults may be used for the check. In such a case, the check may break if those arguments are not provided. It is up to the user to design his or her checks in a robust way.

The 'uuid' key word argument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You should not have to set it directly, but the ``uuid`` key word argument is very important, as it controls where the check is stored in S3. It is a string formatted timestamp of when the check was run. It will be automatically set when running checks through the ``queue_check_group`` utility.

The 'primary' key word argument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Foursight UI will automatically display the latest run check that was run with the ``primary`` key word argument set to ``True``. In most cases, this argument should be set when defining the key word arguments in ``check_setup.json``\ ; in some cases, you may want to set it during testing. Omitting this argument or setting its value to ``False`` will still cause the check to store its record in AWS S3 and overwrite the ``latest`` result for that check, but that result will not be shown on the UI.

The 'queue_action' key word argument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is a boolean kwarg that can be set to automatically queue the action associated with a check for running after the check is complete. This is best leveraged in the check setup for check/action combinations that you are confident in running without manual intervention. To queue an action, the check must have a valid ``check.action`` set and ``check.allow_action`` must be True. To control on which stages actions are queued, ``check.queue_action`` must be a string that exactly matches the current Foursight stage. For example, if running the check on the ``prod`` stage, you must have ``{"queue_action": "prod"}`` in the check kwargs.

Handling exceptions in checks
-----------------------------

Foursight will automatically catch any exceptions when running check code and automatically log the traceback of the exception to the ``full_output`` field. In such a case, the status of the check will be set to ``ERROR`` and the kwargs it was run with will be stored. All of this data is made available from the UI to facilitate debugging of the checks. For this reason, it is usually not necessary to write general try/except blocks in your check unless you are handling specific exceptions relevant to your code.

Appending check results
-----------------------

Sometimes you may want the same check to run multiple times and report results from all of the runs. Some possible examples would be a long running check that is split up by item type. This can be achieved by initializing the check results and passing in a ``uuid`` parameter of a previously run check. This will initialize the new check with the stored attributes of the old check and then allow you to add to them in your check function.

For example, let's use the check that we've been demonstrating over the past couple sections. It finds all items of a certain type that have been created in the past day and takes an ``item_type`` key word argument that determines the type. In addition, the ``full_output`` attribute is a dictionary keyed by the item type. So, we can easily pull a previous result from that check (that ran for ``item_type = Experiment``\ , for example) and add another item type (say, ``Biosample``\ ) to it. The desired ``full_output`` would have the following form:

.. code-block::

   {
       'Experiment': [ ... ],
       'Biosample': [ ... ]
   }

To achieve this, we will use manipulate the ``item_type`` key word argument and initialize the check running for ``Biosample`` with the results of the previous check that used ``Experiment``. All we need to do is change a couple lines from the ``items_created_in_the_past_day`` check that we defined above

First, add the ``uuid`` parameter to the constructor. Read it from the kwargs. This will take care of initializing the check result with the attributes of the results of the check with the given uuid (if it exists).

.. code-block::

   init_uuid = kwargs.get('uuid')
   check = CheckResult(connection, 'items_created_in_the_past_day', init_uuid=init_uuid)

Then, we just need to add the logic to use the ``full_output`` from previous results if available:

.. code-block::

   full_output = check.full_output if check.full_output else {}

Accessing previous/other check results
--------------------------------------

Another possibility for a check is to operate on the previous results of the same or other checks. To get results for the same check, you can use the same CheckResult object that is defined using the check name at the beginning of the check:

.. code-block::

   check = CheckResult(connection, 'change_in_item_counts')

Using the CheckResult ``check`` object, you have access to all CheckResult methods, which include the ``get_primary_result``\ , ``get_latest_result`` and ``get_closest_result`` methods, which both return dictionary representations of those historic check results. Here's quick summary of what they do:


* ``get_primary_result`` will return the result for the check with the ``primary=True`` key word argument, which is the one displayed on the Foursight front end.
* ``get_latest_result`` will return the last run result of the check, which does not necessarily mean it is ``primary``.
* ``get_closest_result`` can be used to get the check result that is closest the given time difference from the current time. See the example below:

.. code-block::

   check = CheckResult(connection, 'change_in_item_counts')

   # get the most recent primary result for this check (in dictionary form)
   primary = check.get_primary_result()

   # get the most recent result (of any kind!) for this check (in dictionary form)
   latest = check.get_latest_result()

   # get the dictionary results for this result run closest to 10 hours, 30 mins ago
   # args are in form (hours, minutes)
   older = check.get_closest_result(diff_hours=10, diff_mins=30)

The functions can be used to easily make a check that is aware of its own previous results. You can also make checks that use the results of other checks; to do this, define another check result object with the name of a different check. Consider the following example:

.. code-block::

   @check_function()
   def change_in_item_counts(connection, **kwargs):
       # use this check to get the comparison
       check = CheckResult(connection, 'change_in_item_counts')
       counts_check = CheckResult(connection, 'item_counts_by_type')
       primary = counts_check.get_primary_result()
       # get_item_counts run closest to 24 hours ago
       prior = counts_check.get_closest_result(diff_hours=24)

       # now do something with the primary and prior dictionaries
       # and set the fields of check accordingly

This check would compare the latest result and the result run closest to 24 hours ago from the current time for ``counts_check``. After any comparison is done, the fields of ``check`` would be set and finally we return ``check``.

Running checks from the UI
--------------------------

On the Foursight UI, users with administrator privileges can run individual checks directly, outside of the scope of the defined schedules. When this is done, the user can input values for all defined check kwargs within its ``check_function()`` decorator (hence the importance of those default arguments). The check will run with the these kwargs that are specified.

Check setup
-----------

As we have seen in the previous section, kwargs can be set individually for each check in the schedule, allowing a high level of flexibility with what can be done even with a single check. There are a couple more important points to mention about check setup.

Quick reference to important check setup requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


* The check setup is determined by the content of ``check_setup.json``.
* The entry for each check in the check setup must have the exact string name of the check function.
* Each check must only appear in the check setup once.
* All checks within the same schedule will automatically have the same ``uuid`` key word argument available to them.
* Dependencies can be set for a check by using the ``dependencies`` field within the schedule. This should be a list of string check names within the schedule that must be finished before the check will run.

Dependencies
^^^^^^^^^^^^

Using the running example from above, the following setup would require ``item_counts_by_type`` (not defined here) to run before ``items_created_in_the_past_day``. This depends on ``item_counts_by_type`` also using the same ``morning_checks`` schedule.

.. code-block::

   {
       "items_created_in_the_past_day": {
           "title": "Items created in the past day",
           "group": "My example checks",
           "schedule": {
               "morning_checks": {
                   "all": {
                       "dependencies": ["item_counts_by_type"]
                   }
               }
           }
       }
