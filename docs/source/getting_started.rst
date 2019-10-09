
Getting Started
===============

Foursight provides insight into your application (namely, Fourfront) using AWS infrastructure.

The big picture
---------------

Foursight is a Chalice application, which essentially means it is a combination of AWS Lambda functions that are linked to different endpoints through API gateway. It allows for scheduling of lambdas through Cloud Watch. Using Chalice makes it easy to deploy all these different resources with cohesive settings. Currently, Foursight is written for **Python 3.6**. You may encounter errors if running on previous Python versions.

Foursight itself is based around the concepts of 'checks', which perform customized Python code using AWS Lamdbda functions. Each check is supposed to do some small-ish amount of computation and store JSON results on AWS ElasticSearch/S3. The endpoints for the Chalice application (defined in app.py) determine which checks are called, when results are fetched, and the associated scheduling. It also builds a simple front end using Jinja2 to visualize check results, though if you love JSON, viewing the raw results directly is completely fine.

Checks are defined in individual files (called check modules), which live in the chalicelib/checks directory. The organization and scheduling of checks in done in chalicelib/check_setup.json. For example, if you made a handful of checks that you wanted to run daily, you would create entries for each of these checks in the check setup and schedule them to use the ``morning_checks`` schedule, which is tied to a AWS CloudWatch CRON. An example check module is chalicelib/checks/system_checks.py. More details on how to write checks and the check_setup are below.

Installing dependencies
-----------------------

Before developing with Foursight, you must install the required Python packages and set up your AWS credentials locally. It is best practice to use a virtual environment. Packages can be installed with pip using the following command from the root Foursight directory:

.. code-block::

   pip install -r requirements.txt

Stages and environments
-----------------------

These are two important concepts to introduce. There are currently two Foursight stages, ``dev`` and ``prod``\ , which each define a separately deployed Chalice application. That means there is a different set of Lambdas, API Gateways, etc. for each stage. The reason to have multiple stages is so the one stage, ``dev``\ , can be used for testing new stuff while ``prod`` is for battle-tested checks. For information on how to deploy to specific stages, see the `deployment documentation <./deployment.md>`_.

Foursight environments correspond to different Fourfront configurations and store their results separately of each other. For example, Fourfront has production and staging environments, which should have tests run individually on them. Environments are dynamically initialized for each Foursight API requests\ and are based off of items in ES/S3. Both stages of Foursight have access to each individual environment and the results for checks get stored separately. For example, there checks are stored separately for the production environment on dev stage and the production environment on prod stage. There would also be a unique bucket for checks on the staging environment on dev stage and the staging environment on prod stage, etc. Thus, there is a unique storage location for each combination of stage and environment. To read more about environments, see the `environments documentation <./environments.md>`_.

Creating a check
----------------

The most fundamental unit of Foursight is the check. These encapsulate Python code that will run using AWS Lambda and will store results in ES/S3, which can later be visualized and queried. For the following, it's assumed that you want to create a check within an existing check module (i.e. file containing checks). Such a file is chalicelib/checks/system_checks.py.

Here's a simple check and then we'll go through the details:

.. code-block::

   @check_function()
   def my_first_check(connection, **kwargs):
       check = CheckResult(connection, 'my_first_check')
       check.full_output = {'key1': 'ok_value', 'key2': 'warning_value'}
       check.brief_output = {'key2': 'warning_value'}
       # check status should be one of ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE']
       check.status = 'PASS'
       check.summary = 'The first check I've ever made!'
       return check

The first thing to note is each check *must* start with the ``@check_function()`` decorator. This allows Foursight to determine which functions are checks. In addition, any default key word arguments (kwargs) that you want to define for a check can be passed into the decorator as parameters (more on this later). Next, the check itself must take a ``connection`` parameter and ``**kwargs``... the latter is not strictly necessary but should always be included as good form. Once adding the ``@check_function()`` decorator to a function, the function will be considered a check no matter what, and will be displayed on the Foursight front end.

The ``kwargs`` system allows a lot of flexibility when desiging checks. By making the functionality of your check vary based on its kwargs, you can join multiple small checks together or more finely control the parameters the check runs with. There are two very import key word arguments that are always present in the kwargs (they are added automatically if not provided): ``uuid`` and ``primary``. ``uuid`` is a string timestamp that identifies the check and ``primary`` is a boolean that control whether a certain run of the check will be stored as the "de-facto" result that is presented on the Foursight UI. Read more about `checks documentation <./checks.md>`_.

In the body of the check, the first thing to do is initialize a CheckResult object (named ``check``\ , above) using the constructor. The CheckResult is an object that internally takes care of things like check querying and storing of results in ES/S3. It is key that the constructor is **given the connection as its first argument and the exact name of the check function as its second argument.** That is worth repeating: for my check above, named "my_first_check", I must pass that exact string as the second argument to the constructor; otherwise, the check will be stored under a different name and impossible to retrieve using the automated front end. In Python this, is equivalent to ``function.__name__`` for the function you are writing.

The CheckResult has a number of fields that can be set, namely: ``status``\ , ``summary``\ , ``description``\ , ``full_output``\ , and ``brief_output`` (and a few others). These are the displayed values for the check. With the exception of status, they are all flexible and can be set to any value you choose within your check. Status must be one of: ``PASS``\ , ``WARN``\ , ``FAIL``\ , ``ERROR``\ , or ``IGNORE``. These fields determine how the check is displayed. ``full_output`` is generally considered the entire output of the check that you care about, whereas ``brief_output`` is the output relevant to the final status of the check. Consider the revised check, below:

.. code-block::

   @check_function()
   def my_first_check(connection, **kwargs):
       check = CheckResult(connection, 'my_first_check')
       # custom fxn below. lets say we are worried if its output is < length 5
       check.full_output = my_function_to_get_all_results()
       if len(check.full_output) < 5:
           check.brief_output = 'Length is less than 5!'
           check.status = 'WARN'
       else:
           check.status = 'PASS'
       check.summary = 'The first check I've ever made!'
       return check

Returning ``check`` at the end of the check causes the result of the check to be written to ES/S3 with a unique key created by the check name and time the check was initiated. In addition, if a key word argument of ``primary=True`` is provided to your check, running it will overwrite the last "primary" check result, which is the one displayed from the Foursight front end. This is an important behavior of Foursight--the latest ``primary=True`` result is the one displayed.

There are many possibilities to what a check can do. Please visit the `writing checks documentation <./checks.md>`_ for more information.

Creating a schedule
-------------------

To get your checks running on a CRON or rate schedule, the current method is add the desired schedule at the top of app.py. ``queue_check_group`` will cause your checks to be added to an AWS SQS queue that will kick of asynchronous lambdas that will run them. The numbers of currently running and pending checks are displayed at the top of the Foursight UI. The code below defines the ``morning_checks`` schedule that will be used in the following steps.

.. code-block::

   @app.schedule(Cron(0, 11, '*', '*', '?', '*'))
   def morning_checks(event):
       queue_scheduled_checks('all', 'morning_checks')

This code will run all checks in check_setup.json using the ``morning_checks`` schedule on all Foursight environments every morning. For more information on scheduling, `see this documentation <./development_tips.md#scheduling-your-checks>`_.

Adding checks to check_setup
----------------------------

Let's say we've created two checks in the system_checks.py check module, named ``my_first_check`` and ``my_second_check``. To get these checks to run, we must create an entry for them in check_setup.json. For this example, we already have a schedule named ``morning_checks`` which was set up in the previous step. The first step is to add empty object entries in check_setup.json with keys that are EXACTLY equal to the names of our check functions. To these , add a string title and group. The group can be any string and is used to organize the checks on the UI.

.. code-block::

   {
       "my_first_check": {
           "title": "My first check",
           "group": "Awesome test checks"
       },
       "my_second_check": {
           "title": "My second check",
           "group": "Awesome test checks"
       }
   }

Now we need to add the schedule. Include a new key in each check entry called ``schedule`` and, under that, key another object with the names of the Foursight environments that you want the checks to run on. In this example, we use ``all``\ , which means the checks will run on every environment. Recall that ``morning_checks`` is the name of the schedule with a CRON that causes it to run at 6:00 am EST every day.

.. code-block::

   {
       "my_first_check": {
           "title": "My first check",
           "group": "Awesome test checks",
           "schedule": {
               "morning_checks": {
                   "all": {}
               }
           }
       },
       "my_second_check": {
           "title": "My second check",
           "group": "Awesome test checks",
           "schedule": {
               "morning_checks": {
                   "all": {}
               }
           }
       }
   }

Almost there! The last step is to add the parameters to the schedule for running the checks. In the innermost object in our JSON (currently keyed by ``all``\ ), we can specify dependencies that must be required for the check to run. This allows you to order the runs the checks within a schedule. So, if we wanted to ensure that ``my_second_check`` doesn't run until ``my_first_check`` is finished, we can leverage the ``dependencies`` field of ``my_second_check``. This field is simply a list of other check names that we the check to depend on. If you have no dependencies to specify, you may omit the field. Here is such a setup:

.. code-block::

   {
       "my_first_check": {
           "title": "My first check",
           "group": "Awesome test checks",
           "schedule": {
               "morning_checks": {
                   "all": {}
               }
           }
       },
       "my_second_check": {
           "title": "My second check",
           "group": "Awesome test checks",
           "schedule": {
               "morning_checks": {
                   "all": {
                       "dependencies": ["my_first_check"]
                   }
               }
           }
       }
   }

Lastly, you can also add specific key word arguments (\ ``kwargs``\ ) for running each check in each schedule/environment combination. If you do not specify ``kwargs``\ , the default ones for the check will be used. Arguments are input as an object under the ``kwargs`` at the same level that ``id`` and ``dependencies`` are defined. Let's say we wrote our ``my_first_check`` function to use a keyword called ``my_arg`` and we want to give it different values for running on the ``data`` and ``staging`` environments (both under the ``morning_checks`` schedule). The code below achieves this.

.. code-block::

   {
       "my_first_check": {
           "title": "My first check",
           "group": "Awesome test checks",
           "schedule": {
               "morning_checks": {
                   "data": {
                       "kwargs": {"my_arg": "some value"}
                   },
                   "staging": {
                       "kwargs": {"my_arg": "other value"}
                   },
               }
           }
       }

That's it! Now your check will automatically run with all other morning checks. The environments that you schedule your check for also determine where its results are displayed on the Foursight UI; for example, the setup we specified above will cause ``my_first_check`` to be displayed only on the ``data`` and ``staging`` environments. By default, the same setup is used for production and development Foursight.

Now that your check is built and scheduled, it can be run or retrieved using the Foursight API. This is the last topic covered in this file. For more information on configuring the check setup, `go here <./checks.md#check-setup>`_.

Using the UI
------------

The easiest way to interact with Foursight is through the UI, which allows viewing and running of checks. Here is `production Foursight <https://foursight.4dnucleome.org/view/all>`_ and here is `development Foursight <https://kpqxwgx646.execute-api.us-east-1.amazonaws.com/api/view/all>`_. Checks are presented in groups, as specified in ``check_setup.json``. Opening any group by clicking on it presents information on individual checks, which be further examined by clicking on the check title. If you have administrator privileges, you can log into your account and queue checks for running directly from the page. When doing this, you can adjust key word arguments for the check directly on the UI; this allows a high level of flexibility, including the choice to not overwrite the primary record for the check by setting ``primary`` to something else besides ``True``. Please note that running any checks requires either administrator privileges or a special authorization code.

For any individual check on the /view/ page, you can access the past history of the checks on the /history/ page: ``https://foursight.4dnucleome.org/history/<environ>/<check>``. This will give a paginated list of past runs for that check or action and displauy the status and key word arguments used to run the check. From there, individual results can be viewed in JSON format if you are logged in as admin.

Foursight API basics
--------------------

The most import endpoints are described below. The can be invoked from the command line, programmatically accessed, or visited in your browser. The Foursight address used below is the default address for the prod stage. The URLs are in form: ``<Foursight address>/api/<endpoint>/<environment>/<check>``.

**NOTE**\ : to hit endpoints from the command line you must provide the secret authorization string under the ``Authorization`` header. You can do so with the http library as shown below. Alternately, you can log in on the UI using the 4DN administrator account.

.. code-block::

   http https://foursight.4dnucleome.org/checks/data/my_test_checks 'Authorization: XXXXXXXXXXX'

The run endpoint with a GET request fetches the latest result for the given check on the given environment (\ ``data`` in this case).

.. code-block::

   GET https://foursight.4dnucleome.org/checks/data/my_test_checks

If you know the uuid of a check result you're interested in, you can get it with a GET request with the uuid as the last parameter.

.. code-block::

   GET https://foursight.4dnucleome.org/checks/data/my_test_checks/<uuid>

The run endpoint with a PUT request manually creates a check result for the given check using the PUT request body.

.. code-block::

   PUT https://foursight.4dnucleome.org/checks/data/my_test_checks

The view endpoint with a GET request returns a HTML visualization of the check results for the given environments. Environments may be a comma separated list (such as: ``staging,data``\ ) or ``all`` for all environments. The view endpoint is best used with a browser.

.. code-block::

   GET https://foursight.4dnucleome.org/view/all

   GET https://foursight.4dnucleome.org/view/staging,data
