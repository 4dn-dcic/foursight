# Development Tips #

This documentation is meant to help you get up and running writing checks for Foursight. It includes some useful tips and functions outside of the scope of the strictly necessary stuff contained in the [getting started](./getting_started.md) and [checks](./checks.md) documentation. First, we will go over the timeline of writing a new check module containing new checks and adding a new check group.

## Overall process for adding, testing, and scheduling a brand new check group
1. Create your new check module file in the `chalicelib` directory.
2. Write your checks within that file.
3. Add the check module to CHECK_MODULES within chalicelib/check_groups.py.
4. Add a new check group to CHECK_GROUPS within chalicelib/check_groups.py.
5. Do some testing of your new check group.
6. Schedule your new check group in app.py.
7. Deploy to Foursight `dev` and test it live. [See here](./deployment.md).

## Testing tips

### Manual testing of your check
Let's assume that you've already finished steps 1 through 4 in the list above (these are pretty much covered in the getting started and checks documentation). For step 5, it is recommended that you go into a local Python interpreter and run your check directly to ensure that it provides the output you want. Below is some code run from the root directory of this project that will outline manual testing of the `items_created_in_the_past_day` check contained within the `wrangler_checks` check module. The code below is run from the Python interpreter.

```
>>> import app
# create a Foursight connection to the 'mastertest' environment
>>> connection, _ = app.init_connection('mastertest')
# run your check using the run_check utility
# args are: FS connection object, string check name, dict of kwargs to run with
# it could be useful to add a break point within your check function to see what's happening
>>> app.run_check(connection, 'items_created_in_the_past_day', {})
# some possible output:
{'name': 'items_created_in_the_past_day', 'title': 'Items Created In The Past Day',
'description': 'No items have been created in the past day.', 'status': 'PASS',
'uuid': '2018-01-16T19:14:34.025445', 'extension': '.json', 'brief_output': None,
'full_output': {}, 'admin_output': None, 'ff_link': None, 'runnable': True}

# you can also run with kwargs...
>>> app.run_check(connection, 'wrangler_checks/items_created_in_the_past_day', {'item_type': 'File'})
```

It's important to note that if your check ends with the `check.store_result()` function, then the result will always get written to S3. If this is not desirable during your testing, either insert a break point before that function or omit it until testing is finished.

### Manual testing of your check group
Let's say you want to run a whole check group and not an individual check. There are two ways to test this: `app.run_check_group`, which will run your checks synchronously. Alternatively, you can use `app.queue_check_group`, which causes your checks to run synchronously. The first function is useful for testing but is limited by its speed and may actually timeout if the checks take too long to run. The second function is actually who scheduled check groups are run, but it is difficult to track output. Below are examples of both from the Python interpreter with the example check group named `my_test_checks`.

```
>>> import app
# create a Foursight connection to the 'mastertest' environment
>>> connection, _ = app.init_connection('mastertest')
# the code below will return the results from the checks
>>> app.run_check_group(connection, 'my_test_checks')

# queue_check_group takes the environment name directly (not connection)
>>> app.queue_check_group('mastertest', 'my_test_checks')
# once checks have run, you can check the foursight UI or endpoints for the results
```

### Some other testing notes
* By default, you will use the `dev` stage of Foursight from the Python interpreter and test.py. To change to `prod` (USE WITH CARE), use `app.STAGE = 'PROD'`.
* You can get the latest check group results using `app.get_check_group_latest(connection, name)` given a Foursight connection and a valid check group name.

### Scheduling your check group
Okay, so you've got a check group that you're confident in. To schedule it using a CRON or rate expression, go to the top of app.py and create a new scheduled function (leading with the `@app.schedule()` decorator). Two examples are below:

```
@app.schedule(Rate(1, unit=Rate.HOURS))
def one_hour_checks(event):
    for environ in list_environments():
        queue_check_group(environ, 'my_test_checks')
```

Or scheduling with a CRON expression... for more info, [see here](http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html).
```
# run at 10 am UTC every day
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def daily_checks(event):
    for environ in list_environments():
        queue_check_group(environ, 'my_test_checks')
```

It is easy to constrict the environments that a given check is run on in the `for` loop of the schedule function.
