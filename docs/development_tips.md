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
```

It's important to note that if you return the `check` at the end of your function, then the result will always get written to S3. In this case, it is probably best to not overwrite the primary check result when testing (which is the one shown on the Foursight UI). Since running a check will always overwrite the `latest` result, you can test safely by omitting the `primary=True` key word argument and fetching those results with `get_latest_result` method of your check result.

To overwrite the primary check result, you must set the `primary=True` key word argument for your check. If you want to, you can pass this dictionary into the `run_check_or_action` function:

```
# will overwrite the latest result for items_created_in_the_past_day, which won't display on the UI
app.run_check_or_action(connection, 'wrangler_checks/items_created_in_the_past_day', {})

# will overwrite the latest + primary results for items_created_in_the_past_day and display it on the UI
app.run_check_or_action(connection, 'wrangler_checks/items_created_in_the_past_day', {'primary': True})
```

If writing any results to S3 is not desirable during your testing, either insert a break point before that function or not return the check result.

### Testing on the UI
The Foursight UI is also a useful place to test your checks, since it is very easy to adjust the check key word arguments when signed in as admin. In this mode, setting the `primary` argument to anything but `True` will cause your test to run (and be available on the /history/ page) with overwriting the current primary result.

Just make sure to do your testing on the development stage of Foursight, which you can deploy to locally using `python -m deploy dev`. Please keep in mind that you will need to have some environment variables locally to make this work.

### Manual testing of your action
Actions function very similarly to checks when run individually. In fact, testing them is completely the same; the only difference is the different output. Below is some code that would test an action called `patch_file_size` in the `wrangler_checks` module.

**WARNING:** when manually running an action, be aware that it actually be executed on the given Foursight connection. For that reason, when in testing stages it is best to remove any impactful code within an action or insert a break point to have manual control.

```
>>> import app
# create a Foursight connection to the 'mastertest' environment
>>> connection, _ = app.init_connection('mastertest')
>>> app.run_check_or_action(connection, 'wrangler_checks/patch_file_size', {})
# some possible output:
{'name': 'patch_file_size','description': None, 'status': 'DONE',
'uuid': '2018-01-16T19:14:34.025445', 'output': []}

# you can also run with kwargs...
>>> app.run_check_or_action(connection, 'wrangler_checks/patch_file_size', {'some_arg': 'some_value'})
```

### Manual testing of your check group
Let's say you want to run a whole check group and not an individual check. To test this, you can use `app.queue_check_group`, which causes your checks to run synchronously. This function is the one that is internally used to schedule check groups for, but it is difficult to track output. For that reason, it may be easier to test with `run_check_or_action` as described above. Below are examples from the Python interpreter with the example check group named `my_test_checks`.

**NOTE:** if a check group has kwargs including `primary=True`, then the result will be written live to the Foursight UI. Omitting this argument when testing your check group may be desirable.

```
>>> import app
# queue_check_group takes the environment name directly (not connection)
# runs async; to see the results, see the Foursight UI, S3, or use Foursight API
>>> app.queue_check_group('mastertest', 'my_test_checks')
```

### Manual testing of your action group
Just like testing individual action functions is very similar to testing individual checks, testing action groups is almost the same as testing check groups. The only difference is you need to pass `use_action_group=True` into the `queue_check_group` function. Below is an example using the `patch_file_size` action group.

```
>>> import app
>>> app.queue_check_group('mastertest', 'patch_file_size', use_action_group=True)
# once checks have run, you can check the foursight UI or endpoints for the results
```

### Some other testing notes
* By default, you will use the `dev` stage of Foursight from the Python interpreter and test.py. To change to `prod` (USE WITH CARE), use `app.STAGE = 'PROD'`.
* You can get the latest check group results using `app.get_check_group_results(connection, name)` given a Foursight connection and a valid check group name.

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
