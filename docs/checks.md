# Foursight Checks #

Checks are the fundamental unit of work done in Foursight. They contain the entirety of code needed to make some observation or do some work and then take care of setting the result fields and storing the result. As mentioned in the [getting started](./getting_started.md) documentation, checks are written in files called check modules and are grouped together to run as check groups. This document contains information on writing checks, as well as best practices for running them.

It is assumed that you've already read the getting started documentation. If not, head over and check that out before continuing. If you are interested in tips on the check development process, [go here](./development_tips.md).

## Quick reference for important check requirements
* Checks must always start with the `@check_function()` decorator.
* Checks should always initialize their check results using the `init_check_res` function, with the first argument as the FS connection and the second argument as the **exact string name of the check**.
* There should NOT be two checks with the same name. The tests will fail if this happens.
* Attributes of the check result, such as status, are simply set like: `check.status = 'PASS'`.
* Checks should end by returning the value of the stored check result: `return check.store_result()`.

## Attributes you can set on a check result
The check result (i.e. the output of running `init_check_res`) has a number of important attributes that determine what is stored as output of your check. Below is a list of different fields you can set on your check result within the body of your check. As always, the results should stored at the end of the check by using `check.store_result()`. Any of the following attributes can be set like this:

```
check = init_check_res(connection, 'my_check_name')
check.status = 'PASS'
check.description = 'Test descritpion'
check.<other_attr> = <value>
...
return check.store_result()
```

Here is a list of attributes that you will routinely use, with brief descriptions:
* **status**: string value. Must be one of: 'PASS', 'WARN', 'FAIL', 'ERROR', or 'IGNORE', otherwise it will be set to 'ERROR' automatically. 'IGNORE' by default, which means the check result will not be displayed on the UI.
* **description**: string description of what the check is doing or explaining the output. Will be displayed on the UI.
* **brief_output**: Any value that will be displayed on the UI if set. The intended use of this attribute is as any output relevant to a check having a non-PASS status.
* **full_output**: same as brief_output, but is intended to hold the entirety of the check data.
* **admin_output**: same as brief_output, but is only visible to admins to view on the UI. Use for sensitive data.
* **ff_link**: a link to (presumably) Fourfront that will be displayed in the UI if provided. Should be relevant to the check.

Lastly, there are a number of attributes that are used internally. These do not usually need to be manually set, but can be.
* **s3_connection**: is set automatically when you use `init_check_res`.
* **name**: the string name of the check that should be exactly equal to the name of the function you want the result to represent.
* **title**: generated automatically from the name attribute unless it is set manually.
* **uuid**: this is explained further later in this document. The only reason to use this is if you want a check to be automatically populated by a previous result. Usually handled by passing in the `uuid` parameter to `init_check_res`.
* **extension**: the extension and format of the s3 object storing the check result. Is automatically set to `.json` and should not be changed.

## Our example check
Let's say we want to write a check that will check Fourfront for all items that were released in the past day, which we will do by leveraging the "date_created" field. A reasonable place for this check to live is chalicelib/wrangler_checks.py, since it is a metadata-oriented check. First, let's put down a barebones framework for our check using the `check_function` decorator and `init_check_res` to initialize the result for the check.

```
@check_function()
def items_created_in_the_past_day(connection, **kwargs):
    check = init_check_res(connection, 'items_created_in_the_past_day')
    return check.store_result()
```

At the moment, this check won't do anything but write a result to the `items_created_in_the_past_day` check directory, which will have some default values (namely status=ERROR). So, the body of the check can be thought of as doing the computation necessary to fill those fields of the check result. Let's use some helper functions defined in chalicelib/wrangler_utils.py to establish a connection to Fourfront (a FDN_Connection, such as those used in Submit4DN). If making a new check module, remember to import these helper functions. If the connection fails, store that information in the check result and abort the check. Let's also set a description and status for the check if the connection is established.

```
@check_function()
def items_created_in_the_past_day(connection, **kwargs):
    check = init_check_res(connection, 'items_created_in_the_past_day')
    fdn_conn = wrangler_utils.get_FDN_connection(connection)
    if not fdn_conn:
        check.status = 'ERROR'
        check.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return check.store_result()
    check.status = 'PASS'
    check.description = 'Working description.'
    return check.store_result()
```

Okay, now we have a check that will attempt to make a Fourfront connection and fail and provide helpful information if it can't. Next we need to get a search result from Fourfront and use those results within our check. The big idea is that we will iterate through the search results and see which items have a `date_created` value of less than a day ago. I'm going to go ahead and add a lot to the check and describe it afterwards.

```
@check_function()
def items_created_in_the_past_day(connection, **kwargs):
    check = init_check_res(connection, 'items_created_in_the_past_day')
    fdn_conn = wrangler_utils.get_FDN_connection(connection)
    if not fdn_conn:
        check.status = 'ERROR'
        check.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return check.store_result()

    ### let item_type = 'Item for now'
    item_type = 'Item'
    # date string of approx. one day ago in form YYYY-MM-DD
    date_str = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    search_query = ''.join(['/search/?type=', item_type, '&q=date_created:>=', date_str])
    search_res = ff_utils.get_metadata(search_query, connection=fdn_conn, frame='object')
    results = search_res.get('@graph', [])
    full_output = {}
    item_output = []
    for res in results:
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
    return check.store_result()
```

There are a couple funky things happening in the check above. First, the ff_utils package is an external package used by 4DN and uses the fdn_conn object we established earlier. The `get_metadata` function gets search results for all items of the type `item_type` with a date_created field greater than that of the current UTC time minus one day. We then iterate through those results and add them to the `full_output` dictionary, keyed by `item_type`. If any results were found, let's set the status to WARN and give a helpful description. If no items have been created in the past day, let's set the status to PASS to show that this check requires no attention.

This check is fully functional as written above, but it has a couple limitations. For example, it only operates on the `item_type` Item, which is the most generalized type of item and may cause a timeout in the lambda running this function if the resulting search result is very large. In the next section, we will use default check arguments and the check_group to further break down the check into different runs for different item types.

## Check arguments
A key word arguments (kwargs) object can be passed into your checks for internal use a couple ways. The first is through the `check_function` decorator. Any kwargs used in it's declaration will be available in the check. For example, the `item_type` variable in the check above would be better set as a default kwarg for the check as-so:

```
@check_function(item_type='Item')
def items_created_in_the_past_day(connection, **kwargs):
    ...
```

These kwargs defined in the check function can be overwritten by those defined in the check group. So, if we wanted to run the `items_created_in_the_past_day` check in a check group with `item_type = Experiment` we could add the following check info to a check group:

```
['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Experiment'}, [], 'dependency_id']
```

This will cause the `item_type` to be overwritten in the check code. If you wanted to use the default `item_type` kwarg, you would just leave an empty dictionary for the check in the check group:

```
['wrangler_checks/items_created_in_the_past_day', {}, [], 'dependency_id']
```

Lastly, arguments that are not defined in the default kwargs through the `check_function` decorator can also be added to the dictionary:

```
['wrangler_checks/items_created_in_the_past_day', {'another_arg': 'another_val'}, [], 'dependency_id']
```

This would execute the `items_created_in_the_past_day` check with the default kwarg `item_type=Item` and the provided `another_arg=another_val` kwarg. This system allows checks to be with different parameters in check groups.

Default kwargs are very important to set if they are required for a check, since there are instances in which your check can be run outside of a check group. In such a case, it may break if those arguments are not provided. Really, this is up to the user to design his or her checks in a robust way.

## Appending check results
Sometimes you may want the same check to run multiple times and report results from all of the runs. Some possible examples would be a long running check that is split up by item type. This can be acheived by initializing the check results (with `init_check_res`) and passing in a `uuid` parameter of a previously run check. This will initialize the new check with the stored attributes of the old check and then allow you to add to them in your check function.

For example, let's use the check that we've been demonstrating over the past couple sections. It finds all items of a certain type that have been created in the past day and takes an `item_type` key word argument that determines the type. In addition, the `full_output` attribute is a dictionary keyed by the item type. So, we can easily pull a previous result from that check (that ran for `item_type = Experiment`, for example) and add another item type (say, `Biosample`) to it. The desired `full_output` would have the following form:

```
{
    'Experiment': [ ... ],
    'Biosample': [ ... ]
}
```

To achieve this, we will use manipulate the `item_type` key word argument and initialize the check running for `Biosample` with the results of the previous check that used `Experiment`. All we need to do is change a couple lines from the `items_created_in_the_past_day` check that we defined above

First, add the `uuid` parameter to the `init_check_res` function. Read it from the kwargs. This will take care of initializing the check result with the attributes of the results of the check with the given uuid (if it exists).
```
init_uuid = kwargs.get('uuid')
check = init_check_res(connection, 'items_created_in_the_past_day', uuid=init_uuid)
```

Then, we just need to add the logic to use the `full_output` from previous results if available:
```
full_output = check.full_output if check.full_output else {}
```

Now our `full_output` results will be keyed correctly! The final step is to create a check group that coordinates the runs of these two checks. It is the default behavior that all checks in the same check group are given the same uuid. What this means is if you put multiple instances of a check in any one check group, you can always use this uuid trick to append results. In fact, if you use the same check multiple times and DON'T use the uuid argument in `init_check_res`, then the results will overwrite each other and the last instance of the check run will be the saved one.

Here is a check group that would work with our example:
```
'wrangler_test_checks': [
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Biosample'}, [], 'dependency_id_1'],
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Experiment'}, [], 'dependency_id_2']
]
```

## Accessing previous/other check results
Another possibility for a check is to operate on the previous results of the same or other checks. To get results for the same check, you can use the same CheckResult object that is defined using the check name at the beginning of the check:

```
check = init_check_res(connection, 'change_in_item_counts')
```

Using the CheckResult `check` object, you have access to all CheckResult methods, which include the `get_latest_check` and `get_closest_check` methods, which both return dictionary representations of those historic check results. Getting the latest check will always return the result with the "latest" tag, which is also the one displayed on the Foursight front end. The `get_closest_check` method can be used to get the check result that is closest the given time difference from the current time. See the example below:

```
check = init_check_res(connection, 'change_in_item_counts')
# get the latest dictionary result for this check
latest = check.get_latest_check()

# get the dictionary results for this result run closest to 10 hours, 30 mins ago
# args are in form (hours, minutes)
older = check.get_closest_check(10, 30)
```

The functions can be used to easily make a check that is aware of its own previous results. You can also make checks that use the results of other checks; to do this, define another check result object with the name of a different check. Consider the following example:

```
@check_function()
def change_in_item_counts(connection, **kwargs):
    # use this check to get the comparison
    check = init_check_res(connection, 'change_in_item_counts')
    counts_check = init_check_res(connection, 'item_counts_by_type')
    latest = counts_check.get_latest_check()
    # get_item_counts run closest to 24 hours ago
    prior = counts_check.get_closest_check(24)

    # now do something with the latest and prior dictionaries
    # and set the fields of check accordingly
```

This check would compare the latest result and the result run closest to 24 hours ago from the current time for `counts_check`. After any comparison is done, the fields of `check` would be set and finally `check.store_result()` would be called to save them.

## Making checks runnable from the UI
On the Foursight UI, users with administrator privileges can run individual checks directly, outside of the scope of a check group. When this is done, the check will run with the default kwargs that are within its `check_function()` decorator (hence the importance of those default arguments). Since not all checks are meant to be run individually, you must pass a `runnable=True` argument to `init_check_res` to achieve this functionality.

```
check = init_check_res(connection, 'change_in_item_counts', runnable=True)
```

## Check groups
As we have seen in the previous section, kwargs can be set individually for each check in the check group, allowing a high level of flexibility with what can be done even with a single check. There are a couple more important points to mention about check groups.

### Quick reference to important check groups requirements
* **All** check modules and check groups need to be declared/defined within check_groups.py.
* `CHECK_MODULES` is a list of the names of files with checks defined within them (without the ".py").
* `CHECK_GROUPS` is a dictionary keyed by the name of the check group. Check group names should always end in `_checks`.
* Check groups are lists of check entries that define parameters for the individual checks to run with. The first entry is the check module and check name (separated by a '/'), the second element is a dictionary of kwargs to pass to the check, the third element is a list of dependency IDs that must be finished for the check to run, and the final element is the string dependency ID of the check entry itself.
* All checks within the same check group will automatically have the same `uuid` key word argument available to them (see: Appending check results, above).

### Passing kwargs to checks
First, check groups containing the same check multiple times will overwrite the same output rather than writing an output for each check. Take the following check group, within the overall CHECK_GROUPS object:

```
'wrangler_test_checks': [
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Biosample'}, [], 'dependency_id_1'],
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Experiment'}, [], 'dependency_id_2'],
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'File'}, [], 'dependency_id_3']
]
```

This check group calls our example check three times, each with a different `item_type`. The expected outcome of running this check group should be one result that has information for all three types rather than multiple results that overwrite each other. Internally, this is done by setting the `uuid` key word argument internally whenever a check group is run with multiple occurrences of the same check.

This functionality also allows check groups to be dynamically made. Consider the following check group, which would run a check for each of the item types in the list below.

```
item_types = ['Biosample', 'Biosource', 'Experiment', 'File']
wrangler_test_checks = []
for idx, item_type in enumerate(item_types):
    wrangler_test_checks.append(
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': item_type}, [], 'dependency_id_' + str(idx)]
    )
```

### Dependencies in check groups
The last item in an entry within a check group is the dependency id, which is a string that allows other checks coordinate the order in which they run. Effectively, it allows you to ensure that some checks are run before others. This is important if you have a check that depends on the most up-to-date output of another check. To add a dependency to a check entry, add the dependency id of another check *within* the check group. For example, consider the following:

```
'wrangler_test_checks': [
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Biosample'}, [], 'wt1'],
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Experiment'}, ['wt1'], 'wt2'],
    ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'File'}, ['wt2'], 'wt3']
]
```

There are three dependency ids, `wt1`, `wt2`, and `wt3` that correspond to running the check for `item_type = Biosample`, `Experiment`, and `File`, respectively. We would set up the dependency IDs above if we want to guarantee that the check for `Biosample` runs first, followed by the check for `Experiment`, and lastly `File`. As seen from "Appending check results" section above, the check group as written will concatenate results from these three runs.

Another common example of specifying dependencies is below. Let's say we have a check named `uses_other_results` that needs to have the most up-to-date results from two other checks: `makes_other_results_1` and `makes_other_results_2`. Using unique dependency IDs for each entry in the check group, it could be set up like:

```
'example_dependent_checks: [
    ['wrangler_checks/makes_other_results_1', {}, [], 'example_id1'],
    ['wrangler_checks/makes_other_results_2', {}, [], 'example_id2'],
    ['wrangler_checks/uses_other_results', {}, ['example_id1', 'example_id2'], 'example_id3']
]'
```

### Implementing your check groups
All that you need to do to get your check group up and running is build it within the chalicelib/check_groups.py file. In the same file, make sure to add the string module name (without the `.py`) to the `CHECK_MODULES` list at the top of the file. That's all you need to do! Your check group can now be run using the following endpoint:

```
https://foursight.4dnucleome.org/api/run/<environment>/<my_check_group>
```

You can get the latest results for checks defined in your check group by running the GET command or visiting that same endpoint in your browser (you must be logged in as 4DN admin to run check groups outside of the schedule). Check groups can be run collectively from the Foursight UI with administrator privileges, as well.
