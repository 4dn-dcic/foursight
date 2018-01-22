# Foursight Actions #

This is an advanced topic. Make sure that you've read the [getting started](./getting_started.md) and [checks](./checks.md) documentation before going through this.

Actions are executable functions that can be linked to checks. These are useful because, in some cases, you want to take action on an issue identified by a check. Actions provide an easy way to do that, as they are written very similarly to checks and can be executed from the Foursight UI. Whereas checks are meant to be purely observational and not change any data within the target web application, actions are meant to execute meaningful changes to some part of the application.

There are a few key requirements to keep in mind when writing an action:
* Action functions must always start with the `@action_function()` decorator.
* Action results should be initialized with `init_action_res`, which works just like `init_check_res`.
* Action functions should return the action results (initialized with `init_action_res`).
* The name of the action function must exactly match the name passed to `init_action_res`.
* The name of the action function must exactly match the value of `check.action` to link a check to an action. Only one action can be linked to a check.
* To allow an action to be executed, use `check.allow_action = True`.
* You must defined a group with `ACTION_GROUPS` (defined in check_groups.py) that has the same name as the action function.
* Only users with administrator privileges can run actions. Actions cannot be run when there are already items on the run queue, which prevents redundant copies of the same action to mistakenly queued.

Action results work much like check results, but have a smaller number of attributes that they use. Here are the ones you should know:
* **status**: string value. Must be one of: 'DONE', 'PEND', or 'FAIL', otherwise it will be set to 'FAIL' automatically.
* **description**: string description of what the check is doing or explaining the output. Will be displayed on the UI.
* **output**: Any value containing results relevant to the execution of the action.

Like check results, there are also a number of fields that are used internally:
* **kwargs**: the key word arguments used to set the check. Should be set automatically.
* **s3_connection**: is set automatically when you use `init_action_res`.
* **name**: the string name of the action that should be exactly equal to the name of the function you want to execute.
* **uuid**: timestamp of the action.

This document elaborates on the points above and illustrates how to make an action and link it to a check.

## Writing an action

### Getting the check set up
Before we write an action, we should set up a check to work with it. Let's write a stupid check that doesn't do anything useful- it will store a list of randomly generated integers in a list and store it as the `full_output`.

```
@check_function()
def make_random_test_nums(connection, **kwargs):
    check = init_check_res(connection, 'make_random_test_nums')
    check.status = 'IGNORE'
    output = []
    for i in range(random.randint(1,20)):
        output.append(random.randint(1,100))
    check.full_output = output
    check.description = 'A test check'
    return check
```

### Writing the action
Let's write an action that adds up all the numbers in the list within `full_output` for the most recent result of the check we just wrote. It will be decorated with the `@action_function()` decorator and will be within one of our defined check module files. Note that actions can take key word arguments (kwargs) just like checks; our example below will demonstrate this with the `offset` value. Actions should return the action result object, just like checks return the check result.

```
@action_function(offset=0)
def add_random_nums(connection, **kwargs):
    action = init_action_res(connection, 'add_random_nums')

    # get the latest result for the check we just made
    check = init_check_res(connection, 'make_random_test_nums')
    check_latest = check.get_latest_result()
    nums = check_latest.get('full_output', [])

    # add up the numbers from the check and add the kwarg 'offset' value
    total = sum(nums) + kwargs.get('offset', 0)

    # set fields on the action result and return it
    action.output = total
    action.status = 'DONE'
    action.description = 'A test action'
    return action
```

### Linking the action to the check
Now that we've written both the check and action functions, it's time to create the connection between the two. This is done using two fields on the check result, namely `action` and `allow_action`. Add these lines to the check function that we wrote a little while back.

```
@check_function()
def make_random_test_nums(connection, **kwargs):
    check = init_check_res(connection, 'make_random_test_nums')
    check.status = 'IGNORE'
    output = []
    for i in range(random.randint(1,20)):
        output.append(random.randint(1,100))

    # these lines link the action
    check.action = 'add_random_nums'
    check.allow_action = True

    check.full_output = output
    check.description = 'A test check'
    return check
```

It's critical that the value of `check.action` is *exactly* the same as the name of the action function. Setting `check.allow_action` to True allows the action to be run from the Foursight UI; if it's value is not set to True (default False), the action will be viewable from the UI but will not be able to be executed. This allows fine control of situations that the action can actually be run. For example, one possible scenario is that we only want to allow the action to be run if the status of its linked check is `FAIL` or `WARN`.

### Defining the action group
The last thing we need to do to get this action working is to make an action group entry in the `ACTION_GROUPS` dictionary within check_groups.py. This is required because what actually happens when an action is run is all of the entries from the relevant action group are added to the check runner queue. The name of the action group entry **MUST** be exactly the same as the name of the action function (which is also what we set `check.action` equal to). Below is a possible entry to action group to go with our example. Note how the name of the action group--**add_random_nums**--is the same as the name of the action function and value of the `check.action` in the most recent version of our check function.

```
'add_random_nums': [
    ['test_checks/make_random_test_nums', {}, [], 'tag1'],
    ['test_checks/add_random_nums', {}, [], 'tag2']
],
```

Action groups allow you to bundle other checks (and possibly actions) together to be run when your action is executed. Just like using check groups, you can add kwargs and dependencies to these entries within the action group. In the example above, the action group will run the action itself (`add_random_nums`) and our check (`make_random_test_nums`).

Let's say we want to make the action use a key word argument (`offset = 5`) and also run after the `make_random_test_nums` check is run. In addition, we want to run `make_random_test_nums` again after the action is finished. We would then set up our action group like this:

```
'add_random_nums': [
    ['test_checks/make_random_test_nums', {}, [], 'tag1'],
    ['test_checks/add_random_nums', {'offset': 5}, ['tag1'], 'tag2'],
    ['test_checks/make_random_test_nums', {}, ['tag2'], 'tag3']
],
```

Using techniques like the one above, you can make arbitrarily complicated combinations of actions and checks. One important use case is having a check run again after an action completes if the action would change the output of the check. This is especially important if a user might trigger the action another time based on the output of the check itself. A concrete example is a check that identifies all items in your system missing a certain field and an action that uses those check results to patch that field for all items. You would want to re-run the check after executing the action so that the UI displays the most up-to-date results for the check and so that nobody would accidentally run the action again after it had already been run.

**Note:** You don't need to set `primary = True` as a key word argument for actions. The most recent action run will always be the one displayed on the UI (see the next section).

### Viewing action results
The results of run actions can be seen using the `Toggle latest action` button in the linked check on the Foursight UI. This box will always show the most recent run of the linked action in JSON form.

### Final tips
Actions are queued just like checks, using the `queue_check_group` function. They are also run individually the same way as checks, using the `run_action_or_check` function. See the [development tips](./development_tips.md) documentation for more information.
