# Foursight Actions #

This is an advanced topic. Make sure that you've read the [getting started](./getting_started.md) and [checks](./checks.md) documentation before going through this.

Actions are executable functions that can be linked to checks. These are useful because, in some cases, you want to take action on an issue identified by a check. Actions provide an easy way to do that, as they are written very similarly to checks and can be executed from the Foursight UI. Whereas checks are meant to be purely observational and not change any data within the target web application, actions are meant to execute meaningful changes to some part of the application.

Actions have a connection to the check result that was used to call them through the `called_by` key word argument. Using this, you can get the specific results from a certain check. This allows you to put any sort of data you want inside your check and be able to use that data within your action, as well. Since actions should only ever be used from the UI (or command line, with `called_by` supplied), you can write your actions to use this system.

There are a few key requirements to keep in mind when writing an action:
* Action functions must always start with the `@action_function()` decorator.
* Action results should be initialized with `init_action_res`, which works just like `init_check_res`.
* Action functions should return the action results (initialized with `init_action_res`).
* The name of the action function must exactly match the name passed to `init_action_res`.
* The name of the action function must exactly match the value of `check.action` to link a check to an action. Only one action can be linked to a check.
* To allow an action to be executed, use `check.allow_action = True`.
* Only users with administrator privileges can run actions.

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

### The 'called_by' key word argument
All actions are guaranteed to have the `called_by` key word argument; if they don't, the action will not run. This argument is the uuid of the check result that was used to call the action. A common operation within an action is to get the information of the check result that called it. This can be done like so, given a check with name `make_random_test_nums`:

```
<inside an action>
    my_check = init_check_res(connection, 'make_random_test_nums')
    check_data = my_check.get_result_by_uuid(kwargs['called_by'])
```

### Writing the action
Let's write an action that adds up all the numbers in the list within `full_output` for the result of the check that called it. It will be decorated with the `@action_function()` decorator and will be within one of our defined check module files. Note that actions can take key word arguments (kwargs) just like checks; our example below will demonstrate this with the `offset` value. Actions should return the action result object, just like checks return the check result.

```
@action_function(offset=0)
def add_random_nums(connection, **kwargs):
    action = init_action_res(connection, 'add_random_nums')

    # get the results from the check
    check = init_check_res(connection, 'make_random_test_nums')
    check_data = check.get_result_by_uuid(kwargs['called_by'])
    nums = check_data.get('full_output', [])

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

### Displaying action messages
When executing an action from the UI, a message will be shown before the action is run. Anything can be put in this message and it has a default value of `'Are you sure you want to run this action?'`. To change this message, set the `action_message` field on the check. This should probably happen around the place that `allow_action` is set to true. Using our example from above:

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

    # add a message showing something about the action
    check.action_message = 'Add up all of my numbers! They are: ' + str(output)

    check.full_output = output
    check.description = 'A test check'
    return check
```

### Viewing action results
The results of run actions can be seen using the `Toggle latest action` button in the linked check on the Foursight UI. This box will always show the most recent run of the linked action in JSON form.
