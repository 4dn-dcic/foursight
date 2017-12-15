# Getting Started #

Foursight provides insight into your application (namely, Fourfront) using AWS infrastructure.

## The big picture
Foursight is a Chalice application, which essentially means it is a combination of AWS Lambda functions that are linked to different endpoints through API gateway. It allows for scheduling of lambdas through Cloud Watch. Using Chalice makes it easy to deploy all these different resources with cohesive settings. Currently, Foursight is written in **Python 2.7**.

Foursight itself is based around the concepts of 'checks', which operate on the server(s) you set it up with. Each check is supposed to do some small-ish amount of computation, record results, and then store itself using AWS S3. The endpoints for the Chalice application (defined in app.py) determine the which checks are called, when results are fetched, and the associated scheduling. It also builds a simple front end using Jinja2 to visualize check results, though if you if you love JSON, viewing the endpoints directly is completely fine.

Checks are defined in individual files (called check modules) and grouped together into check groups, which are run as units. For example, if you made a bunch of checks that you wanted to run daily, you would create a group for these checks and schedule it to run on a CloudWatch CRON in app.py. Currently check groups are defined in check_groups.py. An example check module is system_checks.py. More details on how to write checks and check groups are below.

## Installing dependencies
Before developing with Foursight, you must install the required Python packages and set up your AWS credentials locally. It is best practice to use a virtual environment. Packages can be installed with pip using the following command from the root Foursight directory:

```
pip install -r requirements.txt
```

## Stages and environments
These are two important concepts to introduce. There are currently two Foursight stages, ```dev``` and ```prod```, which each define a separately deployed Chalice application. That means there is a different set of Lambdas, API Gateways, etc. for each stage. The reason to have multiple stages is so the one stage, ```dev```, can be used for testing new stuff while ```prod``` is for battle-tested checks. For information on how to deploy to specific stages, see the [deployment documentation](./deployment.md).

Foursight environments correspond to different Fourfront configurations and store their results separately of each other. For example, Fourfront has production and staging environments, which should have tests run individually on them. Environments are dynamically initialized for each Foursight API requests and are based off of items in S3. Both stages of Foursight have access to each individual environment, but the results for checks get stored separately. For example, there checks are stored separately for the production environment on dev stage and the production environment on prod stage. There would also be a unique bucket for checks on the staging environment on dev stage and the staging environment on prod stage, etc. Thus, there is a unique storage location for each combination of stage and environment. To read more about environments, see the [environments documentation](./environments.md).

## Creating a check
The most fundamental unit of Foursight is the check. These encapsulate code that will be run using AWS Lambda and will store results in S3, which can later be visualized and queried. For the following, it's assumed that you want to create a check within an existing check module (i.e. file containing checks). Such a file is chalicelib/system_checks.py.

Here's a simple check and then we'll go through the details:

```
@check_function()
def my_first_check(connection, **kwargs):
    check = init_check_res(connection, 'my_first_check')
    check.full_output = {'key1': 'ok_value', 'key2': 'warning_value'}
    check.brief_output = {'key2': 'warning_value'}
    # check status should be one of ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE']
    check.status = 'PASS'
    check.description = 'The first check I've ever made!'
    check.store_result()
```

The first thing to note is each check *must* start with the ```@check_function()``` decorator. This allows Foursight to determine which functions are checks. In addition, any default key word arguments (kwargs) that you want to define for a check can be passed into the decorator as parameters (more on this later). Next, the check itself must take a ```connection``` parameter and ```**kwargs```... the latter is not strictly necessary but should always be included as good form. Once adding the ```@check_function()``` decorator to a function, the function will be considered a check no matter what, and will be displayed on the Foursight front end.

In the body of the check, the first thing to do is initialize a CheckResult object (named ```check```, above) using the ```init_check_res``` function. The CheckResult is an object that internally takes care of things like check querying and storing of results in S3. It is key that ```init_check_res``` is **given the connection as its first argument and the exact name of the check function as its second argument.** That is worth repeating: for my check above, named "my_first_check", I must pass that exact string as the second argument to ```init_check_res```; otherwise, the check will be stored under a different name and impossible to retrieve using the automated front end.

The CheckResult has a number of fields that can be set, namely: ```status```, ```description```, ```full_output```, and ```brief_output```. These are the displayed values for the check. With the exception of status, they are all flexible and can be set to any value you choose within your check. Status must be one of: ```PASS```, ```WARN```, ```FAIL```, ```ERROR```, or ```IGNORE```. These fields determine how the check is displayed. ```full_output``` is generally considered the entire output of the check that you care about, whereas ```brief_output``` is the output relevant to the final status of the check. Consider the revised check, below:

```
@check_function()
def my_first_check(connection, **kwargs):
    check = init_check_res(connection, 'my_first_check')
    # custom fxn below. lets say we are worried if its output is < length 5
    check.full_output = my_function_to_get_all_results()
    if len(check.full_output) < 5:
        check.brief_output = 'Length is less than 5!'
        check.status = 'WARN'
    else:
        check.status = 'PASS'
    check.description = 'The first check I've ever made!'
    check.store_result()
```

Calling `check.store_result()` at the end of the check causes the result of the check to be written to S3 with a unique key created by the check name and time the check was initiated. In addition, each run of a check will overwrite the last "latest" check result, which is the one displayed from the Foursight front end. This is an important behavior of Foursight--the latest result is the one displayed.

There are many possibilities to what a check can do. Please visit the ```writing_checks.md``` document for more information.

## Adding a check group
Let's say we've created two check in the system_checks.py check module, named ```my_first_check``` and ```my_second_check```. We also have a third check named ```my_third_check``` in wrangler_checks.py. To get these checks to run as a cohesive unit, we need to create a check group for them. This is done within the chalicelib.check_groups.py file. Each item in a check group is a list with three elements. The first element is a string (called a check string) in the form ```<check_module>/<check_name>```, the second element is a dictionary of kwargs for the check, and the third element is a list of check strings dependencies within the check group. The dependencies would be used in the case that we want to wait for one check to finish before running another. Let's say we want some kwargs passed into ```my_first_check``` and we want ```my_third_check``` to run after ```my_second_check```. A check group would look like this, and are written as items in the CHECK_GROUPS dictionary in the check_groups.py file.

```
'my_test_checks': [
    ['system_checks/my_first_check', {'arg_key': 'arg_value'}, []],
    ['system_checks/my_second_check', {}, []],
    ['wrangler_checks/my_third_check', {}, ['system_checks/my_second_check']]
]
```

It is also important to note that you need to add any check modules (e.g. system_checks or wrangler_checks) to the list of ```CHECK_MODULES``` at the top of check_groups.py. As a side note, ```all``` is a reserved check group name that includes all checks within the application using the ```@check_function``` decorator without any explicit arguments or dependencies.

Now that your check group is defined, it can be run or retrieved using the Foursight API. This is the last topic covered in this file.

## Scheduling your check group
To get your check group running on a CRON or rate schedule, the current method is add it at the bottom of app.py. This will change in the near future, but here is an example using our demo check group.

```
@app.schedule(Rate(1, unit=Rate.HOURS))
def one_hour_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        connection, error_res = init_connection(environ)
        if connection:
            run_check_group(connection, 'my_test_checks')
```

This schedule will run ```my_test_checks``` on all Foursight environments every one hour. For more information on using a CRON scheduler, check out [this AWS page](http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html). The code above will run on all environments, but could be easily constricted to specific ones.

## Foursight API basics
The most import endpoints are described below. The can be invoekd from the command line, programatically accessed, or visited through your browser. The Foursight address used below is the default address for the prod stage. The URLs are in form: ```<Foursight address>/api/<endpoint>/<environment>/<check_group>```.

The run endpoint with a GET request fetches the latest results for the given check group on the given environment (```data``` in this case).
```
curl -X GET https://foursight.4dnucleome.org/api/run/data/my_test_checks
```

The run endpoint with a PUT request runs the check group on the given environment and returns the results.
```
curl -X PUT https://foursight.4dnucleome.org/api/run/data/my_test_checks
```

The view endpoint with a GET request returns a HTML visualization of the check results for the given environments. Environments may be a comma separated list (such as: ```staging,data```) or ```all``` for all environments. The view endpoint is best used with a browser.
```
curl -X GET https://foursight.4dnucleome.org/api/view/all

curl -X GET https://foursight.4dnucleome.org/api/view/staging,data
```
