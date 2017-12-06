# Getting Started #

Foursight provides insight into your application (namely, Fourfront) using AWS infrastructure.

## The big picture
Foursight is a Chalice application, which essentially means it is a combination of AWS Lambda functions that are linked to different endpoints through API gateway. It allows for scheduling of lambdas through Cloud Watch. Using Chalice makes it easy to deploy all these different resources with cohesive settings.

Foursight itself is based around the concepts of 'checks', which operate on the server(s) you set it up with. Each check is supposed to do some small-ish amount of computation, record results, and then store itself using AWS S3. The endpoints for the Chalice application (defined in app.py) determine the which checks are called, when results are fetched, and the associated scheduling. It also builds a simple front end using Jinja2 to visualize check results, though if you if you love JSON, viewing the endpoints directly is completely fine.

Checks are defined in individual files (called check modules) and grouped together into check groups, which are run as units. For example, if you made a bunch of checks that you wanted to run daily, you would create a group for these checks and schedule it to run on a CloudWatch CRON in app.py. Currently check groups are defined in check_groups.py. An example check module is system_checks.py. More details on how to write checks and check groups are below.
