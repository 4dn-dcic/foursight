# foursight #

A serverless chalice application to monitor and run tasks on [Fourfront](https://github.com/4dn-dcic/fourfront). Essentially, the app provides an number of endpoints to run checks, fetch results, dynamically create environments to check, and more.

## Beta version

Foursight is under active development and features will likely change.

To fetch latest results for all checks on a specific FF environment:
```
GET <foursight_server>/api/latest/<environment>/all
```

To run all checks on a specific FF environment:
```
GET <foursight_server>/api/run/<environment>/all
```

Other endpoints are available and are being added as needed, so check back soon.
