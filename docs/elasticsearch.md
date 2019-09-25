# ElasticSearch README #

As mentioned in [getting_started](./getting_started.md), Foursight maintains two connections - one to AWS S3 and another to an Elasticsearch instance, also running on AWS. The idea is that we use Elasticsearch to optimize certain Foursight requests such as getting the main and history pages, which are very slow when grabbing all of the data sequentially from S3. S3 serves as a backup for Elasticsearch. You can think of ES as if it is an optimized cache for S3. All `GETs` will first check ES and then go to S3 if the given ID is not found.

## ES Mapping

The current mapping can be found in [mapping.json](../chalicelib/mapping.json). At this time the mapping is simply a modified version of the dynamic mapping, which is automatically disabled when creating indexes using ES connection. This mapping is subject to change.

## ES Migration

The primary method for migration checks from S3 to ES is to use [migration.py](../migration.py). When run with no arguments, this script will migrate all checks from S3 to ES. This script makes use of a check that can be found in [es_checks.py](./checks/es_checks.py).

## ES Connection

The ES Connection object can be found in [es_connection.py](../chalicelib/es_connection.py) with associated tests in [test_es_connection.py](../tests/test_es_connection.py). It implements the AbstractConnection 'interface' defined in [abstract_connection.py](../chalicelib/abstract_connection.py). `RunResult` has been refactored to utilize this connection to post all results to both S3 and ES. It will by default check ES first and fallback to S3.