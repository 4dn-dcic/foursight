=========
foursight
=========


----------
Change Log
----------

3.3.4
=====

`PR 526: Dependency updates <https://github.com/4dn-dcic/foursight/pull/526/files>`_

* Bump foursight-core + dcicutils, and allow higher PyJWT versions for consistency with foursight-cgap

3.3.3
=====

* Fixes the FF build cluster value

3.3.2
=====
* Update to foursight-core 3.3.2 (and dcicutils 6.8.0).

3.3.1
=====

`PR 522: limit beddb reruns for reference files <https://github.com/4dn-dcic/foursight/pull/522>`_

* Bug fix: prevent automatic execution of bedtobeddb workflow on FileReference
  items when at least 2 previous runs exist.

3.3.0
=====
* Changes related editing user projects/institutions.
* Removed the trigger_codebuild_run check (in foursight-core now).

3.2.1
=====

`PR 519: Bug fix ont upd check <https://github.com/4dn-dcic/foursight/pull/519>`_

* bug fix for check_for_ontology_updates - request more of the file header to get version info

3.2.0
=====
* Changes related to support for running actions in Foursight React.

3.1.1
=====

`PR 515: Bug fix consistent rep info check <https://github.com/4dn-dcic/foursight/pull/515>`_

* The ``consistent_replicate_info`` badge check was updating messages for all
  linked items (e.g. protocols), when there was no need to. Bug fixed.
* Fix ``app_utils_obj`` import.
* Add ``microscope_configuration_master`` to the list of fields to compare.

3.1.0
=====
* Updated foursight-core version; changes there related to /accounts page.
* Moved lookup of check_setup.json (and accounts.json) to foursight-core,
  from foursight/chalicelib_fourfront/app_utils.py.

2.3.3
=====

`PR 514: Errored runs chk edit <https://github.com/4dn-dcic/foursight/pull/514>`_

* Edited the check for errored workflow runs to only report recent ones.

  * By default in the past 30 days. This can be modified using the ``days_back`` arg.
  * Use 0 to search all errored runs.

2.3.2
=====

`PR 510: Edit consistent replicate info check <https://github.com/4dn-dcic/foursight/pull/510>`_

* Edit the ``consistent_replicate_info`` badge check to print a more readable message.

2.4.0
=====
* Changes related to a new experimental /accounts page in Foursight which can show summary
  Foursight and Portal info from other AWS accounts. To take advantage of it there is an
  accounts.json file in the chalicelib_fourfront directory which contains a simple list
  of Foursight URLs for other AWS accounts. If this file is not present no harm.
  This file has been manually encrypted, since it contains internal URLs, using
  the ENCODED_AUTH0_SECRET value in the Global Application Configuration (GAC)
  in the AWS Secrets Manager. There are convenience poetry scripts, to encrypt
  and/or decrypt this file locally: encrypt-accounts-file, decrypt-accounts-file.
  Currently this file contains just URL for 4dn-dcic dev and prod.
  Again, this is experimental, and easily disabled (remove accounts.json).
* Moved lookup of check_setup.json (and accounts.json) to foursight-core,
  from foursight-cgap/chalicelib_cap/app_utils.py.

2.3.3
=====

`PR 514: Errored runs chk edit <https://github.com/4dn-dcic/foursight/pull/514>`_

* Edited the check for errored workflow runs to only report recent ones.

  * By default in the past 30 days. This can be modified using the ``days_back`` arg.
  * Use 0 to search all errored runs.

2.3.2
=====

`PR 510: Edit consistent replicate info check <https://github.com/4dn-dcic/foursight/pull/510>`_

* Edit the ``consistent_replicate_info`` badge check to print a more readable message.

2.3.1
=====

`PR 512: Add DNase Hi-C to insulation and compartment pipes <https://github.com/4dn-dcic/foursight/pull/512>`_

* Added DNase Hi-C to experiment types that compartment caller and insulation scores and boundaries pipelines will run on


2.3.0
=====

* Add check/action to run the Hi-C pipeline on HiChIP datasets.

2.2.0
=====

* Changes related to Foursight React.

  * Renamed chalicelib directory to chalicelib_fourfront.
  * Renamed target package (pyproject.toml) from chalicelib to chalicelib_fourfront.
  * Moved all Chalice routes to foursight-core (same with foursight-cgap).
  * Moved schedules to chalicelib_fourfront/check_schedules.py.
  * Using new schedule decorator from foursight_core.schedule_decorator.
  * Changed check_setup.json lookup (in chalicelib_fourfront/app_utils.py) to look
    for check_setup.json in the directory specified by the FOURSIGHT_CHECK_SETUP_DIR
    environment variable, if set, otherwise look in the local chalicelib_fourfront directory;
    and setup a fallback directory for this lookup to this local chalicelib_fourfront directory,
    which foursight-core will use if there is no (non-empty) check_setup.json in the specified directory.

2.1.2
=====

`PR 507: Check schedule edits <https://github.com/4dn-dcic/foursight/pull/507>`_

* Update check schedule to reduce the number of metadata-related checks running on
  staging and non-production environments.


2.1.1
=====

* Update ``check_status_mismatch`` to ignore higlass items linked to other_processed_files
  (both can have a status mismatch related to the Experiment Set).
* Update dependencies.


2.1.0
=====

* Added this CHANGELOG.rst.
* Spruced up Foursight UI a bit (virtually all in foursight-core but mentioning here).

  * New header/footer.
  
    * Different looks for Foursight-CGAP (blue header) and Foursight-Fourfront (green header).
    * More relevant info in header (login email, environment, stage).
    
  * New /info and /users page.
  * New /users and /users/{email} page.
  * New dropdown to change environments.
  * New logout link.
  * New specific error if login fails due to no user record for environment.
