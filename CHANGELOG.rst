=========
foursight
=========


----------
Change Log
----------

2.2.1
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
