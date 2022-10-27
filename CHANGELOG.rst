=========
foursight
=========


----------
Change Log
----------

2.2.0
=====

* Changes related to Foursight React.
  * Moved all Chalice routes to foursight-core; same with foursight;
    and app-cgap and app-fourfront.py in 4dn-cloud-infra.

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
