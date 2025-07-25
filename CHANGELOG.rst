=========
foursight
=========


----------
Change Log
----------

4.9.28
==========
* update fastq checks to not run pipelines on pre-release status files that have not been uploaded as OK as they will become restricted
* md5run_uploaded, fastqc, fastq_first_line


4.9.27
==========
* modify missing raw file check to not add badge if files are restricted and missing as this is expected in some cases

4.9.26
==========
* only a version bump to allow tagging via tag-and-push and make release history clear in pypi

4.9.25
==========

* fix ui bug due to removal of action from check_subclass_short - add back 'empty' action
* refactor hi-c summary table check to use generator in search query

4.9.24
======

`PR 599: rm action reference <https://github.com/4dn-dcic/foursight/pull/599>`_

* remove reference to deleted action in check_subclass_short check


4.9.23
======

`PR 598: update lock file <https://github.com/4dn-dcic/foursight/pull/598>`_

* update poetry.lock


4.9.22
======

`PR 597: update badge check, assay_subclass_short check and rm action <https://github.com/4dn-dcic/foursight/pull/597>`_

* made queries more specific and adjusted the reported output for the badge checks
* simplified the check for assay_subclass_short and removed the action as it was trying to do too much for too little 

4.9.21
======

`PR 596: remove pdb <https://github.com/4dn-dcic/foursight/pull/596>`_

* remove pdb

4.9.20
======

`PR 595: pending lab bug fix <https://github.com/4dn-dcic/foursight/pull/595>`_

* fix bug (unbound local) in pending lab check that caused exception due to incorrect nesting


4.9.19
======

`PR 594: request header update <https://github.com/4dn-dcic/foursight/pull/594>`_

* Update header with new user agent info in the help section url check


4.9.18
======

`PR 593: bug fix <https://github.com/4dn-dcic/foursight/pull/593>`_

* Fix bug that cause exception in user with pending lab check if a property is missing.


4.9.17
======

`PR 592: bug fixes <https://github.com/4dn-dcic/foursight/pull/592>`_

* Adjust RNA-seq workflows to rely on benchmarking and adjust mergebed to avoid micro instances


4.9.16
======

`PR 591: bug fixes <https://github.com/4dn-dcic/foursight/pull/591>`_

* Let the higlass registration check not try to register files with a skip_processing tag
* Allow the action to run for the insulation and boundary caller but do not automatically queue it.


4.9.15
======

`PR 590: Deduplicate control TAs in ChIP-seq input JSON <https://github.com/4dn-dcic/foursight/pull/590>`_

* fix for opf status mismatch audit to deal correctly with highglass viewconf mismatch even if ignore tag was present
* fix to add ability to ignore tagged files unlinked to wfrs that should not be restricted from showing up in restricted pf check


4.9.14
======
Thug commit yet again to master branch to remove again dcicutils install from Makefile/publish-to-ga; AND
added poetry.toml with '[virtualenvs] / create = false'  like other repos; THINK this is the publish problem.


4.9.13
======
Thug commit to again to master branch to put back dcicutils install from Makefile/publish-to-ga,
buit with the latest (8.17.0) version; some lingering issue here with this to resolve.


4.9.12
======
Thug commit to master branch to remove unnecessary dcicutils install from Makefile/publish-to-ga.


4.9.11
======

* Update lock file to use a newer version of dcicutils to get around publish script issue due to change in github API


4.9.10
=====

`PR 588: Deduplicate control TAs in ChIP-seq input JSON <https://github.com/4dn-dcic/foursight/pull/588>`_

* For ChIP-seq sets with 2+ identical control TA files, replace multi-file ctl list with one-item list


4.9.9
=====

`PR 587: Remove restricted status from search query <https://github.com/4dn-dcic/foursight/pull/587>`_

* Remove the 'restricted' status from the search query in the fastq_first_line check
* Remove the 'restricted' status from the search query in the bamQC check (and add pre-release)
* will allow files that have not been uploaded to be set to 'restricted' status and not trigger runs of these checks


4.9.8
=====

`PR 586: Ignore non-mcools in insulation-score check <https://github.com/4dn-dcic/foursight/pull/586>`_

* The insulation score and boundaries caller check won't append a file to its output unless it is the mcool
* Add pre-released files to mcoolQC query

4.9.7
=====

`PR 585: Remove rate limit for md5_status check <https://github.com/4dn-dcic/foursight/pull/585>`_

* remove rate limiting (and related variables) for md5_status check


4.9.6
=====
* 2024-10-11/dmichaels
* Updated dcicutils version (8.16.1) for vulnerabilities.


4.9.5
=====

`PR 583: Update opf status mismatch check <https://github.com/4dn-dcic/foursight/pull/583>`_

* add a filter to filter on 'ignore_status_mismatch' tag on items (opfs, quality metrics or higlass_viewconfs) to ignore in opf status mismatch
* small update to search for bed2beddb files to respect the 'skip_processing' tag if present


4.9.4
=====

`PR 582: Refactor and bug fix for states file action <https://github.com/4dn-dcic/foursight/pull/582>`_

* refactor states file action once it was clear bug fix in previous PR was insufficient
* make use of s3Utils s3 access with update to read_s3 method in dcicutils 8.15.0


4.9.3
=====

`PR 581: Bug fix for states file action <https://github.com/4dn-dcic/foursight/pull/581>`_

* fix bug in states file action


4.9.2
=====

`PR 580: Bug fixes for 3 checks <https://github.com/4dn-dcic/foursight/pull/580>`_

* fixed 'grouped with' relationship check to ignore 'paired with' when needed
* tweak entrez_gene_id validation check to gracefully deal with no ncbi response
* refactor the update higlass_defaults check and action so it actually works 


4.9.1
=====

* Update entrez_gene_id validation check to play more nicely with ncbi
* add kwargs so gids can be added or removed from the ignore list - for those genes without any status live or other


4.9.0
=====

`PR 577: Get wf details from db not hard coding <https://github.com/4dn-dcic/foursight/pull/577>`_

* Refactor of wfr_utils to utilize properties and tags on updated schemas for workflow and experiment_type to generate info about accepted versions of workflows and pipelines
* Updated wfr_checks and wfr_encoded_checks to utilize the new info from db
* provided additional keyword args to override the accepted versions (useful for testing or some cases where re-runs of updated pipelines are needed)
* fix bug that allowed more than 2 errored fastq_first_line wfr runs
* update poetry lock


4.8.2
=====

* Increases EC2 utilized by select QC pipelines from t3.small to t3.medium
`PR 578: increase EC2 instance size for select QCs <https://github.com/4dn-dcic/foursight/pull/578>`_

4.8.1
=====

* Bug fix to remove 2 un-used variables from fastqc_status and fastq-first-line checks after removal of rate limiting


4.8.0
=====

* Support for Python 3.12.


4.7.3
=====

* Change clean_up_webdev_wfrs to run manually and correct test UUIDs
* fastqc and fastq-first-line checks are no longer rate limited
* Correct key used for fruit fly reference files and parameters
`PR 575: cleanup and remove rate limit for select checks <https://github.com/4dn-dcic/foursight/pull/575>`_

4.7.2
=====

* Exclude chromsizes files that are not `higlass_reference` from registration on the Higlass server.

4.7.1
=====

# Modify bam-processing accepted_versions list to enable workflow rerun
`PR 572: remove bam-processing accepted version for reruns <https://github.com/4dn-dcic/foursight/pull/572>`_

4.7.0
=====

# Add new version (0.3.0) of Hi-C pipeline and workflows to accepted versions
`PR 571: Hi-C update to 0.3.0 <https://github.com/4dn-dcic/foursight/pull/571>`_

4.6.0
=====
* Fix calls to get_es_metadata in checks/audit_checks.py to work when ES_HOST_LOCAL is set.
* Refactored item_status_mismatch check to run on fewer items if need be

`PR 570: Fix status mismatch check <https://github.com/4dn-dcic/foursight/pull/570>`_

4.5.0
=====
* Update Tibanna

4.4.6
=====
* Add organism when ATAC-seq check calls stepper helper

`PR 567: Add organism when calling stepper <https://github.com/4dn-dcic/foursight/pull/567>`_

4.4.5
=====
* Replace outdated file name for ChIP-seq ctl output bed in workflow settings

`PR 566: Fix for ChIP-seq ctl output assembly patching <https://github.com/4dn-dcic/foursight/pull/566>`_

4.4.4
=====
* wrangler_checks.py: action finalize_user_pending_labs removes external-lab by default

`PR 565: Action can remove pending lab external-lab <https://github.com/4dn-dcic/foursight/pull/565>`_

4.4.3
=====
* Add helper to convert user input str to list for select queries in higlass_checks.py
* Adjust output of check_validation_errors check to list affected items by type in full_output if not too many
* update lock file to use foursight-core with bug fix for local-check-execution script

`PR 564: Improved handling of user query for higlass items <https://github.com/4dn-dcic/foursight/pull/564>`_

4.4.2
=====
* Added 'input_bed' to attr_keys in wfr_utils.py's start_missing_run for ATAC-seq pipeline

`PR 563: Add ATAC-seq file key to attr_keys <https://github.com/4dn-dcic/foursight/pull/563>`_

4.4.1
=====
* updated check_setup to autoqueue chipseq check on data/prod
* Update lock file


4.4.0
=====
* Added update of a gitinfo.json file in GitHub Actions (.github/workflows/main-publish.yml).
* Update foursight-core with fix to Portal Reindex page (to not show initial deploy),
  and straighten out blue/green staging/data dichotomy on Reindex and Redeploy pages. 

4.3.0
=====
* Fix wfr_checks.md5run_status for bug where it was missing the first item in the result
  set because it was calling any() on a generator before iterating through it, which is
  destructive of the generator, i.e. causing to to move one item forwared.

4.2.2
=====

* modification of the biorxiv update check to squash a bug 
* if a doi is misformatted or contains an unwanted v# in it they are reported
  
`PR 560: Fix for biorxiv version update check bug <https://github.com/4dn-dcic/foursight/pull/560>`_

4.2.1
=====

* a refactor of the refactor to make more efficient
* will only check all combinations for similarity if the 'find_similar' parameter = True

`PR 559: another refactor doppelganger check <https://github.com/4dn-dcic/foursight/pull/559>`_

4.2.0
=====

* refactor of doppelganger check so it won't fail if ignore list becomes too long
* increased stringency for warning to case insensitive equality

`PR 558: refactor doppelganger check <https://github.com/4dn-dcic/foursight/pull/558>`_

4.1.4
=====

* bug fix to correct output of md5status check

`PR 555: bug fix for output of md5_status <https://github.com/4dn-dcic/foursight/pull/556>`_

4.1.3
=====

* additional improvement to md5status check to add option to limit number of files checked

`PR 555: add file limit option for md5_status <https://github.com/4dn-dcic/foursight/pull/555>`_

4.1.2
=====

* Minor UI fix to Ingestion page (foursight-core).

`PR 554: UI fix to core <https://github.com/4dn-dcic/foursight/pull/554>`_

4.1.1
=====

* Fix for md5_status check to allow checking all file metadata but only kickoff limited number of runs if too many files

4.1.0
=====

* New Portal Reindex page; foursight-core 5.1.0.
* Update poetry to 1.4.2.

4.0.3
=====

`PR 551: Upgrade foursight to run repliseq v16.1 <https://github.com/4dn-dcic/foursight/pull/551>`_

* configure Repli-seq pipeline to run v16.1, introducing additional output file

4.0.2
=====

`PR: 550: Add new expt type to assay_subclass_short dictionary <https://github.com/4dn-dcic/foursight/pull/550>`_

* Fixed a bug in the hi-c markdown table generation check.

4.0.1
=====

`PR:549: Fix bug in hi-c table generation check <https://github.com/4dn-dcic/foursight/pull/549>`_

* Fixed a bug in the hi-c markdown table generation check.
* also allow dataset_group field search when relevant

4.0.0
=====

* Update to Python 3.11.

3.9.0
=====

`PR 542: Google Analytics Data API v1 migration <https://github.com/4dn-dcic/foursight/pull/542>`_

* includes Google Reporting API v4 to Google Analytics Data API v1(beta) migration updates required for running sync_google_analytics_data check

3.8.3
=====

* fixed a bug in keyword args to not use hyphens which caused syntax error

3.8.2
=====

* add a non-dcic boolean option to BamQC and PairsQC to allow these workflows to run on lab provided files

3.8.1
=====

`PR 528: ChIP-seq update to 2.1.6 <https://github.com/4dn-dcic/foursight/pull/528>`_

* Modify wfr_encode_checks to run the updated (v2.1.6) ChIP-seq pipeline
* Update helpers (utils and settings) to run the modified check

3.8.0
=====

`PR 545: update dcicutils version <https://github.com/4dn-dcic/foursight/pull/545>`_

* update locked dcicutils version needed due to fourfront schema version updates

3.7.0
=====

`PR 543: rewrite sync_users_oh to remove pandas and numpy <https://github.com/4dn-dcic/foursight/pull/543>`_

* Removed dependency on pandas by refactoring code in wrangler_checks.py
  to use new convert_table_to_ordered_dict function in check_utils.py.

3.6.3
=====

`PR 541: add uploaded status to beta-actin count check <https://github.com/4dn-dcic/foursight/pull/541>`_

* update locked version of dcicutils to ^7.7.0

3.6.2
=====

`PR 540: add uploaded status to beta-actin count check <https://github.com/4dn-dcic/foursight/pull/540>`_

* small update to include fastq files with uploaded status that are linked to RNA-seq experiments to be checked for beta-actin counts in order to verify strandedness.

3.6.1
=====

`PR 539: badge bug fix <https://github.com/4dn-dcic/foursight/pull/539>`_

* Fixed a bug in the replicate set consistency badge check 

3.6.0
=====
* Changes (to foursight-core) to the access key check; making sure the action does not run every single day.

3.5.2
=====

`PR 538: Update checks that check for number of runs - rate limits output <https://github.com/4dn-dcic/foursight/pull/538>`_

* Adding info to brief output and WARN if the function that checks the number of runs over the past 6 hours indicates not to start new runs.

3.5.1
=====

`PR 535: Add new audit check for ChIP-seq target tags <https://github.com/4dn-dcic/foursight/pull/535>`_

* New check that makes sure that BioFeatures linked to ChIP-seq experiments as targets have the correct tag added

3.5.0
=====
* Changes in foursight-core (4.3.0) to fix access key check.

3.4.8
=====
* No difference between this (3.4.8) version and 3.4.7, except that 3.47 mistakenly was referring
  to the beta version of foursight (4.2.0.1b6) rather than the real non-beta version (4.2.0).

3.4.7
=====
* Fix to prepare_static_headers_Chromatin_Tracing in checks/header_checks.py from fix_sh_ct_dec branch.
* Minor UI fixes for display of status text for checks/actions - in foursight-core.
* Added UI warning for registered action functions with no associated check - in foursight-core.
* Added UI display of Redis info on INFO page - in foursight-core.
* Added a d default .chalice/config.json and removed this from .gitignore


3.4.6
=====
* small bug fix for assay_subclass_short check so new experiment_type gets right value

3.4.5
=====
* Update foursight-core 4.1.2.
  Fixes for check arguments not being converted (from string) to int/float/etc as
  appropriate in the React version only (was not calling query_params_to_literals).

3.4.4
=====
* Small update to assay_subclass_short update check to use new FISH assay_subclass_short as new value
* bug fix where an extra slash was added in url string to check against causing erroneus broken link to be reported

3.4.3
=====
* Added a new check in the header_checks.py for automate patching of FOF-CT static section for chromatin tracing datasets (Multiplexed FISH).

3.4.2
=====
* Version changes related to foursight-core changes for SSL certificate and Portal access key checking.
* Using new dcicutils.scripts.publish_to_pypi for publish.

3.3.5
=====

`PR 522: Add new params to ignore uuids or reset external expset with no pub check <https://github.com/4dn-dcic/foursight/pull/527>`_

* add 'uuids_to_ignore' parameter for a list of uuids to ignore and hence not warn for this check
* add 'reset_ignore' parameter to clear the list of uuids that are ignored

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
