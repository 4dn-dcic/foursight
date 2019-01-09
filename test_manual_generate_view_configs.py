# cp ../test_scripts_fourfront/test_script.py . && python test_script.py

from time import sleep

import app
connection = app.init_connection('mastertest')

from dcicutils import ff_utils

# GRCm38 Beddb: a31800d0-c275-425d-b610-3efd78bdde96
# GRCm38 chromsizes: bd0784a5-2a3d-42f0-ba9c-d9b3dc0539c6
# Step 1: Upload these two to mastertest, change whatever you need to DONE
beddb_uuid = "a31800d0-c275-425d-b610-3efd78bdde96"
beddb_higlassuid = "LgnwPjYoSQiIjWxJOTr5CA"
beddb_accession = "4DNFIH4I1MY4"

chromsize_uuid = "bd0784a5-2a3d-42f0-ba9c-d9b3dc0539c6"
chromsize_higlassuuid = "Z_YbBZW9RAq5V1oKvvp3hg"
chromsize_accession = "4DNFI3UBJ3HZ"

# GRCm38 mcool: a9bce30d-e6cf-44b7-b8dc-bd04dde1932f
# Step 2: Upload this mcool file
# higlass_uid: "SqT1f8vJSsC5aFUZW28-Qw"
mcool_uuid = "a9bce30d-e6cf-44b7-b8dc-bd04dde1932f"
mcool_higlass_uid = "SqT1f8vJSsC5aFUZW28-Qw"
mcool_accession = "4DNFI5TWFPOL"

def test1():
    """generate_higlass_view_confs_files
    """
    # Make sure file has higlass_uid and genome_assembly
    # Run generate_higlass_view_confs_files
    check_result = app.run_check_or_action(connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': "test_higlass_checks"})
    # File should be in target_files
    print("# File should be in target_files")
    print(check_result["full_output"]["target_files"]["GRCm38"])
    print(mcool_uuid)
    if mcool_uuid in check_result["full_output"]["target_files"]["GRCm38"]:
        print("YES, it's in target_files")
    else:
        print ("OH NO it's not! ABORT! ABORT!")
        return 1
    # Should see reference files
    print("# Should see reference files")
    print(check_result["full_output"]["reference_files"]["GRCm38"])
    print(beddb_uuid)
    print(chromsize_uuid)
    if beddb_uuid in check_result["full_output"]["reference_files"]["GRCm38"] and chromsize_uuid in check_result["full_output"]["reference_files"]["GRCm38"]:
        print("YES, they are in target_files")
    else:
        print ("OH NO it's not! ABORT! ABORT!")
        return 1
    print (check_result)

"""post_higlass_view_confs_files
"""
# Comment out patch_metadata call
# Run generate_higlass_view_confs_files and capture the output (especially the called_by)
# Run post_higlass_view_confs_files with the uuid in the called_by
# Because fourfront master hasn't been patched yet, the code will reject some of the formtypes
# Now modify code so to_post only sends the file and not the reference files
# You should see new_view_confs_by_file
# Now uncomment the patch_metadata call
# Run function again, you should see static_content with a new section for auto_generated_higlass_view_config
def test2():
    """If you need to delete the static content, try to PATCH {} (Empty body is important!)
    http://mastertest.4dnucleome.org/files-processed/a9bce30d-e6cf-44b7-b8dc-bd04dde1932f/?delete_fields=static_content
    """

    # TODO Comment out patch_metadata call
    # TODO Delete static content from file

    # Run generate_higlass_view_confs_files and capture the output (especially the called_by)
    check_result = app.run_check_or_action(connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': "test_higlass_checks"})
    check_uuid = check_result["uuid"]

    # Run post_higlass_view_confs_files with the uuid in the called_by
    action_result = app.run_check_or_action(connection, 'higlass_checks/post_higlass_view_confs_files', {'called_by': check_uuid})

    # Because fourfront master hasn't been patched yet, the code will reject some of the formtypes.
    print(action_result)
    print("# Because fourfront master hasn't been patched yet, the code will reject some of the formtypes.")
    if mcool_uuid in action_result["output"]["failed_post_files"]:
        print("They failed, did you comment out the chromsizes files?")
    else:
        # If you see this, either fourfront master was patched or you added the code below.
        print ("Didn't fail to post. Did you comment out the chromsizes files from getting posted?")

    # Now modify code so to_post only sends the file and not the reference files
    """
    to_post = {'files': reference_files + files}
    to_post = {'files': [] + files}

       Also, add:
                    for key in ('locationLocks', 'zoomLocks', 'editable', 'valueScaleLocks', 'exportViewUrl', 'trackSourceServers', 'zoomFixed', 'views'):
                        view_conf.pop(key, None)
    """

    # You should see new_view_confs_by_file
    print("# You should see new_view_confs_by_file")
    if mcool_uuid in action_result["output"]["new_view_confs_by_file"]:
        print("      YES, the mcool created another viewconf")
    else:
        print ("     NOPE, the mcool failed to create a viewconf. Did you forget to remove the auto_generated_higlass_view_config?")
        return 1

    new_view_conf_uuid = action_result["output"]["new_view_confs_by_file"][mcool_uuid]

    # Now uncomment the patch_metadata call
    # Run function again, you should see static_content with a new section for auto_generated_higlass_view_config
    file_res = ff_utils.get_metadata(mcool_uuid, key=connection.ff_keys, ff_env=connection.ff_env, check_queue=True)
    file_static_content = file_res.get('static_content', [])

    found_static_content = any(sc for sc in file_static_content if sc['description'] == 'auto_generated_higlass_view_config')

    print("# Run function again, you should see static_content with a new section for auto_generated_higlass_view_config")
    if found_static_content:
        print("     YES, autogenerated viewconf static content")
    else:
        print ("    NOPE, no autogenerated viewconf static content here. Uncomment the ff_patch command to add the auto_generated_higlass_view_config code?")
        return 1

    # Make sure we don't have to make a new viewconf anymore. Wait first so the changes can propagate.
    sleep(5)
    check_result = app.run_check_or_action(connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': "test_higlass_checks"})
    print(check_result)
    # File should NOT be in target_files
    if not "GRCh38" in check_result["full_output"]["target_files"]:
        print ("    YES, no need to generate static content when it's already made")
    elif mcool_uuid in check_result["full_output"]["target_files"]["GRCh38"]:
        print("     NOPE, the file should not regenerate, it already has static content")
        return 1
    else:
        print ("    YES, no need to generate static content when it's already made")

def test3():
    # Pass in a bogus file type, it should fail
    check_result = app.run_check_or_action(connection, 'higlass_checks/files_not_registered_with_higlass', {'called_by': "test_higlass_checks", 'filetype': 'BOGUS'})
    print("# Pass in a bogus file type, it should fail")
    if "Filetype must be one of:" in check_result["description"]:
        print ("     YES, it failed")
    else:
        print ("     NO, it didn't fail?")
        return 1

    # TODO PATCH some of the files so they don't have the higlass_uid (data body must be blank)
    # { }
    # http://mastertest.4dnucleome.org/files-reference/bd0784a5-2a3d-42f0-ba9c-d9b3dc0539c6/?delete_fields=higlass_uid
    # http://mastertest.4dnucleome.org/files-processed/a9bce30d-e6cf-44b7-b8dc-bd04dde1932f/?delete_fields=higlass_uid

    # TODO comment out the s3 check
    # make sure file exists on s3
    #if not connection.ff_s3.does_key_exist(file_info['upload_key'], bucket=typebucket):
    #    not_found_s3.append(file_info)
    #    continue

    # Run the check.
    check_result = app.run_check_or_action(connection, 'higlass_checks/files_not_registered_with_higlass', {'called_by': "test_higlass_checks"})

    # These files should appear in ready to register.
    print("# These files should appear in ready to register.")
    if any(info for info in check_result["full_output"]["files_not_registered"]["chromsizes"] if info['uuid'] == chromsize_uuid):
        print ("     YES, found the chromsizes file")
    else:
        print ("     NO, can't find the chromsizes file. Does it have a higlass_uid?")
        return 1

    if any(info for info in check_result["full_output"]["files_not_registered"]["mcool"] if info['uuid'] == mcool_uuid):
        print ("     YES, found the mcool file")
    else:
        print ("     NO, can't find the mcool file. Does it have a higlass_uid?")
        return 1

    # The other file has a higlass_uid so it should not need to be registered.
    print("# The other file has a higlass_uid so it should not need to be registered.")
    if any(info for info in check_result["full_output"]["files_not_registered"]["beddb"] if info['uuid'] == beddb_uuid):
        print ("     YES, can't find the beddb file. It must have a higlass_uid?")
    else:
        print ("     NO, why is the beddb file getting registered?")
        return 1

    # TODO Patch the beddb file so it's using a bogus higlass_uid
    # Check higlass. The beddb file is using a bogus id, so it has to be registered.
    print("# Check higlass. The beddb file is using a bogus id, so it has to be registered.")
    check_result = app.run_check_or_action(connection, 'higlass_checks/files_not_registered_with_higlass', {'called_by': "test_higlass_checks", "confirm_on_higlass": True})
    if any(info for info in check_result["full_output"]["files_not_registered"]["beddb"] if info['uuid'] == beddb_uuid):
        print ("     YES, the beddb file has a bogus higlass_uid. It is registered.")
    else:
        print ("     NO, the beddb file has a bad higlass_uid, it should be registered!")
        return 1

def test4():
    # TODO beddb, chromsize files should have a higlass_uid.
    # TODO mcool file should NOT have a higlass_uid.

    # TODO Prep the code like you did in test3 to avoid the Amazon S3 checks.

    # Run the check.
    check_result = app.run_check_or_action(connection, 'higlass_checks/files_not_registered_with_higlass', {'called_by': "test_higlass_checks", "confirm_on_higlass": True})
    check_uuid = check_result["uuid"]

    print("# These files should appear in ready to register.")
    if any(info for info in check_result["full_output"]["files_not_registered"]["chromsizes"] if info['uuid'] == chromsize_uuid):
        print ("     YES, found the chromsizes file")
    else:
        print ("     NO, can't find the chromsizes file. Does it have a higlass_uid?")
        return 1

    if any(info for info in check_result["full_output"]["files_not_registered"]["mcool"] if info['uuid'] == mcool_uuid):
        print ("     YES, found the mcool file")
    else:
        print ("     NO, can't find the mcool file. Does it have a higlass_uid?")
        return 1

    # Now run the action. Because these are test files we expect registration errors.
    action_result = app.run_check_or_action(connection, 'higlass_checks/patch_file_higlass_uid', {'called_by': check_uuid})
    print(action_result)

    print("# Now run the action. Because these are test files we expect registration errors.")
    if len(action_result["output"]['registration_failure']) > 0:
        print ("     YES, Tried to register test files on Higlass but it failed.")
    else:
        print ("     NO, no failures found. Did we try to patch?")

    # Make sure the patch_success shows the file accessions.
    print ("# Make sure the patch_success shows the file accessions.")
    if chromsize_accession in action_result["output"]['registration_failure']:
        print ("     YES, found the chromsizes file accession.")
    else:
        print ("     NO, can't find the chromsizes file accession. It didn't fail?")
        return 1

    if mcool_accession in action_result["output"]['registration_failure']:
        print ("     YES, found the mcool file accession.")
    else:
        print ("     NO, can't find the mcool file accession. It didn't fail?")
        return 1

# TODO
# Get an expset
#- expsets with processed_files with higlass_uid
#- expsets with other_processed_files with higlass_uid
# - in each case
# - - obtain a list of files, add appropriate reference files to the top

# ExpSet 4DNESOPFAAA1 has mcool and hic files
expset_uuid = "331106bc-8535-3338-903e-854af460b544"
expset_accession = "4DNESOPFAAA1"

# TODO: Add Human chromsize and Beddb files
human_beddb_uuid = "4a6d10ee-2edb-4402-a98f-0edb1d58f5e9"
human_beddb_accession = "4DNFI823LSII"

human_chromsizes_uuid = "9c78392f-d183-418f-8cbb-eafe544b9de0"
human_chromsizes_accession = "4DNFIWG6CQQA"

# This has multiple files:
# 4DNFIAAAAAA4 (cool file)
expset_cool_uuid = "8571c559-22b4-487e-9cb8-7ac402e93a9f"
expset_cool_acession = "4DNFIAAAAAA4"

# 4DNFIAAAAAA5 (hic file, GRCh38)
expset_hic_uuid = "5b58e449-b7f0-4120-b20b-634f19c67f9e"
expset_hic_accession = "4DNFIAAAAAA5"

# 4DNFIMCOOL01 (mcool file, GRCh38)
expset_mcool_uuid = "d273d710-6b6d-4e43-a84c-5658a891c032"
expset_mcool_accession = "4DNFIMCOOL01"

# Not processed files
# 4DNFIAAAAAA1 (pairs)
# 4DNFIAAAAAA2 (pairs)
# 4DNFIAAAAAA3 (pairs file)

# Test
# You want the ExpSet to have static content

def test5():
    # TODO Make sure ExpSet doesn't have static content

    # Make sure the mcool file doesn't have static content, too
    # http://mastertest.4dnucleome.org/files-processed/d273d710-6b6d-4e43-a84c-5658a891c032/?delete_fields=static_content
    # Run higlass check to make more
    check_result = app.run_check_or_action(connection, 'higlass_checks/generate_higlass_view_confs_files_for_expsets', {'called_by': "test_higlass_checks"})
    print(check_result)
    # ExpSet should be in target files
    print("# ExpSet should be in target files")

    if expset_uuid in check_result["full_output"]["target_files"]["GRCh38"]:
        print("YES, found the ExpSet")
    else:
        print("NO, where is the ExpSet? Does it still have static content? ")
        return 1

    # ExpSet should mention the files needed for higlass
    print("# ExpSet should mention the files needed for higlass")

    files_for_expset = check_result["full_output"]["target_files"]["GRCh38"][expset_uuid]

    if expset_mcool_uuid in files_for_expset:
        print("YES, target files contains the mcool file")
    else:
        print("NO, target files doesn't have the mcool file")
        return 1

    if expset_hic_uuid not in files_for_expset:
        print("YES, target files don't mention the hic file")
    else:
        print("NO, target files mentioned the hic file?")
        return 1

    # ExpSet should have reference files for GRCh38
    print("# ExpSet should have reference files for GRCh38")
    if human_chromsizes_uuid in check_result["full_output"]["reference_files"]["GRCh38"]:
        print("YES, chromsizes reference file was found")
    else:
        print("NO, where is the chromsizes reference file?")
        return 1

    if human_beddb_uuid in check_result["full_output"]["reference_files"]["GRCh38"]:
        print("YES, beddb reference file was found")
    else:
        print("NO, where is the beddb reference file?")
        return 1

def test6():
    # TODO Make sure ExpSet doesn't have static content
    # Make sure the mcool file doesn't have static content, too
    # http://mastertest.4dnucleome.org/files-processed/d273d710-6b6d-4e43-a84c-5658a891c032/?delete_fields=static_content
    # http://mastertest.4dnucleome.org/files-processed/a9bce30d-e6cf-44b7-b8dc-bd04dde1932f/?delete_fields=static_content
    # If fourfront master doesn't have the new code yet, comment out the code. See test2.

    # check to make sure the ExpSet wants to scan files.
    check_result = app.run_check_or_action(connection, 'higlass_checks/generate_higlass_view_confs_files_for_expsets', {'called_by': "test_higlass_checks"})

    # Now use the action to create view configs as needed.
    check_uuid = check_result["uuid"]
    action_result = app.run_check_or_action(connection, 'higlass_checks/post_higlass_view_confs_expsets', {'called_by': check_uuid})
    print(action_result)

    # The ExpSet should have a view conf.
    print("# The ExpSet should have a view conf.")
    if expset_uuid in action_result["output"]["new_view_confs_by_file"]:
        print("YES, the ExpSet was worked on")
    else:
        print("NO, the ExpSet was ignored. Did you patch out the code? Does ExpSet have a static content already? ")
        return 1

    # The ExpSet should have a static content section.
    file_res = ff_utils.get_metadata(expset_uuid, key=connection.ff_keys, ff_env=connection.ff_env, check_queue=True)
    file_static_content = file_res.get('static_content', [])

    found_static_content = any(sc for sc in file_static_content if sc['description'] == 'auto_generated_higlass_view_config')

    print("# The ExpSet should have a static content section.")
    if found_static_content:
        print("     YES, autogenerated viewconf static content")
    else:
        print ("    NOPE, no autogenerated viewconf static content here.")
        return 1

    # Make sure we don't generate the static_content again.
    sleep(5)
    stall_for_updates = ff_utils.get_metadata(expset_uuid, key=connection.ff_keys, ff_env=connection.ff_env, check_queue=True)
    check_result = app.run_check_or_action(connection, 'higlass_checks/generate_higlass_view_confs_files_for_expsets', {'called_by': "test_higlass_checks"})
    print(check_result)
    print("# Make sure we don't generate the static_content again.")
    if "GRCh38" not in check_result["full_output"]["target_files"]:
        print("YES, no need to generate new static content if it's already made")
    elif expset_uuid not in check_result["full_output"]["target_files"]["GRCh38"]:
        print("YES, the ExpSet is not targetted.")
    else:
        print("NO, we're trying to generate another view conf for the ExpSet")
        return 1
test6()
