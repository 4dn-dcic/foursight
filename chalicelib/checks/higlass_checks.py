from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
import requests
import json
import time

def does_processed_file_have_auto_viewconf(hg_file):
    """
    See if the file has an auto generated higlass view config in its static contents.

    Args:
        hg_file(bool): A dict representing the file with static content.

    Returns:
        True if the file has an auto generated higlass view config, False otherwise
    """
    # - registered files will have a static_content item with description 'auto_generated_higlass_view_config'.

    # it might be better to check the static_content.location instead...
    sc_descrips = [sc.get('description') for sc in hg_file.get('static_content', [])]
    if 'auto_generated_higlass_view_config' in sc_descrips:
        return True
    return False

def get_reference_files(connection):
    """
    Find all of the tagged reference files needed to create Higlass view configs.

    Args:
        connection: The connection to Fourfront.

    Returns:
        Returns a dictionary of reference files.
            Each key is the genome assembly (examples: GRCm38, GRCh38)
            Each value is a list of uuids.
    """
    # first, find and cache the reference files
    reference_files_by_ga = {}
    ref_search_q = '/search/?type=File&tags=higlass_reference'
    ref_res = ff_utils.search_metadata(ref_search_q, key=connection.ff_keys, ff_env=connection.ff_env)
    for ref in ref_res:
        if 'higlass_uid' not in ref or 'genome_assembly' not in ref:
            continue
        # file_format should be 'chromsizes' or 'beddb'
        ref_format = ref.get('file_format', {}).get('file_format')

        if ref_format not in ['chromsizes', 'beddb']:
            continue
        # cache reference files by genome_assembly
        if ref['genome_assembly'] in reference_files_by_ga:
            reference_files_by_ga[ref['genome_assembly']].append(ref['uuid'])
        else:
            reference_files_by_ga[ref['genome_assembly']] = [ref['uuid']]
    return reference_files_by_ga

def post_viewconf_to_visualization_endpoint(connection, reference_files, files, ff_auth, headers):
    """
    Given the list of files, contact fourfront and generate a higlass view config.
    Then post the view config.
    Returns the viewconf uuid upon success, or None otherwise.

    Args:
        connection              : The connection to Fourfront.
        reference_files(dict)   : Reference files, stored by genome assembly (see get_reference_files)
        files(list)             : A list of file identifiers.
        ff_auth(dict)           : Authorization needed to post to Fourfront.
        headers(dict)           : Header information needed to post to Fourfront.

    Returns:
        A dictionary:
            view_config_uuid: string referring to the new Higlass view conf uuid if it succeeded. None otherwise.
            error: string describing the error (blank if there is no error.)
    """
    # start with the reference files and add the target files
    to_post = {'files': reference_files + files}

    view_conf_uuid = None
    # post to the visualization endpoint
    ff_endpoint = connection.ff_server + 'add_files_to_higlass_viewconf/'
    res = requests.post(ff_endpoint, data=json.dumps(to_post),
                        auth=ff_auth, headers=headers)

    # Handle the response.
    if res and res.json().get('success', False):
        view_conf = res.json()['new_viewconfig']

        # Post the new view config.
        viewconf_description = {
            "genome_assembly": res.json()['new_genome_assembly'],
            "viewconfig": view_conf
        }
        try:
            viewconf_res = ff_utils.post_metadata(viewconf_description, 'higlass-view-configs',
                                                  key=connection.ff_keys, ff_env=connection.ff_env)
            view_conf_uuid = viewconf_res['@graph'][0]['uuid']
            return {
                "view_config_uuid": view_conf_uuid,
                "error": ""
            }
        except Exception as e:
            return {
                "view_config_uuid": None,
                "error": str(e)
            }
    else:
        if res:
            return {
                "view_config_uuid": None,
                "error": res.json()["errors"]
            }

        return {
            "view_config_uuid": None,
            "error": "Could not contact visualization endpoint."
        }

def add_viewconf_static_content_to_file(connection, item_uuid, view_conf_uuid, sc_location):
    """
    Add some static content for the item that shows the view config created for it.
    Returns True upon success.

    Args:
        connection          : The connection to Fourfront.
        item_uuid(str)      : Identifier for the item.
        view_conf_uuid(str) : Identifier for the Higlass view config.
        sc_location(str)    : Name for the new Static Content's location field.

    Returns:
        boolean. True indicates success.
        string. Contains the error (or an empty string if there is no error.)
    """

    # requires get_metadata to make sure we have most up-to-date static_content
    file_res = ff_utils.get_metadata(item_uuid, key=connection.ff_keys, ff_env=connection.ff_env, add_on="frame=raw")
    file_static_content = file_res.get('static_content', [])

    # If the viewconf was already added, don't add it again.
    for sc in file_static_content:
        if sc.content == view_conf_uuid:
            return True, "static content already exists"

    new_view_conf_sc = {
        'location': sc_location,
        'content': view_conf_uuid,
        'description': 'auto_generated_higlass_view_config'
    }
    new_static_content = file_static_content + [new_view_conf_sc]
    try:
        ff_utils.patch_metadata(
            {'static_content': new_static_content},
            obj_id=item_uuid,
            key=connection.ff_keys,
            ff_env=connection.ff_env
        )
    except Exception as e:
        return False, str(e)
    return True, ""

def create_view_config_and_patch_to_file(connection, reference_files, target_file, source_files, ff_auth, headers, sc_location):
    """
    Take the reference_files and source_files to create a new Higlass view config. Then post the view config and patch the target_file so refers to the view config in its static content.

    Args:
        connection              : The connection to Fourfront.
        reference_files(dict)   : Reference files, stored by genome assembly (see get_reference_files)
        target_file(str)        : Use this identifier to find the file to associate the view config file to.
        source_files(list)      : A list of file identifiers used to create the new view config.
        ff_auth(dict)           : Authorization needed to post to Fourfront.
        headers(dict)           : Header information needed to post to Fourfront.
        sc_location(str)        : Name for the new Static Content's location field.

    Returns:
        Dictionary containing:
        success(boolean): Return True if higlass viewconf was posted and patched.
        view_conf_uuid(string): The new view config uuid. (may be None if there was an error)
        error(string): A string showing the type of error
    """
    # Create a new view config based on the file list and reference files.
    post_info = post_viewconf_to_visualization_endpoint(connection, reference_files, source_files, ff_auth, headers)
    view_conf_uuid = post_info["view_config_uuid"]
    post_error = post_info["error"]

    # If no view conf was generated, an error occured while creating the view conf.
    if post_error:
        return {
            "success": False,
            "view_conf_uuid": None,
            "error" : 'failed_post_file: ' + post_error,
        }

    # now link the view conf to the file with a patch
    success, patch_error = add_viewconf_static_content_to_file(connection, target_file, view_conf_uuid, sc_location)
    if success:
        return {
            "success": True,
            "view_conf_uuid": view_conf_uuid,
            "error" : 'message during patch: ' + patch_error,
        }
    return {
        "success": False,
        "view_conf_uuid": None,
        "error" : 'failed_patch_file: ' + patch_error,
    }

@check_function()
def generate_higlass_view_confs_files(connection, **kwargs):
    """
    Check to generate Higlass view configs on Fourfront for appropriate files

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        check results object.
    """
    check = init_check_res(connection, 'generate_higlass_view_confs_files')
    check.full_output = {}  # we will store results here
    # associate the action with the check. See the function named
    # 'post_higlass_view_confs' below
    check.action = 'post_higlass_view_confs'

    # first, find and cache the reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # if we don't have two reference files for each genome_assembly, fail
    if any([len(reference_files_by_ga[ga]) != 2 for ga in reference_files_by_ga]):
        check.status = 'FAIL'
        check.summary = check.description = "Could not find two reference files for each genome_assembly. See full_output"
        return check

    # next, find the files we are interested in (exclude reference files)
    target_files_by_ga = {}
    search_query = '/search/?type=File&higlass_uid!=No+value&genome_assembly!=No+value''&tags!=higlass_reference'
    search_res = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
    for hg_file in search_res:
        # Skip the file if it has previously been registered by Foursight.
        if does_processed_file_have_auto_viewconf(hg_file):
            continue

        if hg_file['genome_assembly'] in target_files_by_ga:
            target_files_by_ga[hg_file['genome_assembly']].append(hg_file['uuid'])
        else:
            target_files_by_ga[hg_file['genome_assembly']] = [hg_file['uuid']]

    check.full_output['target_files'] = target_files_by_ga
    check.status = 'PASS'

    if not target_files_by_ga:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
    else:
        all_files = sum([len(target_files_by_ga[ga]) for ga in target_files_by_ga])
        check.summary = "Ready to generate %s Higlass view configs" % all_files
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True  # allow the action to be run
    return check

@action_function()
def post_higlass_view_confs_files(connection, **kwargs):
    """ Action that is used with generate_higlass_view_confs_files to actually
    POST new higlass view configs and PATCH the old files.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object.
    """
    action = init_action_res(connection, 'post_higlass_view_confs')
    action_logs = {'new_view_confs_by_file': {}, 'failed_post_files': [],
                   'failed_patch_files': []}

    # get latest results
    gen_check = init_check_res(connection, 'generate_higlass_view_confs_files')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
    else:
        gen_check_result = gen_check.get_primary_result()

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}

    # pointer to the reference files (by genome_assembly)
    ref_files_by_ga = gen_check_result['full_output'].get('reference_files', {})

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # these are the files we care about
    # loop by genome_assembly
    target_files_by_ga = gen_check_result['full_output'].get('target_files', {})
    for ga in target_files_by_ga:
        if time_expired:
            break
        if ga not in ref_files_by_ga:  # reference files not found
            continue
        for file in gen_check_result['full_output']['target_files'][ga]:
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # Create a new config file and patch it to the experiment file.
            create_results = create_view_config_and_patch_to_file(connection, ref_files_by_ga[ga], file, [file], ff_auth, headers, 'tab:higlass_testing') # TODO revert to tab:higlass
            error = create_results["error"]
            success = create_results["success"]

            # Note if there was an error while creating the view config.
            if not success and "failed_post_file" in error:
                action_logs['failed_post_files'].append(file)
                continue

            action_logs['new_view_confs_by_file'][file] = create_results["view_conf_uuid"]

            # Note if we failed to patch the static content to this file.
            if not success and "patch" in error:
                # We failed to patch the static content to this file.
                action_logs['failed_patch_files'].append(file)
    action.status = 'DONE'
    action.output = action_logs

    target_files_by_ga = gen_check_result['full_output'].get('target_files', {})
    file_count = sum([len(target_files_by_ga[ga]) for ga in target_files_by_ga])
    action.progress = "{completed} out of {possible} files".format(
        completed=len(action_logs["new_view_confs_by_file"].keys()),
        possible=file_count
    )
    return action

@check_function(expset_uuid=None)
def generate_higlass_view_confs_files_for_expsets(connection, **kwargs):
    """
    Check to generate Higlass view configs on Fourfront for ExpSets.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        check results object.
    """
    check = init_check_res(connection, 'generate_higlass_view_confs_files_for_expsets')
    check.full_output = {}  # we will store results here

    # associate the action with the check. See the function named
    # 'post_higlass_view_confs_expsets' below
    check.action = 'post_higlass_view_confs_expsets'

    # first, find and cache the reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # if we don't have two reference files for each genome_assembly, fail
    if any([len(reference_files_by_ga[ga]) != 2 for ga in reference_files_by_ga]):
        check.status = 'FAIL'
        check.summary = check.description = "Could not find two reference files for each genome_assembly. See full_output"
        return check

    # next, find the Experiment Sets we are interested in.
    target_files_by_ga = {}
    files_to_generate_viewconfs_for_count = 0
    exp_sets_to_generate_viewconfs_for_count = 0

    if kwargs['expset_uuid']:
        search_query = '/search/?uuid=' + kwargs['expset_uuid']
    else:
        search_query = '/search/?type=ExperimentSet'

    search_res = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)

    for exp_set in search_res:
        # Skip the file if it has previously been registered by Foursight.
        if does_processed_file_have_auto_viewconf(exp_set):
            continue

        # Get all of the processed files with higlass_uids.
        file_info = gather_processedfiles_for_expset(exp_set)

        if file_info["error"]:
            continue

        processed_file_genome_assembly = file_info["genome_assembly"]
        processed_file_viewconf_source_files = file_info["files"]
        files_to_generate_viewconfs_for_count += len(file_info["files"])

        # Note the number of experiment sets that need to generate view confs.
        exp_sets_to_generate_viewconfs_for_count += 1

        if processed_file_genome_assembly not in target_files_by_ga:
            target_files_by_ga[processed_file_genome_assembly] = {}

        if exp_set["uuid"] not in target_files_by_ga[processed_file_genome_assembly]:
            target_files_by_ga[processed_file_genome_assembly][exp_set["uuid"]] = processed_file_viewconf_source_files

    check.full_output['target_files'] = target_files_by_ga
    check.status = 'PASS'

    if not target_files_by_ga:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
    else:
        check.summary = "Ready to generate {file_count} Higlass view configs for {exp_sets} Experiment Sets".format(file_count=files_to_generate_viewconfs_for_count, exp_sets=exp_sets_to_generate_viewconfs_for_count)
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True  # allow the action to be run
    return check

@action_function(expset_uuid=None)
def post_higlass_view_confs_files_for_expsets(connection, **kwargs):
    """ Action that is used with generate_higlass_view_confs_files_for_expsets to
    actually POST new higlass view configs and PATCH the old files.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object.
    """
    action = init_action_res(connection, 'post_higlass_view_confs_expsets')
    action_logs = {'new_view_confs_by_file': {}, 'failed_post_files': {},
                   'failed_patch_files': []}
    # get latest results
    gen_check = init_check_res(connection, 'generate_higlass_view_confs_files_for_expsets')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
    else:
        gen_check_result = gen_check.get_primary_result()

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}

    # pointer to the reference files (by genome_assembly)
    ref_files_by_ga = gen_check_result['full_output'].get('reference_files', {})

    # these are the files we care about
    # loop by genome_assembly
    for ga in gen_check_result['full_output'].get('target_files', {}):
        if ga not in ref_files_by_ga:  # reference files not found
            continue
        for experiment_set_uuid in gen_check_result['full_output']['target_files'][ga]:
            # If a uuid was specified, reject all others
            if kwargs["expset_uuid"] and experiment_set_uuid != kwargs["expset_uuid"]:
                continue

            # Get the files used to create this Experiment Set.
            experiment_set_files = gen_check_result['full_output']['target_files'][ga][experiment_set_uuid]

            # Create a new config file and patch it to the experiment file.
            create_results = create_view_config_and_patch_to_file(connection, ref_files_by_ga[ga], experiment_set_uuid, experiment_set_files, ff_auth, headers, 'tab:processed_files')
            error = create_results["error"]
            success = create_results["success"]

            # Note if there was an error while creating the view config.
            if not success and "failed_post_file" in error:
                action_logs['failed_post_files'][experiment_set_uuid] = error
                continue

            action_logs['new_view_confs_by_file'][experiment_set_uuid] = create_results["view_conf_uuid"]

            # Note if we failed to patch the static content to this file.
            if not success and "patch" in error:
                # We failed to patch the static content to this file.
                action_logs['failed_patch_files'].append(experiment_set_uuid)
    action.status = 'DONE'
    action.output = action_logs
    return action

def gather_processedfiles_for_expset(expset):
    """Collects all of the files for processed files.

    Returns:
    A dictionary with the following keys:
        genome_assembly(string, optional, default=""): The genome assembly all
            of the files use. Blank if there is an error or no files are found.
        files(list)                         : A list of identifiers for the
            discovered files.
        error(string, optional, default="") : Describes any errors generated.
    """

    # Collect all of the Processed files with a higlass uid.
    processed_files = []

    if "processed_files" in expset:
        # The Experiment Set may have Processed Files.
        processed_files = [ pf for pf in expset["processed_files"] if "higlass_uid" in pf ]

    # Search each Experiment, they may have Processed Files.
    if "experiments_in_set" in expset:
        for experiment in expset["experiments_in_set"]:
            exp_processed_files = [ pf for pf in experiment if "higlass_uid" in pf ]
            processed_files += exp_processed_files

    if len(processed_files) < 1:
        return {
            "error": "No processed files found",
            "files": [],
            "genome_assembly": "",
        }

    # Make sure all of them have the same genome assembly.
    genome_assembly_set = { pf["genome_assembly"] for pf in processed_files if "genome_assembly" in pf }

    if len(genome_assembly_set) > 1:
        return {
            "error": "Too many genome assemblies {gas}".format(gas=genome_assembly_set),
            "files": [],
            "genome_assembly": ""
        }

    # Return all of the processed files.
    unique_uuids = {}
    unique_files = []
    for pf in processed_files:
        if pf["uuid"] in unique_uuids:
            continue
        unique_uuids[pf["uuid"]] = pf
        unique_files.append(pf["uuid"])

    return {
        "error": "",
        "files": unique_files,
        "genome_assembly": processed_files[0]["genome_assembly"]
    }

@check_function(confirm_on_higlass=False, filetype='all', higlass_server=None)
def files_not_registered_with_higlass(connection, **kwargs):
    """
    Used to check registration of files on higlass and also register them
    through the patch_file_higlass_uid action.

    If confirm_on_higlass is True, check each file by making a request to the
    higlass server. Otherwise, just look to see if a higlass_uid is present in
    the metadata.

    The filetype arg allows you to specify which filetypes to operate on.
    Must be one of: 'all', 'mcool', 'bg', 'bw', 'beddb', 'chromsizes'.
    'chromsizes' and 'beddb' are from the raw files bucket; all other filetypes
    are from the processed files bucket.

    higlass_server may be passed in if you want to use a server other than
    higlass.4dnucleome.org.

    Since 'chromsizes' file defines the coordSystem (assembly) used to register
    other files in higlass, these go first.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object.
    """
    check = init_check_res(connection, 'files_not_registered_with_higlass')
    check.status = "FAIL"
    check.description = "not able to get data from fourfront"
    # keep track of mcool, bg, and bw files separately
    valid_types_raw = ['chromsizes', 'beddb']
    valid_types_proc = ['mcool', 'bg', 'bw', 'bed']
    all_valid_types = valid_types_raw + valid_types_proc

    files_to_be_reg = {}
    not_found_upload_key = []
    not_found_s3 = []
    no_genome_assembly = []

    # Make sure the filetype is valid.
    if kwargs['filetype'] != 'all' and kwargs['filetype'] not in all_valid_types:
        check.description = check.summary = "Filetype must be one of: %s" % (all_valid_types + ['all'])
        return check
    reg_filetypes = all_valid_types if kwargs['filetype'] == 'all' else [kwargs['filetype']]
    check.action = "patch_file_higlass_uid"

    # can overwrite higlass server, if desired. The default higlass key is always used
    higlass_key = connection.ff_s3.get_higlass_key()
    higlass_server = kwargs['higlass_server'] if kwargs['higlass_server'] else higlass_key['server']

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Run the check against all filetypes.
    for ftype in reg_filetypes:
        if time_expired:
            break
        files_to_be_reg[ftype] = []
        if ftype in valid_types_raw:
            typenames = ['FileReference']
            typebucket = connection.ff_s3.raw_file_bucket
        else:
            typenames = ['FileProcessed', 'FileVistrack']
            typebucket = connection.ff_s3.outfile_bucket
        typestr = 'type=' + '&type='.join(typenames)

        # Find all files with the file type and published status.
        search_query = 'search/?file_format.file_format=%s&%s' % (ftype, typestr)
        search_query += '&status!=uploading&status!=to+be+uploaded+by+workflow&status!=upload+failed'
        possibly_reg = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)

        for procfile in possibly_reg:
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            if 'genome_assembly' not in procfile:
                no_genome_assembly.append(procfile['accession'])
                continue
            file_info = {
                'accession': procfile['accession'],
                'uuid': procfile['uuid'],
                'file_format': procfile['file_format'].get('file_format'),
                'higlass_uid': procfile.get('higlass_uid'),
                'genome_assembly': procfile['genome_assembly']
            }

            # bg files use an bw file from extra files to register
            # bed files use a beddb file from extra files to regiser
            # don't FAIL if the bg is missing the bw, however
            type2extra = {'bg': 'bw', 'bed': 'beddb'}
            if ftype in type2extra:
                for extra in procfile.get('extra_files', []):
                    if extra['file_format'].get('display_title') == type2extra[ftype] and 'upload_key' in extra:
                        file_info['upload_key'] = extra['upload_key']
                        break
                if 'upload_key' not in file_info:  # bw or beddb file not found
                    continue
            else:
                # mcool and bw files use themselves
                if 'upload_key' in procfile:
                    file_info['upload_key'] = procfile['upload_key']
                else:
                    not_found_upload_key.append(file_info['accession'])
                    continue
            # make sure file exists on s3
            if not connection.ff_s3.does_key_exist(file_info['upload_key'], bucket=typebucket):
                not_found_s3.append(file_info)
                continue
            # check for higlass_uid and, if confirm_on_higlass is True, check the higlass server
            if file_info.get('higlass_uid'):
                if kwargs['confirm_on_higlass'] is True:
                    higlass_get = higlass_server + '/api/v1/tileset_info/?d=%s' % file_info['higlass_uid']
                    hg_res = requests.get(higlass_get)
                    # Make sure the response completed successfully and did not return an error.
                    if hg_res.status_code >= 400:
                        files_to_be_reg[ftype].append(file_info)
                    elif 'error' in hg_res.json().get(file_info['higlass_uid'], {}):
                        files_to_be_reg[ftype].append(file_info)
            else:
                files_to_be_reg[ftype].append(file_info)

    check.full_output = {'files_not_registered': files_to_be_reg,
                         'files_without_upload_key': not_found_upload_key,
                         'files_not_found_on_s3': not_found_s3,
                         'files_missing_genome_assembly': no_genome_assembly}
    if no_genome_assembly or not_found_upload_key or not_found_s3:
        check.status = "FAIL"
        check.summary = check.description = "Some files cannot be registed. See full_output"
    else:
        check.status = 'PASS'

    file_count = sum([len(files_to_be_reg[ft]) for ft in files_to_be_reg])
    if file_count != 0:
        check.status = 'WARN'
    if check.summary:
        check.summary += '. %s files ready for registration' % file_count
        check.description += '. %s files ready for registration. Run with confirm_on_higlass=True to check against the higlass server' % file_count
    else:
        check.summary = '%s files ready for registration' % file_count
        check.description = check.summary + '. Run with confirm_on_higlass=True to check against the higlass server'

    check.action_message = "Will attempt to patch higlass_uid for %s files." % file_count
    check.allow_action = True  # allows the action to be run
    return check

@action_function()
def patch_file_higlass_uid(connection, **kwargs):
    """ After running "files_not_registered_with_higlass",
    Try to register files with higlass.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object.
    """
    action = init_action_res(connection, 'patch_file_higlass_uid')
    action_logs = {'patch_failure': [], 'patch_success': [],
                   'registration_failure': [], 'registration_success': 0}
    # get latest results
    higlass_check = init_check_res(connection, 'files_not_registered_with_higlass')
    if kwargs.get('called_by', None):
        higlass_check_result = higlass_check.get_result_by_uuid(kwargs['called_by'])
    else:
        higlass_check_result = higlass_check.get_primary_result()

    # get the desired server
    higlass_key = connection.ff_s3.get_higlass_key()
    if higlass_check_result['kwargs'].get('higlass_server'):
        higlass_server = higlass_check_result['kwargs']['higlass_server']
    else:
        higlass_server = higlass_key['server']

    # Prepare authentication header
    authentication = (higlass_key['key'], higlass_key['secret'])
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Files to register is organized by filetype.
    to_be_registered = higlass_check_result.get('full_output', {}).get('files_not_registered')
    for ftype, hits in to_be_registered.items():
        if time_expired:
            break
        for hit in hits:
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # Based on the filetype, construct a payload to upload to the higlass server.
            payload = {'coordSystem': hit['genome_assembly']}
            if ftype == 'chromsizes':
                payload["filepath"] = connection.ff_s3.raw_file_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'chromsizes-tsv'
                payload['datatype'] = 'chromsizes'
            elif ftype == 'beddb':
                payload["filepath"] = connection.ff_s3.raw_file_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'beddb'
                payload['datatype'] = 'gene-annotation'
            elif ftype == 'mcool':
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'cooler'
                payload['datatype'] = 'matrix'
            elif ftype in ['bg', 'bw']:
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'bigwig'
                payload['datatype'] = 'vector'
            elif ftype == 'bed':
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'beddb'
                payload['datatype'] = 'bedlike'
            # register with previous higlass_uid if already there
            if hit.get('higlass_uid'):
                payload['uuid'] = hit['higlass_uid']
            res = requests.post(higlass_server + '/api/v1/link_tile/',
                                data=json.dumps(payload), auth=authentication,
                                headers=headers)
            # update the metadata file as well, if uid wasn't already present or changed
            if res.status_code == 201:
                action_logs['registration_success'] += 1
                # Get higlass's uuid. This is Fourfront's higlass_uid.
                response_higlass_uid = res.json()['uuid']
                if 'higlass_uid' not in hit or hit['higlass_uid'] != response_higlass_uid:
                    patch_data = {'higlass_uid': response_higlass_uid}
                    try:
                        ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], key=connection.ff_keys, ff_env=connection.ff_env)
                    except Exception as e:
                        acc_and_error = '\n'.join([hit['accession'], str(e)])
                        action_logs['patch_failure'].append(acc_and_error)
                    else:
                        action_logs['patch_success'].append(hit['accession'])
            else:
                action_logs['registration_failure'].append(hit['accession'])
    action.status = 'DONE'
    action.output = action_logs
    return action

@check_function()
def find_cypress_test_items_to_purge(connection, **kwargs):
    """ Looks for all items that are deleted and marked for purging by cypress test.
    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object
    """

    check = init_check_res(connection, 'find_cypress_test_items_to_purge')
    check.full_output = {
        'items_to_purge':[],
        'rationale':{},
    }

    # associate the action with the check.
    check.action = 'purge_cypress_items'

    # Search for all Higlass View Config that are deleted.
    search_query = '/search/?type=Item&status=deleted'
    search_response = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)

    # For each view config
    for viewconf in search_response:
        uuid = viewconf["uuid"]
        # Get their tags
        tags = viewconf.get("tags", [])

        # For a given file, track:
        # - purge(boolean): If we should definitely purge it (or definitely keep it)
        # - reason(string): The rule dictating purge/keep (this shows the reason)
        # - priority(number): The priority of the reason (so higher priority can ignore lower ones and we get consistent behavior)
        rule_rationale = []

        # If it's marked for deletion by Cypress testing, purge.
        if "deleted_by_cypress_test" in tags:
            rule_rationale.append({
                "purge" : True,
                "reason" : "cypress_test",
                "priority" : 100
            })

        # All tags have been processed, pick the highest priority rule.
        if len(rule_rationale) > 0:
            max_rule = max(
                rule_rationale,
                key = lambda rul : rul["priority"]
            )

            # If the file should be purged, add it.
            if max_rule["purge"]:
                check.full_output['items_to_purge'].append(uuid)

            # Add the rationale to the full output.
            check.full_output['rationale'][uuid] = rule_rationale

    # Note the number of items ready to purge
    num_viewconfigs = len(check.full_output['items_to_purge'])
    check.status = 'PASS'

    if num_viewconfigs == 0:
        check.summary = check.description = "No new items to purge."
    else:
        check.summary = "Ready to purge %s items" % num_viewconfigs
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True
    return check

@action_function()
def purge_cypress_items(connection, **kwargs):
    """ Using the find_cypress_test_items_to_purge check, deletes the indicated items.
    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check object
    """

    action = init_action_res(connection, 'purge_cypress_items')
    action_logs = {
        'viewconfs_purged':[],
        'failed_to_purge':{}
    }

    # get latest results
    gen_check = init_check_res(connection, 'find_cypress_test_items_to_purge')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
    else:
        gen_check_result = gen_check.get_primary_result()

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Purge the deleted files.
    for view_conf_uuid in gen_check_result["full_output"]["items_to_purge"]:
        # If we've taken more than 270 seconds to complete, break immediately
        if time.time() - start_time > 270:
            time_expired = True
            break

        purge_response = ff_utils.purge_metadata(view_conf_uuid, key=connection.ff_keys, ff_env=connection.ff_env)
        if purge_response['status'] == 'success':
            action_logs['items_purged'].append(view_conf_uuid)
        else:
            action_logs['failed_to_purge'][view_conf_uuid] = purge_response["comment"]

    action.status = 'DONE'
    action.output = action_logs
    return action
