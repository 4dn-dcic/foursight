@check_function(lab_title=None, start_date=None)
def bed2multivec_status(connection, **kwargs):
    """Searches for bed files states types that don't have bed2multivec
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'bed2multivec_status')
    my_auth = connection.ff_keys
    check.action = "bed2multivec_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # Build the query (find bg files without bw files)
    query = ("search/?file_format.file_format=bed&file_type=states&type=FileProcessed&extra_files.file_format.display_title!=bed.multires.mv5")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query += '&lab.display_title=' + lab
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_runs_without_output(res, check, 'bedtomultivec', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bed2multivec_start(connection, **kwargs):
    """Start bed2multivec runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'bed2multivec_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bed2multivec_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(bed2multivec_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bed2multivec_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        org = wfr_utils.mapper[a_file['genome_assembly']]
        org = [k for k, v in wfr_utils.mapper.items() if v == a_file['genome_assembly']][0]
        states_tag = [i for i in wfr_utils.mapper[a_file['tag']] if 'SPIN_states' in i][0]

        chrsize = wfr_utils.chr_size[org]
        rows_info = wfr_utils.states_color_mapper[states_tag]

        inp_f = {'bedfile': a_file['@id'], 'chromsizes_file': chrsize, 'rows_info': rows_info}
        wfr_setup = wfrset_utils.step_settings('bedtomultivec',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action
