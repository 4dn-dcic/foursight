from dcicutils import ff_utils
from datetime import datetime

# wfr_name, accepted versions, expected run time
workflow_details = [['md5', ['0.0.4', '0.2.6'], 12],
                    ['fastqc-0-11-4-1', ['0.2.0'], 50],
                    ['bwa-mem', ['0.2.6'], 50],
                    ['pairsqc-single', ['0.2.5', '0.2.6'], 100],
                    ['hi-c-processing-bam', ['0.2.6'], 50],
                    ['hi-c-processing-pairs', ['0.2.6', '0.2.7'], 200],
                    ['hi-c-processing-pairs-nore', ['0.2.6'], 200],
                    ['hi-c-processing-pairs-nonorm', ['0.2.6'], 200],
                    ['hi-c-processing-pairs-nore-nonorm', ['0.2.6'], 200],
                    ['repliseq-parta', ['v13.1', 'v14', 'v16'], 200],
                    ['bedGraphToBigWig', ['v4'], 24],
                    ['bedtobeddb', ['v2'], 24],
                    ['encode-chipseq-aln-chip', ['1.1.1'], 200],
                    ['encode-chipseq-aln-ctl', ['1.1.1'], 200],
                    ['encode-chipseq-postaln', ['1.1.1'], 200],
                    ['encode-atacseq-aln', ['1.1.1'], 200],
                    ['encode-atacseq-postaln', ['1.1.1'], 200],
                    ['mergebed', ['v1'], 200]
                    ]


def get_wfr_out(file_id, wfr_name, auth, versions=[], md_qc=False, run=None):
    """For a given file, fetches the status of last wfr (of wfr_name type)
    If there is a successful run, it will return the output files as a dictionary of
    argument_name:file_id, else, will return the status. Some runs, like qc and md5,
    does not have any file_format output, so they will simply return 'complete'
    args:
     file_id: accession/uuid/alias of file
     wfr_name: base name without version
     auth: connection ff_keys
     versions: acceptable versions for wfr
     md_qc: if no output file is excepted, set to True
     run: if run is still running beyond this hour limit, assume problem
    """
    # get default accepted versions if not provided
    if not versions:
        versions = [i[1] for i in workflow_details if i[0] == wfr_name][0]
    # get default run out time
    if not run:
        run = [i[2] for i in workflow_details if i[0] == wfr_name][0]
    emb_file = ff_utils.get_metadata(file_id, key=auth)
    workflows = emb_file.get('workflow_run_inputs')
    wfr = {}
    run_status = 'did not run'
    my_workflows = [i for i in workflows if i['display_title'].startswith(wfr_name)]
    if not my_workflows:
        return {'status': "no workflow on file"}

    for a_wfr in my_workflows:
        wfr_type, time_info = a_wfr['display_title'].split(' run ')
        wfr_type_base, wfr_version = wfr_type.strip().split(' ')
        # user submitted ones use run on insteand of run
        time_info = time_info.strip('on').strip()
        try:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S')
        a_wfr['run_hours'] = (datetime.utcnow() - wfr_time).total_seconds() / 3600
        a_wfr['run_type'] = wfr_type_base.strip()
        a_wfr['run_version'] = wfr_version.strip()
    my_workflows = [i for i in my_workflows if i['run_version'] in versions]
    if not my_workflows:
        return {'status': "no workflow in file with accepted version"}
    my_workflows = sorted(my_workflows, key=lambda k: k['run_hours'])
    last_wfr = [i for i in my_workflows if i['run_type'] == wfr_name][0]
    # get metadata for the last wfr
    wfr = ff_utils.get_metadata(last_wfr['uuid'], key=auth)
    run_duration = last_wfr['run_hours']
    run_status = wfr['run_status']

    if run_status == 'complete':
        outputs = wfr.get('output_files')
        # some runs, like qc, don't have a real file output
        if md_qc:
            return {'status': 'complete'}
        # if expected output files, return a dictionary of argname:file_id
        else:
            out_files = {}
            for output in outputs:
                if output.get('format'):
                    # get the arg name
                    arg_name = output['workflow_argument_name']
                    out_files[arg_name] = output['value']['@id']
            if out_files:
                out_files['status'] = 'complete'
                return out_files
            else:
                print('no output file was found, maybe this run is a qc?')
                return {'status': "no file found"}
    # if status is error
    elif run_status == 'error':
        return {'status': "no complete run, errrored"}
    # if other statuses, started running
    elif run_duration < run:
        return {'status': "running"}
    # this should be the timeout case
    else:
        return {'status': "no completed run, timout"}


def get_attribution(file_json):
    attributions = {
        'lab': file_json['lab']['@id'],
        'award': file_json['award']['@id']
    }
    cont_labs = []
    if file_json.get('contributing_labs'):
        cont_labs = [i['@id'] for i in file_json['contributing_labs']]

    appendFDN = True
    if attributions['lab'] == '/labs/4dn-dcic-lab/':
        appendFDN = False

    if cont_labs:
        if appendFDN:
            cont_labs.append('/labs/4dn-dcic-lab/')
            cont_labs = list(set(cont_labs))
        attributions['contributing_labs'] = cont_labs

    else:
        if appendFDN:
            cont_labs = ['/labs/4dn-dcic-lab/']
            attributions['contributing_labs'] = cont_labs
        else:
            pass
    return attributions
