# put the names of all modules that have checks within in them below
CHECK_MODULES = [
    'system_checks',
    'wrangler_checks',
    'test_checks'
]

# define check groups for schedules here
# each group is an array with entries corresponding to one check's run
# info, which is ['<mod>/<check>', '<kwargs>', list of check dependencies, dependency id]
# dependecy id can be any unique string

# check group names should end in "_checks" or "_actions"
# define check_groups within this dict

CHECK_GROUPS = {
    'daily_checks': [
        ['wrangler_checks/items_created_in_the_past_day', {}, [], 'd1'],
        ['wrangler_checks/files_associated_with_replicates', {}, [],'d2'],
        ['wrangler_checks/replicate_file_reporting', {}, [], 'd3']
    ],
    'six_hour_checks': [
        ['system_checks/elastic_beanstalk_health', {}, [], 's1'],
        ['system_checks/status_of_elasticsearch_indices', {}, [], 's2'],
        ['system_checks/indexing_records', {}, [], 's3'],
        ['wrangler_checks/item_counts_by_type', {}, [], 's4'],
        ['wrangler_checks/change_in_item_counts', {}, [], 's5'],
        ['system_checks/indexing_progress', {}, [], 's6']
    ],
    'two_hour_checks': [
        ['wrangler_checks/identify_files_without_filesize', {}, [], 't1'],
        ['wrangler_checks/item_counts_by_type', {}, [], 't2'],
        ['system_checks/indexing_progress', {}, [], 't3'],
        ['system_checks/staging_deployment', {}, [], 't4']
    ]
}

# action groups work the same as check groups, but can contain intermixed checks and actions
# minimally, an action group should have the action itself and also the check that triggered it
# (so that the check can be updated)

ACTION_GROUPS = {
    'patch_file_size': [
        ['wrangler_checks/identify_files_without_filesize', {'search_add_on': '&datastore=database'}, [], 'pfs1'],
        ['wrangler_checks/patch_file_size', {}, ['pfs1'], 'pfs2'],
        ['wrangler_checks/identify_files_without_filesize', {'search_add_on': '&datastore=database'}, ['pfs2'], 'pfs3']
    ]
}


######## don't use the check groups below! just for testing ########

TEST_CHECK_GROUPS = {
    'malformed_test_checks': [
        [{}, []], # bad
        ['system_checks/indexing_progress', []], # bad
        ['system_checks/indexing_progress', {}], # bad
    ],
    'wrangler_test_checks': [
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Biosample'}, [], 'wt1'],
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Experiment'}, ['wt1'], 'wt2'],
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'File'}, ['wt2'], 'wt3']
    ]
}

TEST_ACTION_GROUPS = {
    'add_random_test_nums': [
        ['test_checks/test_random_nums', {}, [], 'tag1'],
        ['test_checks/add_random_test_nums', {}, ['tag1'], 'tag2']
    ]
}
