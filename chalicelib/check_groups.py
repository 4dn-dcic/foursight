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
    'all_checks': [
        ['wrangler_checks/items_created_in_the_past_day', {'primary': True}, [], 'all1'],
        ['wrangler_checks/files_associated_with_replicates', {'primary': True}, [],'all2'],
        ['wrangler_checks/replicate_file_reporting', {'primary': True}, [], 'all3'],
        ['system_checks/elastic_beanstalk_health', {'primary': True}, [], 'all4'],
        ['system_checks/status_of_elasticsearch_indices', {'primary': True}, [], 'all5'],
        ['system_checks/indexing_records', {'primary': True}, [], 'all6'],
        ['wrangler_checks/item_counts_by_type', {'primary': True}, [], 'all7'],
        ['wrangler_checks/change_in_item_counts', {'primary': True}, ['all7'], 'all8'],
        ['system_checks/indexing_progress', {'primary': True}, [], 'all9'],
        ['wrangler_checks/identify_files_without_filesize', {'primary': True}, [], 'all10'],
        ['system_checks/staging_deployment', {'primary': True}, [], 'all11']
    ],
    'daily_checks': [
        ['wrangler_checks/items_created_in_the_past_day', {'primary': True}, [], 'd1'],
        ['wrangler_checks/files_associated_with_replicates', {'primary': True}, [],'d2'],
        ['wrangler_checks/replicate_file_reporting', {'primary': True}, [], 'd3']
    ],
    'six_hour_checks': [
        ['system_checks/elastic_beanstalk_health', {'primary': True}, [], 's1'],
        ['system_checks/status_of_elasticsearch_indices', {'primary': True}, [], 's2'],
        ['system_checks/indexing_records', {'primary': True}, [], 's3'],
        ['wrangler_checks/item_counts_by_type', {'primary': True}, [], 's4'],
        ['wrangler_checks/change_in_item_counts', {'primary': True}, ['s4'], 's5'],
        ['system_checks/indexing_progress', {'primary': True}, [], 's6']
    ],
    'two_hour_checks': [
        ['wrangler_checks/identify_files_without_filesize', {'primary': True}, [], 't1'],
        ['wrangler_checks/item_counts_by_type', {'primary': True}, [], 't2'],
        ['system_checks/indexing_progress', {'primary': True}, [], 't3'],
        ['system_checks/staging_deployment', {'primary': True}, [], 't4']
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
    ],
    'valid_test_checks': [
        ['test_checks/add_random_test_nums', {'primary': True}, ['tt3'], 'tt1'],
        ['test_checks/add_random_test_nums', {'primary': True}, [], 'tt2'],
        ['test_checks/add_random_test_nums', {'primary': True}, ['tt2'], 'tt3']
    ]
}

TEST_ACTION_GROUPS = {
    'add_random_test_nums': [
        ['test_checks/add_random_test_nums', {'primary': True}, ['tag1'], 'tag2'],
        ['test_checks/test_random_nums', {'primary': True}, [], 'tag1'],
        ['test_checks/test_random_nums', {'primary': True}, ['tag1']] # purposefully malformed
    ],
    'add_random_test_nums_solo': [
        ['test_checks/add_random_test_nums', {'primary': True}, [], 'tzg1']
    ]
}
