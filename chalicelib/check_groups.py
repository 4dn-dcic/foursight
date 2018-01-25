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
    'ten_min_checks': [
        ['system_checks/elastic_beanstalk_health', {'primary': True}, [], 'm10_1'],
        ['wrangler_checks/items_created_in_the_past_day', {'primary': True}, [], 'm10_2'],
        ['wrangler_checks/item_counts_by_type', {'primary': True}, [], 'm10_3'],
        ['wrangler_checks/change_in_item_counts', {'primary': True}, ['m10_3'], 'm10_4'],
        ['wrangler_checks/replicate_file_reporting', {'primary': True}, [], 'm10_5'],
        ['system_checks/indexing_progress', {'primary': True}, [], 'm10_6'],
        ['system_checks/staging_deployment', {'primary': True}, [], 'm10_7']
    ],
    'thirty_min_checks': [
        ['wrangler_checks/files_associated_with_replicates', {'primary': True}, [],'m30_1'],
        ['system_checks/status_of_elasticsearch_indices', {'primary': True}, [], 'm30_2'],
        ['wrangler_checks/identify_files_without_filesize', {'primary': True}, [], 'm30_3'],
        ['system_checks/indexing_records', {'primary': True}, [], 'm30_4']
    ]
}

# action groups work the same as check groups, but can contain intermixed checks and actions
# minimally, an action group should have the action itself and also the check that triggered it
# (so that the check can be updated)

ACTION_GROUPS = {
    'patch_file_size': [
        ['wrangler_checks/identify_files_without_filesize', {'primary': True, 'search_add_on': '&datastore=database'}, [], 'pfs1'],
        ['wrangler_checks/patch_file_size', {}, ['pfs1'], 'pfs2'],
        ['wrangler_checks/identify_files_without_filesize', {'primary': True, 'search_add_on': '&datastore=database'}, ['pfs2'], 'pfs3']
    ]
}


######## don't use the check groups below! just for testing ########

TEST_CHECK_GROUPS = {
    'all_checks': [
        ['wrangler_checks/items_created_in_the_past_day', {}, [], 'all1'],
        ['wrangler_checks/files_associated_with_replicates', {}, [],'all2'],
        ['wrangler_checks/replicate_file_reporting', {}, [], 'all3'],
        ['system_checks/elastic_beanstalk_health', {}, [], 'all4'],
        ['system_checks/status_of_elasticsearch_indices', {}, [], 'all5'],
        ['system_checks/indexing_records', {}, [], 'all6'],
        ['wrangler_checks/item_counts_by_type', {}, [], 'all7'],
        ['wrangler_checks/change_in_item_counts', {}, ['all7'], 'all8'],
        ['system_checks/indexing_progress', {}, [], 'all9'],
        ['wrangler_checks/identify_files_without_filesize', {}, [], 'all10'],
        ['system_checks/staging_deployment', {}, [], 'all11']
    ],
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
        ['test_checks/add_random_test_nums', {}, ['tag1'], 'tag2'],
        ['test_checks/test_random_nums', {'primary': True}, ['tag2'], 'tag3'],
        ['test_checks/test_random_nums', {'primary': True}, ['tag0'], 'tag1'],
        ['test_checks/test_random_nums', {'primary': True}, [], 'tag0'],
        ['test_checks/test_random_nums', {'primary': True}, ['tagX']] # purposefully malformed
    ],
    'add_random_test_nums_solo': [
        ['test_checks/add_random_test_nums', {}, [], 'tzg1']
    ]
}
