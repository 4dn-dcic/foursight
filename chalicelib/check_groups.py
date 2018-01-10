# put the names of all modules that have checks within in them below
CHECK_MODULES = [
    'system_checks',
    'wrangler_checks',
    'test_checks'
]

# define check groups for schedules here
# each group is an array with entries corresponding to one check's run
# info, which is ['<mod>/<check>', '<kwargs>', list of check dependencies]

# check group names should end in "_checks"
# define check_groups within this dict

CHECK_GROUPS = {
    'daily_checks': [
        ['wrangler_checks/items_created_in_the_past_day', {}, []],
        ['wrangler_checks/files_associated_with_replicates', {}, []],
        ['wrangler_checks/replicate_file_reporting', {}, []]
    ],
    'six_hour_checks': [
        ['system_checks/elastic_beanstalk_health', {}, []],
        ['system_checks/status_of_elasticsearch_indices', {}, []],
        ['system_checks/indexing_records', {}, []],
        ['wrangler_checks/item_counts_by_type', {}, []],
        ['wrangler_checks/change_in_item_counts', {}, []],
        ['system_checks/indexing_progress', {}, []]
    ],
    'two_hour_checks': [
        ['wrangler_checks/identify_files_without_filesize', {}, []],
        ['wrangler_checks/item_counts_by_type', {}, []],
        ['system_checks/indexing_progress', {}, []],
        ['system_checks/staging_deployment', {}, []]
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
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'Biosample'}, []],
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'ExperimentSetReplicate'}, []],
        ['wrangler_checks/items_created_in_the_past_day', {'item_type': 'FileFastq'}, []]
    ]
}
