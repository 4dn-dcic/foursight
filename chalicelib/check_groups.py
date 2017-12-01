# put the names of all modules that have checks within in them below
CHECK_MODULES = [
    'system_checks',
    'wrangler_checks'
]

# define check groups for schedules here
# each group is an array with entries corresponding to one check's run
# info, which is ['<mod>/<check>', '<kwargs>', list of check dependencies]

daily_checks = [
    ['system_checks/elastic_beanstalk_health', {}, []],
    ['system_checks/status_of_elasticsearch_indices', {}, []],
    ['system_checks/indexing_records', {}, []],
    ['system_checks/staging_deployment', {}, []],
    ['wrangler_checks/change_in_item_counts', {}, []]
]

two_hour_checks = [
    ['system_checks/indexing_progress', {}, []],
    ['wrangler_checks/item_counts_by_type', {}, []]
]
