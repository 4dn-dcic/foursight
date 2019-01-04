import unittest
import json

import requests

from dcicutils import ff_utils
import app

# Manual test script for higlass checks.

# Track all created files by the server route used to create them.

def add_processed_file(connection, accession, genome_assembly, higlass_uid, endpoint_url='files-processed', extra_data=None):
    ''' Add file with the given genome assembly and accession
    Also add higlass_uid (if given)
    If the file already exists, patch it with this data.
    '''

    # Make dummy data and overwrite with extra_data
    file_data = {
        "lab": "/labs/4dn-dcic-lab/",
        "status": "released",
        "award":"/awards/1U01CA200059-01/",
        "accession": accession,
        "file_classification":"processed file",
        "date_created":"2018-11-29T01:37:58.357095+00:00",
        "submitted_by": "4dndcic@gmail.com",
        "file_type": "other",
        "genome_assembly": genome_assembly,
    }

    if higlass_uid:
        file_data["higlass_uid"] = higlass_uid

    if endpoint_url == 'files-processed':
        file_data.update({
            "dataset_type": "Dataset",
            "assay_info": "Assay",
            "biosource_name" : "Biosource",
        })

    file_data.update(extra_data)

    # Post to fourfront
    ff_endpoint = connection.ff_server + endpoint_url
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        response = requests.post(
            ff_endpoint,
            data=json.dumps(file_data),
            auth=ff_auth,
            headers=headers
        )
    except Exception as e:
        return

    # Make sure the response is successful and notes a new file
    if response.status_code != 201:
        # If this already exists, patch it instead.
        if response.status_code == 409:
            file_data["status"] = "released"
            try:
                response = requests.patch(
                    ff_endpoint + "/" + accession + "/",
                    data=json.dumps(file_data),
                    auth=ff_auth,
                    headers=headers
                )
            except Exception as e:
                return
        else:
            return

    new_file = response.json()["@graph"][0]

    # Get the file type, wait for the database to update.
    file_metadata = ff_utils.get_metadata(new_file["uuid"], key=connection.ff_keys, ff_env=connection.ff_env, check_queue=True)

    return file_metadata["uuid"]

def get_file_formats(connection):
    """Returns a dict of file formats, with display title as keys.
    """
    # Get authentication
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Get the uuid to file format mappings.
    try:
        response = requests.get(
            connection.ff_server + 'file-formats/?type=FileFormat&limit=all',
            auth=ff_auth,
            headers=headers
        )
    except Exception as e:
        raise

    file_formats = {}
    if response.status_code == 200:
        # Populate the file formats by display_title.
        for raw_format in response.json()["@graph"]:
            file_formats[raw_format["display_title"]] = raw_format
    return file_formats

def maybe_add_reference_files(connection, file_formats):
    """Check the test server to see if refernce files need to be added.
    """
    # Get authentication
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Look for genome assembly files for Human and Mouse. If they don't exist, add them.
    reference_files_by_ga = {
        "GRCh38" : {
            'chromsizes' : None,
            'beddb' : None,
        },
        "GRCm38" : {
            'chromsizes' : None,
            'beddb' : None,
        },
    }
    ref_search_q = '/search/?type=File&tags=higlass_reference'
    ref_res = ff_utils.search_metadata(ref_search_q, key=connection.ff_keys, ff_env=connection.ff_env)
    for ref in ref_res:
        if 'higlass_uid' not in ref or 'genome_assembly' not in ref:
            continue

        genome_assembly = ref['genome_assembly']
        if not genome_assembly in reference_files_by_ga:
            continue

        # Get file format.
        ref_format = ref.get('file_format', {}).get('file_format')
        if ref_format not in reference_files_by_ga[genome_assembly]:
            continue

        # cache reference files by genome_assembly
        reference_files_by_ga[genome_assembly][ref_format] = ref['uuid']

    new_reference_files_by_ga = {
        "GRCh38" : {
            'chromsizes' : {
                "accession" : "TESTCHROMSIZES00",
                "higlass_uid" : "higlass_uid00",
            },
            'beddb' : {
                "accession" : "TESTBEDDB01",
                "higlass_uid" : "higlass_uid01",
            },
        },
        "GRCm38" : {
            'chromsizes' : {
                "accession" : "TESTCHROMSIZES02",
                "higlass_uid" : "higlass_uid05",
            },
            'beddb' : {
                "accession" : "TESTBEDDB02",
                "higlass_uid" : "higlass_uid04",
            },
        },
    }

    for genome_assembly in reference_files_by_ga:
        for filetype, fileuuid in reference_files_by_ga[genome_assembly].items():
            if not fileuuid:
                ref_uuid = add_processed_file(connection, new_reference_files_by_ga[genome_assembly][filetype]["accession"], genome_assembly, new_reference_files_by_ga[genome_assembly][filetype]["higlass_uid"],
                    endpoint_url='files-reference',
                    extra_data={
                        "file_format" : file_formats[filetype]["uuid"],
                        "tags": ["higlass_reference"],
                        "file_classification": "ancillary file",
                    }
                )
                reference_files_by_ga[genome_assembly][ref_format] = ref_uuid
    return reference_files_by_ga

class TestGenerateHiglassViewConfFiles(unittest.TestCase):
    file_formats = {}
    connection = None

    @classmethod
    def setUpClass(cls):
        # Set up connection to test server
        TestGenerateHiglassViewConfFiles.connection = app.init_connection('mastertest')

        # Get file formats
        TestGenerateHiglassViewConfFiles.file_formats = get_file_formats(TestGenerateHiglassViewConfFiles.connection)

        # Add the reference files, if needed
        maybe_add_reference_files(TestGenerateHiglassViewConfFiles.connection, TestGenerateHiglassViewConfFiles.file_formats)

    def test_check_found_viewconf_to_generate(self):
        ''' Create a new mcool file.
        After running the generate_higlass_view_confs_files check, task will note a new file to create.
        '''

        # Add an mcool file. It will have a Human genome assembly.
        file_uuid = add_processed_file(TestGenerateHiglassViewConfFiles.connection, "TESTMCOOL01", "GRCh38", "higlass_uid02", extra_data={
            "file_format" : TestGenerateHiglassViewConfFiles.file_formats["mcool"]["uuid"]
        })

        # Run the check to generate Higlass view configs.
        check = app.run_check_or_action(TestGenerateHiglassViewConfFiles.connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': None})

        # Check is ready to generate Higlass view configs
        self.assertTrue("Ready to generate " in check["summary"])

        # Allow action is True
        self.assertTrue(check["allow_action"])

        # The mcool file should be targeted
        self.assertTrue("GRCh38" in check["full_output"]['target_files'])

        self.assertTrue(file_uuid in check["full_output"]['target_files']['GRCh38'])

        # Status = PASS
        self.assertEqual("PASS", check["status"])

    def test_autogenerated_files_dont_generate(self):
        '''Add a file that already has an auto_generated_higlass_view_config.
        When the check runs it should not mark this file as requiring a new view config.
        '''

        # Add an mcool file. It will have a Human genome assembly.
        # Add a auto_generated_higlass_view_config field.
        file_uuid = add_processed_file(TestGenerateHiglassViewConfFiles.connection, "TESTMCOOL01", "GRCh38", "higlass_uid02", extra_data={
            "file_format" : TestGenerateHiglassViewConfFiles.file_formats["mcool"]["uuid"],
            "static_content": [
                {
                    "description" : "auto_generated_higlass_view_config",
                    "location" : "Where this content should be displayed.",
                    "content" : "linkTo",
                }
            ]
        })

        # Run the check to generate Higlass view configs.
        check = app.run_check_or_action(TestGenerateHiglassViewConfFiles.connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': None})

        # Check is ready to generate Higlass view configs
        self.assertTrue("Ready to generate " in check["summary"])

        # Allow action is True
        self.assertTrue(check["allow_action"])

        # The mcool file should NOT be targeted
        self.assertTrue("GRCh38" in check["full_output"]['target_files'])
        self.assertFalse(file_uuid in check["full_output"]['target_files']['GRCh38'])

        # Status = PASS
        self.assertEqual("PASS", check["status"])

    def test_associate_with_genome_assembly(self):
        ''' Add a new file with a different genome assembly.
        After running the generate_higlass_view_confs_files check, task will note a new file to create using a different genome_assembly.
        '''
        # Add an mcool file. It will have a Mouse genome assembly.
        file_uuid = add_processed_file(TestGenerateHiglassViewConfFiles.connection, "TESTMCOOL02", "GRCm38", "higlass_uid03", extra_data={
            "file_format" : TestGenerateHiglassViewConfFiles.file_formats["mcool"]["uuid"]
        })

        # Run the check to generate Higlass view configs.
        check = app.run_check_or_action(TestGenerateHiglassViewConfFiles.connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': None})

        # Check is ready to generate Higlass view configs
        self.assertTrue("Ready to generate " in check["summary"])

        # Allow action is True
        self.assertTrue(check["allow_action"])

        # The mcool file should be targeted, under the mouse genome_assembly
        self.assertTrue("GRCm38" in check["full_output"]['target_files'])
        self.assertTrue(file_uuid in check["full_output"]['target_files']['GRCm38'])

        # Status = PASS
        self.assertEqual("PASS", check["status"])

class TestPostHiglassViewConfsFiles(unittest.TestCase):
    '''Make sure to wait for the queue to clear before running.
    '''
    file_formats = {}
    connection = None
    reference_files_by_ga = {}

    @classmethod
    def setUpClass(cls):
        # Set up connection to test server
        TestPostHiglassViewConfsFiles.connection = app.init_connection('mastertest')

        # Get file formats
        TestPostHiglassViewConfsFiles.file_formats = get_file_formats(TestPostHiglassViewConfsFiles.connection)

        # Add the reference files, if needed
        TestPostHiglassViewConfsFiles.reference_files_by_ga = maybe_add_reference_files(TestPostHiglassViewConfsFiles.connection, TestPostHiglassViewConfsFiles.file_formats)

    def test_add_higlass_view_conf(self):
        """ Running the action should create a new view conf and add a static section to display it.
        """

        # Post a new file containing a Human genome assembly, wiping out its static content.
        # Get the uuid.
        file_uuid = add_processed_file(TestPostHiglassViewConfsFiles.connection, "TESTMCOOL06", "GRCh38", "higlass_uid06", extra_data={
            "file_format" : TestPostHiglassViewConfsFiles.file_formats["mcool"]["uuid"],
            "static_content": []
        })

        # Call the generate_higlass_view_confs_files check
        file_res = ff_utils.get_metadata(file_uuid, key=TestPostHiglassViewConfsFiles.connection.ff_keys, ff_env=TestPostHiglassViewConfsFiles.connection.ff_env, check_queue=True)

        check_result = app.run_check_or_action(TestPostHiglassViewConfsFiles.connection, 'higlass_checks/generate_higlass_view_confs_files', {'called_by': "test_higlass_checks"})

        self.assertTrue(file_uuid in check_result['full_output']['target_files']["GRCh38"])

        # Now run the action.
        action_result = app.run_check_or_action(TestPostHiglassViewConfsFiles.connection, 'higlass_checks/post_higlass_view_confs_files', {'called_by': check_result["kwargs"]["uuid"]})

        # Action should be DONE.
        self.assertEqual('DONE', action_result["status"])

        # This file should NOT be in the failed_patch_files
        self.assertFalse(file_uuid in action_result["output"]["failed_patch_files"])

        # This file should be in the new_view_confs_by_file
        self.assertTrue(file_uuid in action_result["output"]["new_view_confs_by_file"])

        # You should be able to find the new viewconf file
        viewconf_uuid = action_result["output"]["new_view_confs_by_file"][file_uuid]

        # This file should have a static section, get the metadata again to be sure
        file_metadata = ff_utils.get_metadata(file_uuid, key=TestPostHiglassViewConfsFiles.connection.ff_keys, ff_env=TestPostHiglassViewConfsFiles.connection.ff_env, check_queue=True)
        file_static_content = file_metadata.get('static_content', [])

        def found_viewconf_static_section(static_content):
            return (
                static_content['location'] == 'tab:higlass' and
                static_content['content']['uuid'] == viewconf_uuid and
                static_content['description'] == 'auto_generated_higlass_view_config'
            )

        self.assertTrue(any(found_viewconf_static_section(sc) for sc in file_static_content))

# Check file not registered
# Add chromsize file with mouse genome assembly and no higlass_uid
# Confirm the status is WARN
# "1 files ready for integration"

# Check registered
# Add chromsize file with mouse genome assembly and with a higlass_uid
# Confirm the status is PASS

# Check no genome assembly
# Add chromsize file with no genome assembly and with a higlass_uid
# Confirm the status is FAIL

# Check Extra file types
# Upload a bw file
# Upload a bg file with an extra bw file
# Check the upload key is the bw's key

def run_tests():
    unittest.main(module='chalicelib.tests.test_higlass_checks', defaultTest='TestPostHiglassViewConfsFiles')
