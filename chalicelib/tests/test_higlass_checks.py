import unittest
import json

import requests

from dcicutils import ff_utils
import app

# Manual test script for higlass checks.

# Track all created files by the server route used to create them.

def add_processed_file(accession, genome_assembly, higlass_uid, endpoint_url='files-processed', extra_data=None):
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
    ff_endpoint = TestGenerateHiglassViewConfFiles.connection.ff_server + endpoint_url
    ff_auth = (TestGenerateHiglassViewConfFiles.connection.ff_keys['key'], TestGenerateHiglassViewConfFiles.connection.ff_keys['secret'])
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

    # Get the file type
    file_type = new_file["file_format"]

    return new_file["uuid"]

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
                add_processed_file(new_reference_files_by_ga[genome_assembly][filetype]["accession"], genome_assembly, new_reference_files_by_ga[genome_assembly][filetype]["higlass_uid"],
                    endpoint_url='files-reference',
                    extra_data={
                        "file_format" : file_formats[filetype]["uuid"],
                        "tags": ["higlass_reference"],
                        "file_classification": "ancillary file",
                    }
                )

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
        file_uuid = add_processed_file("TESTMCOOL01", "GRCh38", "higlass_uid02", extra_data={
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
        file_uuid = add_processed_file("TESTMCOOL01", "GRCh38", "higlass_uid02", extra_data={
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
        file_uuid = add_processed_file("TESTMCOOL02", "GRCm38", "higlass_uid03", extra_data={
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

# ref_files_by_ga = gen_check_result['full_output'].get('reference_files', {})
# for ga in gen_check_result['full_output'].get('target_files', {}):
# Read response to get uuid
#                     viewconf_res = ff_utils.post_metadata(view_conf, 'higlass-view-configs',
#                                                          key=connection.ff_keys, ff_env=connection.ff_env)
# Get static content to look for tab:higlass info
#            file_res = ff_utils.get_metadata(file, key=connection.ff_keys, ff_env=connection.ff_env)
#            file_static_content = file_res.get('static_content', [])
# Check to see it posted
#            new_view_conf_sc = {
#                'location': 'tab:higlass',
#                'content': view_conf_uuid,
#                'description': 'auto_generated_higlass_view_config'
#            }


# Action: Post higlass_viewconf_file
# Post a file with a Human genome assembly
# Check on it and make sure it's ready to post
# Call function it should complete
# status = DONE
# You can find the viewconf for this file
# Make sure there is an auto_generated_higlass_view_config static content tag.

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
    unittest.main(module='chalicelib.tests.test_higlass_checks')
