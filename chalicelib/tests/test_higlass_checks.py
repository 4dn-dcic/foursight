import unittest
import json

import requests

from dcicutils import ff_utils
import app

# Manual test script for higlass checks.

# Track all created files by the server route used to create them.

# Look for reference files for the given genome assembly
#) If they don't exist, make them and mark them to be deleted later

# Delete all files added during these tests

class TestGenerateHiglassViewConfFiles(unittest.TestCase):
    file_formats = {}
    files_created_by_type = {}
    connection = None

    @classmethod
    def setUpClass(cls):
        TestGenerateHiglassViewConfFiles.file_formats = {}

        # Set up connection to test server
        TestGenerateHiglassViewConfFiles.connection = app.init_connection('mastertest')

        # Get authentication
        ff_auth = (TestGenerateHiglassViewConfFiles.connection.ff_keys['key'], TestGenerateHiglassViewConfFiles.connection.ff_keys['secret'])
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Get the uuid to file format mappings.
        try:
            response = requests.get(
                TestGenerateHiglassViewConfFiles.connection.ff_server + 'file-formats/?type=FileFormat&limit=all',
                auth=ff_auth,
                headers=headers
            )
        except Exception as e:
            raise

        if response.status_code == 200:
            # Populate the file formats by display_title.
            for raw_format in response.json()["@graph"]:
                TestGenerateHiglassViewConfFiles.file_formats[raw_format["display_title"]] = raw_format

    def setUp(self):
        pass

    def tearDown(self):
        ''' Delete all files added during these tests
        '''

        # Make authority and headers
        ff_auth = (TestGenerateHiglassViewConfFiles.connection.ff_keys['key'], TestGenerateHiglassViewConfFiles.connection.ff_keys['secret'])
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        reference_file_types = ("/file-formats/beddb/", "/file-formats/chromsizes/")
        processed_file_types = ("/file-formats/mcool/")

        for file_type in TestGenerateHiglassViewConfFiles.files_created_by_type:

            ff_endpoint = None
            if file_type in reference_file_types:
                ff_endpoint = TestGenerateHiglassViewConfFiles.connection.ff_server + 'files-reference'
            elif file_type in processed_file_types:
                ff_endpoint = TestGenerateHiglassViewConfFiles.connection.ff_server + 'files-processed'

            for file in TestGenerateHiglassViewConfFiles.files_created_by_type[file_type]:
                uuid_data = {
                    "uuid": file["uuid"]
                }

                # Try to delete the file
                try:
                    response = requests.delete(
                        ff_endpoint + "/" + file["uuid"] + "/",
                        data=json.dumps(uuid_data),
                        auth=ff_auth,
                        headers=headers
                    )
                except Exception as e:
                    continue

                # Delete all view conf files
        pass

    def add_processed_file(self, accession, genome_assembly, higlass_uid, endpoint_url='files-processed', extra_data=None):
        ''' Add file with the given genome assembly and accession
        Also add higlass_uid (if given)
        '''

        # Make dummy data and overwrite with extra_data
        file_data = {
            "lab": "/labs/4dn-dcic-lab/",
            "status": "uploaded",
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
        file_type = new_file["file_format"] # TODO this will be like "/file-formats/beddb/", did you want only beddb ?

        # Add file to files_created_by_type
        if not file_type in TestGenerateHiglassViewConfFiles.files_created_by_type:
            TestGenerateHiglassViewConfFiles.files_created_by_type[file_type] = []
        TestGenerateHiglassViewConfFiles.files_created_by_type[file_type].append(new_file)

        return new_file["uuid"]

    def test_check_found_viewconf_to_generate(self):
        ''' Create a new mcool file.
        After running the generate_higlass_view_confs_files check, task will note a new file to create.
        '''

        # Add reference files for Human genome assembly.
        chromsize_uuid = self.add_processed_file("TESTCHROMSIZES00", "GRCh38", "higlass_uid00",
            endpoint_url='files-reference',
            extra_data={
                "file_format" : TestGenerateHiglassViewConfFiles.file_formats["chromsizes"]["uuid"],
                "tags": ["higlass_reference"],
                "file_classification": "ancillary file",
            }
        )

        beddb_uuid = self.add_processed_file("TESTBEDDB01", "GRCh38", "higlass_uid01",
            endpoint_url='files-reference',
            extra_data={
                "file_format" : TestGenerateHiglassViewConfFiles.file_formats["beddb"]["uuid"],
                "tags": ["higlass_reference"],
                "file_classification": "ancillary file",
            }
        )

        # Add an mcool file. It will have a Human genome assembly.
        file_uuid = self.add_processed_file("TESTMCOOL00", "GRCh38", "higlass_uid02", extra_data={
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

        # Both human reference files should be referenced
        self.assertTrue("GRCh38" in check["full_output"]['reference_files'])
        self.assertTrue(chromsize_uuid in check["full_output"]['reference_files']['GRCh38'])
        self.assertTrue(beddb_uuid in check["full_output"]['reference_files']['GRCh38'])

        # Status = PASS
        self.assertEqual("PASS", check["status"])

    def test_autogenerated_files_dont_generate(self):
        '''Add a file that already has an auto_generated_higlass_view_config.
        When the check runs it should not mark this file as requiring a new view config.
        '''

        # Add reference files for Human genome assembly.
        chromsize_uuid = self.add_processed_file("TESTCHROMSIZES00", "GRCh38", "higlass_uid00",
            endpoint_url='files-reference',
            extra_data={
                "file_format" : TestGenerateHiglassViewConfFiles.file_formats["chromsizes"]["uuid"],
                "tags": ["higlass_reference"],
                "file_classification": "ancillary file",
            }
        )

        beddb_uuid = self.add_processed_file("TESTBEDDB01", "GRCh38", "higlass_uid01",
            endpoint_url='files-reference',
            extra_data={
                "file_format" : TestGenerateHiglassViewConfFiles.file_formats["beddb"]["uuid"],
                "tags": ["higlass_reference"],
                "file_classification": "ancillary file",
            }
        )

        # Add an mcool file. It will have a Human genome assembly.
        # Add a auto_generated_higlass_view_config field.
        file_uuid = self.add_processed_file("TESTMCOOL00", "GRCh38", "higlass_uid02", extra_data={
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

        # Both human reference files should be referenced
        self.assertTrue("GRCh38" in check["full_output"]['reference_files'])
        self.assertTrue(chromsize_uuid in check["full_output"]['reference_files']['GRCh38'])
        self.assertTrue(beddb_uuid in check["full_output"]['reference_files']['GRCh38'])

        # Status = PASS
        self.assertEqual("PASS", check["status"])

# File has different genome assembly
# No view configs to generate

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
