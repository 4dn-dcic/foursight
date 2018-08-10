'''
NOTICE: THIS FILE (and google client library dependency) IS TEMPORARY AND WILL BE MOVED TO **DCICUTILS** AFTER MORE COMPLETE
'''

from apiclient.discovery import build
from google.oauth2.service_account import Credentials
from dcicutils import (
    ff_utils,
    s3_utils
)



DEFAULT_GOOGLE_API_CONFIG = {
    "scopes" : ['https://www.googleapis.com/auth/analytics.readonly'],
    "analytics_view_id" : '132680007'
}


class GoogleAPISyncer:
    '''
    Handles authentication and common requests against Google APIs using `fourfront-ec2-account` (a service_account).
    If no access keys are provided, initiates a connection to production.

    For testing against localhost, please provide a `ff_access_keys` dictionary with server=localhost:8000 and key & secret from there as well.

    Arguments:
        ff_access_keys      - Optional. A dictionary with a 'key', 'secret', and 'server', identifying admin account access keys and FF server to POST to.
        ff_env              - Optional. Provide in place of `ff_access_keys` or `s3UtilsInstance` to automatically grab credentials from S3. Defaults to 'data'.
        google_api_key      - Optional. Override default API key for accessing Google.
        s3UtilsInstance     - Optional. Provide an S3Utils class instance which is connecting to DATA fourfront environment in order
                              to obtain proper Google API key (if none supplied otherwise).
                              If not supplied, a new S3 connection will be created using `ff_env`.
        extra_config        - Additional Google API config, e.g. OAuth2 scopes and Analytics View ID. Shouldn't need to set this.
    '''

    DEFAULT_CONFIG = DEFAULT_GOOGLE_API_CONFIG

    @staticmethod
    def validate_api_key_format(json_api_key):
        try:
            assert json_api_key is not None
            assert isinstance(json_api_key, dict)
            assert json_api_key['type'] == 'service_account'
            assert json_api_key["project_id"] == "fourdn-fourfront"
            for dict_key in ['private_key_id', 'private_key', 'client_email', 'client_id', 'auth_uri', 'client_x509_cert_url']:
                assert json_api_key[dict_key]
        except:
            return False
        return True

    def __init__(
        self,
        ff_access_keys      = None,
        ff_env              = "data",
        google_api_key      = None,
        s3UtilsInstance     = None,
        extra_config        = DEFAULT_GOOGLE_API_CONFIG
    ):
        '''Authenticate with Google APIs and initialize sub-class instances.'''
        if s3UtilsInstance is None:
            self._s3Utils = s3_utils.s3Utils(env=ff_env) # Google API Keys are stored on production bucket only ATM.
        else:
            self._s3Utils = s3UtilsInstance

        if google_api_key is None:
            self._api_key = self._s3Utils.get_google_key()
            if not self._api_key:
                raise Exception("Failed to get Google API key from S3.")
        else:
            self._api_key = google_api_key

        if not GoogleAPISyncer.validate_api_key_format(self._api_key):
            raise Exception("Google API Key is in invalid format.")


        self.oauth_scopes = extra_config.get('scopes', DEFAULT_GOOGLE_API_CONFIG['scopes'])
        self.credentials = Credentials.from_service_account_info(self._api_key, scopes=self.oauth_scopes)

        if ff_access_keys is None:
            ff_access_keys = self._s3Utils.get_access_keys()

        self.server = ff_access_keys['server']
        self.access_key = {
            "key"   : ff_access_keys['key'],
            "secret": ff_access_keys['secret']
        }

        # Init sub-class objects
        self.analytics  = GoogleAPISyncer.AnalyticsAPI(self, extra_config)
        #self.sheets     = GoogleAPISyncer.SheetsAPI(self, extra_config)
        #self.docs       = GoogleAPISyncer.DocsAPI(self, extra_config)


    class AnalyticsAPI:
        '''Interface for accessing Google Analytics data using our Google API Syncer credentials'''

        def __init__(self, syncer_instance, extra_config=DEFAULT_GOOGLE_API_CONFIG):
            self.owner = syncer_instance
            self.view_id = extra_config.get('analytics_view_id', DEFAULT_GOOGLE_API_CONFIG['analytics_view_id'])
            if not self.owner.credentials:
                raise Exception("No Google API credentials set.")
            self._api = build('analyticsreporting', 'v4', credentials=self.owner.credentials)


        def query_reports(self, report_requests, **kwargs):
            '''
            Run a query to Google Analytics API
            Accepts either a list of reportRequests (according to Google Analytics API Docs) and returns their results,
            or a list of strings which reference AnalyticsAPI methods (aside from this one).

            Arguments:
                report_requests - A list of reportRequests.
            '''

            def process_report_request_type(report_request, **kwargs):
                if isinstance(report_request, str): # Convert string to dict by executing AnalyticsAPI[report_request](**kwargs)
                    report_request = getattr(self, report_request)(execute=False, **kwargs)
                return dict(report_request, viewId=self.view_id)

            return self._api.reports().batchGet(body={
                "reportRequests" : [ process_report_request_type(r, **kwargs) for r in report_requests ]
            }).execute()


        def sessions_by_country(self, start_date='2daysAgo', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'metrics': [{'expression': 'ga:sessions'}],
                'dimensions': [{'name': 'ga:country'}]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json

        
        def detail_views_by_file(self, start_date='2daysAgo', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'metrics': [
                    {'expression': 'ga:productDetailViews'}
                ],
                'dimensions': [
                    {'name': 'ga:productName'},
                    {'name': 'ga:productCategoryLevel2'}
                ],
                # TODO: Filter by `productCategoryLevel1 == File``
                #'dimensionFilters' : [
                #    {}
                #]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


    class SheetsAPI:
        '''
        Use this sub-class to help query, read, edit, and analyze spreadsheet data, which will be returned in form of multi-dimensional JSON array.

        TODO: Implement
        '''
        pass


    class DocsAPI:
        '''
        Use this sub-class to help query, read, and edit Google Doc data.

        TODO: Implement
        '''
        pass

