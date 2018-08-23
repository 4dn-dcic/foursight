'''
NOTICE: THIS FILE (and google client library dependency) IS TEMPORARY AND WILL BE MOVED TO **DCICUTILS** AFTER MORE COMPLETE
'''

import inspect
from datetime import (
    date,
    timedelta
)
from collections import OrderedDict
from apiclient.discovery import build
from google.oauth2.service_account import Credentials
from dcicutils import (
    ff_utils,
    s3_utils
)



DEFAULT_GOOGLE_API_CONFIG = {
    "scopes" : [                                                # Descriptions from: https://developers.google.com/identity/protocols/googlescopes
        'https://www.googleapis.com/auth/analytics.readonly',   # View your Google Analytics data
        'https://www.googleapis.com/auth/drive',                # View and manage the files in your Google Drive
        'https://www.googleapis.com/auth/drive.file',           # View and manage Google Drive files and folders that you have opened or created with this app
        'https://www.googleapis.com/auth/spreadsheets'          # View and manage your spreadsheets in Google Drive

    ],
    "analytics_view_id" : '132680007',
    "analytics_page_size" : 10000
}


class _NestedGoogleServiceAPI:
    '''Used as common base class for nested classes of GoogleAPISyncer.'''
    def __init__(self, syncer_instance):
        self.owner = syncer_instance
        if not self.owner.credentials:
            raise Exception("No Google API credentials set.")


def report(func, disabled=False):
    '''Decorator for AnalyticsAPI'''
    if disabled:
        return func
    setattr(func, 'is_report_provider', True)
    return func


class GoogleAPISyncer:
    '''
    Handles authentication and common requests against Google APIs using `fourfront-ec2-account` (a service_account).
    If no access keys are provided, initiates a connection to production.

    Interfaces with Google services using Google API version 4 ('v4').

    For testing against localhost, please provide a `ff_access_keys` dictionary with server=localhost:8000 and key & secret from there as well.

    Arguments:
        ff_access_keys      - Optional. A dictionary with a 'key', 'secret', and 'server', identifying admin account access keys and FF server to POST to.
        ff_env              - Optional. Provide in place of `ff_access_keys` to automatically grab credentials from S3. Defaults to 'fourfront-webprod'.
        google_api_key      - Optional. Override default API key for accessing Google.
        s3UtilsInstance     - Optional. Provide an S3Utils class instance which is connecting to `fourfront-webprod` fourfront environment in order
                              to obtain proper Google API key (if none supplied otherwise).
                              If not supplied, a new S3 connection will be created to `fourfront-webprod`.
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
            self._s3Utils = s3_utils.s3Utils(env="fourfront-webprod") # Google API Keys are stored on production bucket only ATM.
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

        self.extra_config = extra_config
        self.credentials = Credentials.from_service_account_info(
            self._api_key,
            scopes=self.extra_config.get('scopes', DEFAULT_GOOGLE_API_CONFIG['scopes'])
        )

        # These are required only for POSTing/GETing data for TrackingInfo items.
        if ff_access_keys is None:
            ff_access_keys = self._s3Utils.get_access_keys()

        self.server = ff_access_keys['server']
        self.access_key = {
            "key"   : ff_access_keys['key'],
            "secret": ff_access_keys['secret']
        }

        # Init sub-class objects
        self.analytics  = GoogleAPISyncer.AnalyticsAPI(self)
        self.sheets     = GoogleAPISyncer.SheetsAPI(self)
        self.docs       = GoogleAPISyncer.DocsAPI(self)


    class AnalyticsAPI(_NestedGoogleServiceAPI):
        '''
        Interface for accessing Google Analytics data using our Google API Syncer credentials.

        Relevant Documentation:
        https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
        '''

        @staticmethod
        def transform_report_result(raw_result, save_raw_values=False):
            '''
            Transform raw responses (multi-dimensional array) in result to a more usable list-of-dictionaries structure.

            Arguments:
                raw_result - A dictionary containing at minimum `reports` (as delivered from) and `report_key_frames`

            Returns:
                A dictionary with `start_date`, `end_date`, `date_requested`, and parsed reports as `reports`.
            '''

            def format_metric_value(row, metric_dict, metric_index):
                '''Parses value from row into a numerical format, if necessary.'''
                value = row['metrics'][0]["values"][metric_index]
                type = metric_dict['type']
                if type == 'INTEGER':
                    value = int(value)
                elif type in ('FLOAT', 'CURRENCY'):
                    value = float(value)
                elif type == 'PERCENT':
                    value = float(value) / 100
                return value

            def report_to_json_items(report):
                # [(0, "ga:productName"), (1, "ga:productSku"), ...]
                dimension_keys = list(enumerate(report['columnHeader'].get('dimensions', [])))
                # [(0, { "name": "ga:productDetailViews", "type": "INTEGER" }), (1, { "name": "ga:productListClicks", "type": "INTEGER" }), ...]
                metric_key_definitions = list(enumerate(report['columnHeader'].get('metricHeader', []).get('metricHeaderEntries', [])))
                return_items = []
                for row_index, row in enumerate(report.get('data', {}).get('rows', [])):
                    list_item = { dk : row['dimensions'][dk_index] for (dk_index, dk) in dimension_keys }
                    list_item = dict(list_item, **{
                        mk_dict['name'] : format_metric_value(row, mk_dict, mk_index)
                        for (mk_index, mk_dict) in metric_key_definitions
                    })
                    return_items.append(list_item)
                return return_items

            def parse_google_api_date(date_requested):
                '''Returns ISO format date. TODO: Return Python3 date when date.fromisoformat() is available (Python v3.7+)'''
                if date_requested == 'yesterday':
                    return (date.today() - timedelta(days=1)).isoformat()
                if 'daysAgo' in date_requested:
                    days_ago = int(date_requested.replace('daysAgo', ''))
                    return (date.today() - timedelta(days=days_ago)).isoformat()
                return date_requested # Assume already in ISO format.

            parsed_reports = OrderedDict()

            for idx, report_key_name in enumerate(raw_result['report_key_names']):
                if save_raw_values:
                    parsed_reports[report_key_name] = {
                        "request"       : raw_result['requests'][idx],
                        "raw_report"    : raw_result['reports'][idx],
                        "results"       : report_to_json_items(raw_result['reports'][idx])
                    }
                else:
                    parsed_reports[report_key_name] = report_to_json_items(raw_result['reports'][idx])

            # `start_date` and `end_date` must be same for all requests (defined in Google API docs).
            if len(raw_result['requests']) > 0:
                common_start_date   = raw_result['requests'][0]['dateRanges'][0].get('startDate', '7daysAgo')   # Google API default
                common_end_date     = raw_result['requests'][0]['dateRanges'][0].get('endDate', 'yesterday')    # Google API default
                if common_start_date:
                    common_start_date = parse_google_api_date(common_start_date)
                if common_end_date:
                    common_end_date = parse_google_api_date(common_end_date)

            return { 
                "reports"       : parsed_reports,
                "start_date"    : common_start_date,
                "end_date"      : common_end_date,
            }

        def __init__(self, syncer_instance):
            _NestedGoogleServiceAPI.__init__(self, syncer_instance)
            self.view_id = self.owner.extra_config.get('analytics_view_id', DEFAULT_GOOGLE_API_CONFIG['analytics_view_id'])
            self._api = build('analyticsreporting', 'v4', credentials=self.owner.credentials)

        
        def get_report_provider_method_names(self):
            '''Gets every single report defined and marked with `@report` decorator.'''
            report_requests = []
            for method_name in GoogleAPISyncer.AnalyticsAPI.__dict__.keys():
                method_instance = getattr(self, method_name)
                if method_instance and getattr(method_instance, 'is_report_provider', False):
                    report_requests.append(method_name)
            return report_requests


        def query_reports(self, report_requests=None, **kwargs):
            '''
            Run a query to Google Analytics API
            Accepts either a list of reportRequests (according to Google Analytics API Docs) and returns their results,
            or a list of strings which reference AnalyticsAPI methods (aside from this one).

            See: https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportRequest

            Arguments:
                report_requests - A list of reportRequests.
            
            Returns:
                Parsed and transformed analytics data. See: GoogleAPISyncer.AnalyticsAPI.transform_report_result()
            '''

            if report_requests is None:
                report_requests = self.get_report_provider_method_names()

            report_key_names = None

            all_reports_requested_as_strings = True
            for r in report_requests:
                if not isinstance(r, str):
                    all_reports_requested_as_strings = False
                    break

            if all_reports_requested_as_strings:
                report_key_names = report_requests
            else:
                # THIS DEPENDS ON CPYTHON TO WORK. PyPy or Jython = no go.
                caller_method = None
                try:
                    curframe = inspect.currentframe()
                    caller_frame = inspect.getouterframes(curframe, 2)
                    caller_method = caller_frame[1][3]
                except:
                    pass
                if isinstance(caller_method, str) and hasattr(self, caller_method):
                    report_key_names = [caller_method]

            if report_key_names is None:
                raise Exception("Cant determine report key names.")

            def process_report_request_type(report_request, **kwargs):
                if isinstance(report_request, str): # Convert string to dict by executing AnalyticsAPI[report_request](**kwargs)
                    report_request = getattr(self, report_request)(execute=False, **kwargs)

                return dict(report_request, # Add required common key/vals, see https://developers.google.com/analytics/devguides/reporting/core/v4/basics.
                    viewId=self.view_id,
                    pageSize=self.owner.extra_config.get('analytics_page_size', DEFAULT_GOOGLE_API_CONFIG['analytics_page_size'])
                )

            formatted_report_requests = [ process_report_request_type(r, **kwargs) for r in report_requests ]

            # We get back as raw_result:
            #   { "reports" : [{ "columnHeader" : { "dimensions" : [Xh, Yh, Zh], "metricHeaderEntries" : [{ "name" : 1h, "type" : "INTEGER" }, ...] }, "data" : { "rows": [{ "dimensions" : [X,Y,Z], "metrics" : [1,2,3,4] }] }  }, { .. }, ....] }
            raw_result = self._api.reports().batchGet(body={ "reportRequests" : formatted_report_requests }).execute()
            raw_result['requests'] = formatted_report_requests
            raw_result['report_key_names'] = report_key_names
            # This transforms raw_result["reports"] into more usable data structure for ES and aggregation
            #   e.g. list of JSON items instead of multi-dimensional table representation
            return self.transform_report_result(raw_result)     # same as GoogleAPISyncer.AnalyticsAPI.transform_report_result(raw_result)

        def get_latest_tracking_item_end_date(self):
            '''
            IN PROGRESS

            TODO:
            Ideally we want to return a datetime.date here instead of ISOFORMAT.
            This is not supported until Python 3.7 though (without extra libraries). See: https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat
            Once available we can easily get subsequent date range data with:
            ```
                google.analytics.query_reports(start_date=google.analytics.get_latest_tracking_item_end_date() + timedelta(days=1), end_date='yesterday')
            ```

            This would be more reliable than `.query_reports(start_date='yesterday', end_date='yesterday')`.
            '''
            search_response = ff_utils.search_metadata(
                '/search/?type=TrackingItem&tracking_type=google_analytics&sort=-google_analytics.end_date&limit=1',
                key=dict(self.owner.access_key, server=self.owner.server)
            )
            if len(search_response.get('@graph', [])) == 0:
                # raise Exception("No existing Google Analytics Tracking Items found.")
                return None
            return search_response['@graph'][0]['google_analytics']['end_date']


        def create_tracking_item(self, report_data=None, do_post_request=False, **kwargs):
            '''
            Wraps `report_data` in a TrackingItem Item.

            If `do_post_request` is True, will also POST the Item into fourfront database, according to the access_keys
            that the class was instantiated with.

            If `report_data` is not supplied or set to None, will run query_reports() to get all reports defined as are defined in instance methods.
            '''
            if report_data is None:
                report_data = self.query_reports(**kwargs)
                # TODO:
                #   start_date = (self.get_latest_tracking_item_end_date + timedelta(days=1)).isoformat()
                #   if start_date == date.today().isoformat():
                #       raise Exception("Already have latest data.")
                #   report_data = self.query_reports(start_date=start_date)

            # First make sure _all_ reporting methods defined on this class are included. Otherwise we might have tracking items in DB with missing data.
            for method_name in self.get_report_provider_method_names():
                if report_data['reports'].get(method_name) is None:
                    raise Exception("Not all potentially available data is included in report_data. Exiting.")
                if not isinstance(report_data['reports'][method_name], list):
                    raise Exception("Can only make tracking_item for report_data which does not contain extra raw report and request data, per the schema.")

            tracking_item = {
                "status"            : "released",
                "tracking_type"     : "google_analytics",
                "google_analytics"  : report_data
            }
            if do_post_request:
                response = ff_utils.post_metadata(tracking_item, 'tracking-items', key=dict(self.owner.access_key, server=self.owner.server))
                return response['@graph'][0]
            else:
                return tracking_item

        @report
        def sessions_by_country(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'metrics': [
                    { 'expression': 'ga:sessions', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:sessionsPerUser' },
                    { 'expression': 'ga:avgSessionDuration' },
                    { 'expression': 'ga:bounceRate' }
                ],
                'dimensions': [
                    { 'name': 'ga:country' }
                ]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


        @report
        def detail_views_by_file(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'metrics': [
                    { 'expression': 'ga:productDetailViews', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListClicks', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListViews', 'formattingType' : 'INTEGER' }
                ],
                'dimensions': [
                    { 'name': 'ga:productName' },
                    { 'name': 'ga:productSku' },
                    { 'name': 'ga:productCategoryLevel2' },
                    { 'name': 'ga:productBrand' }
                ],
                "orderBys" : [{ 'fieldName' : 'ga:productDetailViews', 'sortOrder' : 'descending' }],
                'dimensionFilterClauses' : [
                    {
                        "filters" : [
                            { "dimensionName" : "ga:productCategoryLevel1", "expressions" : ["File"], "operator" : "EXACT" }
                        ]
                    }
                ]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


        @report
        def detail_views_by_experiment_set(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'metrics': [
                    { 'expression': 'ga:productDetailViews', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListClicks', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListViews', 'formattingType' : 'INTEGER' }
                ],
                'dimensions': [
                    { 'name': 'ga:productName' },
                    { 'name': 'ga:productSku' },
                    { 'name': 'ga:productCategoryLevel2' },
                    { 'name': 'ga:productBrand' }
                ],
                "orderBys" : [{ 'fieldName' : 'ga:productDetailViews', 'sortOrder' : 'descending' }],
                'dimensionFilterClauses' : [
                    {
                        "filters" : [
                            { "dimensionName" : "ga:productCategoryLevel1", "expressions" : ["ExperimentSet"], "operator" : "EXACT" }
                        ]
                    }
                ]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


    class SheetsAPI(_NestedGoogleServiceAPI):
        '''
        Use this sub-class to help query, read, edit, and analyze spreadsheet data, which will be returned in form of multi-dimensional JSON array.

        TODO: Implement
        '''
        pass


    class DocsAPI(_NestedGoogleServiceAPI):
        '''
        Use this sub-class to help query, read, and edit Google Doc data.

        TODO: Implement
        '''
        pass

