"""
NOTICE: THIS FILE (and google client library dependency) IS TEMPORARY AND WILL BE MOVED TO **DCICUTILS** AFTER MORE COMPLETE
"""

import inspect
from datetime import (
    date,
    datetime,
    timedelta
)
import pytz
from types import FunctionType
from calendar import monthrange
from collections import OrderedDict
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dcicutils import ff_utils, s3_utils



DEFAULT_GOOGLE_API_CONFIG = {
    "scopes" : [                                                # Descriptions from: https://developers.google.com/identity/protocols/googlescopes
        'https://www.googleapis.com/auth/analytics.readonly',   # View your Google Analytics data
        'https://www.googleapis.com/auth/drive',                # View and manage the files in your Google Drive
        'https://www.googleapis.com/auth/drive.file',           # View and manage Google Drive files and folders that you have opened or created with this app
        'https://www.googleapis.com/auth/spreadsheets'          # View and manage your spreadsheets in Google Drive

    ],
    "analytics_view_id" : '132680007',
    "analytics_page_size" : 10000,
    "analytics_timezone" : "US/Eastern",                    # 4DN Analytics account is setup for EST time zone.
    "analytics_dimension_name_map" : {
        "currentFilters"    : 1,
        "name"              : 2,
        "field"             : 3,
        "term"              : 4,
        "experimentType"    : 5,
        "userGroups"        : 6
    },
    "analytics_metric_name_map" : {
        "filesize"          : 1,
        "downloads"         : 2
    },
}


class _NestedGoogleServiceAPI:
    """Used as common base class for nested classes of GoogleAPISyncer."""
    def __init__(self, syncer_instance):
        self.owner = syncer_instance
        if not self.owner.credentials:
            raise Exception("No Google API credentials set.")


def report(*args, disabled=False):
    """Decorator for AnalyticsAPI"""
    def decorate_func(func):
        if disabled:
            return func
        setattr(func, 'is_report_provider', True)
        return func
    if len(args) == 1 and isinstance(args[0], FunctionType):
        return decorate_func(args[0])
    else:
        return decorate_func


class GoogleAPISyncer:
    """
    Handles authentication and common requests against Google APIs using `fourfront-ec2-account` (a service_account).
    If no access keys are provided, initiates a connection to production.

    Interfaces with Google services using Google API version 4 ('v4').

    For testing against localhost, please provide a `ff_access_keys` dictionary with server=localhost:8000 and key & secret from there as well.

    Arguments:
        ff_access_keys      - Optional. A dictionary with a 'key', 'secret', and 'server', identifying admin account access keys and FF server to POST to.
        google_api_key      - Optional. Override default API key for accessing Google.
        s3UtilsInstance     - Optional. Provide an S3Utils class instance connected to a bucket with a proper Google API key (if none supplied otherwise).
                              If not supplied, a new S3 connection will be created to the Fourfront production bucket.
        extra_config        - Additional Google API config, e.g. OAuth2 scopes and Analytics View ID. Shouldn't need to set this.
    """

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
        google_api_key      = None,
        s3UtilsInstance     = None,
        extra_config        = DEFAULT_GOOGLE_API_CONFIG
    ):
        """Authenticate with Google APIs and initialize sub-class instances."""
        if s3UtilsInstance is None:
            self._s3Utils = s3_utils.s3Utils(env='data')  # Google API Keys are stored on production bucket only ATM.
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
        """
        Interface for accessing Google Analytics data using our Google API Syncer credentials.

        Relevant Documentation:
        https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
        """

        def transform_report_result(self, raw_result, save_raw_values=False, date_increment="daily"):
            """
            Transform raw responses (multi-dimensional array) from Google Analytics to a more usable
            list-of-dictionaries structure.

            Arguments:
                raw_result - A dictionary containing at minimum `reports` (as delivered from) and `report_key_frames`

            Returns:
                A dictionary with `start_date`, `end_date`, `date_requested`, and parsed reports as `reports`.
            """

            def format_metric_value(row, metric_dict, metric_index):
                """Parses value from row into a numerical format, if necessary."""
                value = row['metrics'][0]["values"][metric_index]
                type = metric_dict['type']
                if type == 'INTEGER':
                    value = int(value)
                elif type in ('FLOAT', 'CURRENCY', 'TIME'):
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
                """
                Returns ISO-formatted date of a date string sent to Google Analytics.
                Translates 'yesterday', 'XdaysAgo', from `date.today()` appropriately.
                TODO: Return Python3 date when date.fromisoformat() is available (Python v3.7+)
                TODO: Handle 'today' and maybe other date string options.
                """

                tz = pytz.timezone(self.owner.extra_config.get("analytics_timezone", "US/Eastern"))
                today = datetime.now(tz).date()
                if date_requested == 'yesterday':
                    return (today - timedelta(days=1)).isoformat()
                if 'daysAgo' in date_requested:
                    days_ago = int(date_requested.replace('daysAgo', ''))
                    return (today - timedelta(days=days_ago)).isoformat()
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

            for_date = None

            # `start_date` and `end_date` must be same for all requests (defined in Google API docs) in a batchRequest, so we're ok getting from just first 1
            if len(raw_result['requests']) > 0:
                common_start_date   = raw_result['requests'][0]['dateRanges'][0].get('startDate', '7daysAgo')   # Google API default
                common_end_date     = raw_result['requests'][0]['dateRanges'][0].get('endDate', 'yesterday')    # Google API default
                if common_start_date:
                    common_start_date = parse_google_api_date(common_start_date)
                if common_end_date:
                    common_end_date = parse_google_api_date(common_end_date)
                # They should be the same
                if date_increment == 'daily' and common_end_date != common_start_date:
                    raise Exception('Expected 1 day interval(s) for analytics, but startDate and endDate are different.')
                if date_increment == 'monthly' and common_end_date[0:7] != common_start_date[0:7]:
                    raise Exception('Expected monthly interval(s) for analytics, but startDate and endDate "YYYY-MD" are different.')
                for_date = common_start_date

            return {
                "reports"        : parsed_reports,
                "for_date"       : for_date,
                "date_increment" : date_increment
            }



        def __init__(self, syncer_instance):
            _NestedGoogleServiceAPI.__init__(self, syncer_instance)
            self.view_id = self.owner.extra_config.get('analytics_view_id', DEFAULT_GOOGLE_API_CONFIG['analytics_view_id'])
            self._api = build('analyticsreporting', 'v4', credentials=self.owner.credentials, cache_discovery=False)



        def get_report_provider_method_names(self):
            """
            Collects name of every single method defined on this classes which is
            marked with `@report` decorator (non-disabled) and returns in form of list.
            """
            report_requests = []
            for method_name in GoogleAPISyncer.AnalyticsAPI.__dict__.keys():
                method_instance = getattr(self, method_name)
                if method_instance and getattr(method_instance, 'is_report_provider', False):
                    report_requests.append(method_name)
            return report_requests



        def query_reports(self, report_requests=None, **kwargs):
            """
            Run a query to Google Analytics API
            Accepts either a list of reportRequests (according to Google Analytics API Docs) and returns their results,
            or a list of strings which reference AnalyticsAPI methods (aside from this one).

            See: https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportRequest

            Arguments:
                report_requests - A list of reportRequests.

            Returns:
                Parsed and transformed analytics data. See: GoogleAPISyncer.AnalyticsAPI.transform_report_result()
            """

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
                    report_request = getattr(self, report_request)(execute=False, **{ k:v for k,v in kwargs.items() if k in ('start_date', 'end_date') })

                return dict(report_request, # Add required common key/vals, see https://developers.google.com/analytics/devguides/reporting/core/v4/basics.
                    viewId=self.view_id,
                    pageSize=report_request.get('pageSize', self.owner.extra_config.get('analytics_page_size', DEFAULT_GOOGLE_API_CONFIG['analytics_page_size']))
                )

            formatted_report_requests = [ process_report_request_type(r, **kwargs) for r in report_requests ]

            # Google only permits 5 requests max within a batchRequest, so we need to chunk it up if over this -
            report_request_count = len(formatted_report_requests)
            if report_request_count > 5:
                raw_result = { "reports" : [] }
                for chunk_num in range(report_request_count // 5 + 1):
                    chunk_num_start = chunk_num * 5
                    chunk_num_end = min([chunk_num_start + 5, report_request_count])
                    for chunk_raw_res in self._api.reports().batchGet(body={ "reportRequests" : formatted_report_requests[chunk_num_start:chunk_num_end] }).execute().get('reports', []):
                        raw_result['reports'].append(chunk_raw_res)
            else:
                raw_result = self._api.reports().batchGet(body={ "reportRequests" : formatted_report_requests }).execute()

            # We get back as raw_result:
            #   { "reports" : [{ "columnHeader" : { "dimensions" : [Xh, Yh, Zh], "metricHeaderEntries" : [{ "name" : 1h, "type" : "INTEGER" }, ...] }, "data" : { "rows": [{ "dimensions" : [X,Y,Z], "metrics" : [1,2,3,4] }] }  }, { .. }, ....] }
            raw_result['requests'] = formatted_report_requests
            raw_result['report_key_names'] = report_key_names
            # This transforms raw_result["reports"] into more usable data structure for ES and aggregation
            #   e.g. list of JSON items instead of multi-dimensional table representation
            return self.transform_report_result(
                raw_result,
                date_increment=kwargs.get('increment')
            )



        def get_latest_tracking_item_date(self, increment="daily"):
            """
            Queries '/search/?type=TrackingItem&sort=-google_analytics.for_date&&google_analytics.date_increment=...'
            to get date of last TrackingItem for increment in database.

            TODO: Accept yearly once we want to collect & viz it.
            """
            if increment not in ('daily', 'monthly'):
                raise IndexError("increment parameter must be one of 'daily', 'monthly'")

            search_results = ff_utils.search_metadata(
                '/search/?type=TrackingItem&tracking_type=google_analytics&sort=-google_analytics.for_date&limit=1&google_analytics.date_increment=' + increment,
                key=dict(self.owner.access_key, server=self.owner.server),
                page_limit=1
            )
            if len(search_results) == 0:
                return None

            iso_date = search_results[0]['google_analytics']['for_date']

            # TODO: Use date.fromisoformat() once we're on Python 3.7
            year, month, day = iso_date.split('-', 2) # In python, months are indexed from 1 <= month <= 12, not 0 <= month <= 11 like in JS.
            return date(int(year), int(month), int(day))



        def fill_with_tracking_items(self, increment):
            '''
            This method is meant to be run periodically to fetch/sync Google Analytics data into Fourfront database.

            Adds 1 TrackingItem for each day to represent analytics data for said day.
            Fill up from latest already-existing TrackingItem until day before current day (to get full day of data).

            TODO:
            `date.fromisoformat(...)`  is not supported until Python 3.7 though (without extra libraries).
            See: https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat

            Args:
                increment - One of 'daily', 'monthly', or 'yearly'. Required.
            '''

            last_tracking_item_date = self.get_latest_tracking_item_date(increment=increment)
            tz = pytz.timezone(self.owner.extra_config.get("analytics_timezone", "US/Eastern"))
            today = datetime.now(tz).date()

            if last_tracking_item_date is None:
                if increment == 'daily':
                    # Fill up with last 60 days of Google Analytics data, if no other TrackingItem(s) yet exist.
                    # TODO: Set this to be ~45 for dev environment & 120 (or more) for production.
                    last_tracking_item_date = today - timedelta(days=61)
                    date_to_fill_from = last_tracking_item_date + timedelta(days=1)
                elif increment == 'monthly':
                    # Fill up with last 12 months Google Analytics data, if no other TrackingItem(s) yet exist.
                    month_to_fill_from = today.month - 12
                    year_to_fill_from = today.year
                    while month_to_fill_from < 1:
                        month_to_fill_from = 12 + month_to_fill_from
                        year_to_fill_from -= 1
                    date_to_fill_from = date(year_to_fill_from, month_to_fill_from, 1)
            else:
                if increment == 'daily':
                    # Day from which we begin to fill
                    date_to_fill_from = last_tracking_item_date + timedelta(days=1)
                elif increment == 'monthly':
                    month_to_fill_from = last_tracking_item_date.month + 1
                    year_to_fill_from = last_tracking_item_date.year
                    if month_to_fill_from > 12:
                        month_to_fill_from = 1
                        year_to_fill_from += 1
                    date_to_fill_from = date(year_to_fill_from, month_to_fill_from, 1)


            counter = 0
            created_list = []

            if increment == 'daily':
                end_date = today - timedelta(days=1)

                print("Filling daily items from", date_to_fill_from, "to", end_date)

                if date_to_fill_from > end_date:
                    return { 'created' : created_list, 'count' : counter }

                while date_to_fill_from <= end_date:
                    for_date_str = date_to_fill_from.isoformat()
                    response = self.create_tracking_item(
                        do_post_request = True,
                        start_date      = for_date_str,
                        end_date        = for_date_str,
                        increment       = increment
                    )
                    counter += 1
                    created_list.append(response['uuid'])
                    print('Created ' + str(counter) + ' TrackingItems so far.', date_to_fill_from)
                    date_to_fill_from += timedelta(days=1)


            elif increment == 'monthly':
                end_year = today.year
                end_month = today.month - 1
                fill_year = date_to_fill_from.year
                fill_month = date_to_fill_from.month
                if end_month == 0:
                    end_year -= 1
                    end_month += 12

                print("Filling monthly items from", date_to_fill_from, "to", str(end_year) + "-" + str(end_month))

                if fill_year > end_year and fill_month > end_month:
                    return { 'created' : created_list, 'count' : counter }

                while fill_year < end_year or (fill_year == end_year and fill_month <= end_month):
                    for_date_start_str = date(fill_year, fill_month, 1).isoformat()
                    for_date_end_str = date(fill_year, fill_month, monthrange(fill_year, fill_month)[1]).isoformat() # Last day of fill month
                    response = self.create_tracking_item(
                        do_post_request = True,
                        start_date      = for_date_start_str,
                        end_date        = for_date_end_str,
                        increment       = increment
                    )
                    counter += 1
                    created_list.append(response['uuid'])
                    print('Created ' + str(counter) + ' TrackingItems so far.', str(fill_year) + "-" + str(fill_month))
                    fill_month += 1
                    if fill_month > 12:
                        fill_month -= 12
                        fill_year += 1

            return { 'created' : created_list, 'count' : counter }



        def create_tracking_item(self, report_data=None, do_post_request=False, **kwargs):
            '''
            Wraps `report_data` in a TrackingItem Item.

            If `do_post_request` is True, will also POST the Item into fourfront database, according to the access_keys
            that the class was instantiated with.

            If `report_data` is not supplied or set to None, will run query_reports() to get all reports defined as are defined in instance methods.
            '''
            if report_data is None:
                report_data = self.query_reports(**kwargs)

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
                    { 'expression': 'ga:users', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:pageviews', 'formattingType' : 'INTEGER' },
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



        ################################
        ### Item Views & Impressions ###
        ################################


        @report
        def views_by_file(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'metrics': [
                    { 'expression': 'ga:productDetailViews', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListClicks', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListViews', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:uniquePurchases', 'formattingType' : 'INTEGER' }, # Total downloads + range queries
                    { 'expression': 'ga:metric' + str(self.owner.extra_config["analytics_metric_name_map"].get("downloads", 2)), 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:metric' + str(self.owner.extra_config["analytics_metric_name_map"].get("filesize", 1)), 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:calcMetric_PercentRangeQueries', 'formattingType' : 'INTEGER' }
                ],
                'dimensions': [
                    { 'name': 'ga:productName' },
                    { 'name': 'ga:productSku' },
                    { 'name': 'ga:productCategoryLevel2' },
                    { 'name': 'ga:productBrand' }
                ],
                "orderBys" : [
                    { 'fieldName' : 'ga:productDetailViews', 'sortOrder' : 'descending' },
                    { 'fieldName': 'ga:uniquePurchases', 'sortOrder' : 'descending' }
                ],
                'dimensionFilterClauses' : [
                    {
                        "filters" : [
                            {
                                "dimensionName" : "ga:productCategoryLevel1",
                                "expressions" : ["File"],
                                "operator" : "EXACT"
                            }
                        ]
                    }
                ],
                'pageSize' : 100
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


        @report
        def views_by_experiment_set(self, start_date='yesterday', end_date='yesterday', execute=True):
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
                ],
                'pageSize' : 100
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


        @report(disabled=True)
        def views_by_other_item(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'dimensions': [
                    { 'name': 'ga:productName' },
                    { 'name': 'ga:productSku' },
                    { 'name': 'ga:productCategoryHierarchy' },
                    { 'name': 'ga:productBrand' }
                ],
                'metrics': [
                    { 'expression': 'ga:productDetailViews', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListClicks', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListViews', 'formattingType' : 'INTEGER' }
                ],
                "orderBys" : [{ 'fieldName' : 'ga:productDetailViews', 'sortOrder' : 'descending' }],
                'dimensionFilterClauses' : [
                    {
                        "filters" : [
                            {
                                "not" : True,
                                "dimensionName" : "ga:productCategoryLevel1",
                                "expressions" : ["ExperimentSet"],
                                "operator" : "PARTIAL"
                            },
                            {
                                "not" : True,
                                "dimensionName" : "ga:productCategoryLevel1",
                                "expressions" : ["File"],
                                "operator" : "EXACT"
                            }
                        ]
                    }
                ],
                'pageSize' : 20
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json



        ################################
        ####### Search Analytics #######
        ################################


        @report(disabled=True)
        def search_search_queries(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'dimensions': [
                    { 'name': 'ga:searchKeyword' }
                ],
                'metrics': [
                    { 'expression': 'ga:users', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:sessions', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:pageviews', 'formattingType' : 'INTEGER' }
                ],
                "orderBys" : [{ 'fieldName' : 'ga:pageviews', 'sortOrder' : 'descending' }],
                'dimensionFilterClauses' : [
                    {
                        "filters" : [
                            {
                                "dimensionName" : "ga:searchDestinationPage",
                                "expressions" : ["/search/"],
                                "operator" : "PARTIAL"
                            }
                        ]
                    }
                ]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


        @report(disabled=True)
        def browse_search_queries(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'dimensions': [
                    { 'name': 'ga:searchKeyword' }
                ],
                'metrics': [
                    { 'expression': 'ga:users', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:sessions', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:pageviews', 'formattingType' : 'INTEGER' }
                ],
                "orderBys" : [{ 'fieldName' : 'ga:pageviews', 'sortOrder' : 'descending' }],
                'dimensionFilterClauses' : [
                    {
                        "filters" : [
                            {
                                "dimensionName" : "ga:searchDestinationPage",
                                "expressions" : ["/browse/"],
                                "operator" : "PARTIAL"
                            }
                        ]
                    }
                ]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json


        @report
        def fields_faceted(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = {
                'dateRanges' : [{ 'startDate' : start_date, 'endDate' : end_date }],
                'dimensions': [
                    #{ 'name': 'ga:eventLabel' }, # Too many distinct terms if we make it this granular.
                    { 'name': 'ga:dimension' + str(self.owner.extra_config["analytics_dimension_name_map"].get("field", 3)) }, # Field Name
                    #{ 'name': 'ga:dimension' + str(self.owner.extra_config["analytics_dimension_name_map"].get("term", 4)) }  # Term Name # # Too many distinct terms if we make it this granular.
                ],
                'metrics': [
                    { 'expression': 'ga:totalEvents', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:sessions', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:users', 'formattingType' : 'INTEGER' }
                ],
                "orderBys" : [{ 'fieldName' : 'ga:totalEvents', 'sortOrder' : 'descending' }],
                "dimensionFilterClauses" : [
                    {
                        "filters" : [
                            {
                                "dimensionName" : "ga:eventAction",
                                "expressions" : ["Set Filter"],
                                "operator" : "EXACT"
                            }
                        ]
                    }
                ]
            }
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json



        #################################
        #### File Download Analytics ####
        #################################

        # N.B. Some file download metrics are bundled into `views_by_file`

        def file_download_base_request_json(self, start_date='yesterday', end_date='yesterday'):
            '''Helper func for DRYness'''
            return {
                "dateRanges" : [{ 'startDate' : start_date, 'endDate' : end_date }],
                "metrics": [
                    # Downloads
                    { 'expression': 'ga:metric' + str(self.owner.extra_config["analytics_metric_name_map"].get("downloads", 2)), 'formattingType' : 'INTEGER' },
                    # Filesize
                    { 'expression': 'ga:metric' + str(self.owner.extra_config["analytics_metric_name_map"].get("filesize", 1)), 'formattingType' : 'INTEGER' },
                    # Range queries (we can't change calculated metric name in analytics after created so.. ya)
                    { 'expression': 'ga:calcMetric_PercentRangeQueries', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productDetailViews', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListClicks', 'formattingType' : 'INTEGER' },
                    { 'expression': 'ga:productListViews', 'formattingType' : 'INTEGER' },
                ],
                "dimensionFilterClauses" : [
                    {
                        "filters" : [
                            {
                                "dimensionName" : "ga:productCategoryLevel1",
                                "expressions" : ["File"],
                                "operator" : "EXACT",
                                "caseSensitive" : True
                            }
                        ]
                    }
                ],
                "orderBys" : [
                    { 'fieldName': 'ga:metric' + str(self.owner.extra_config["analytics_metric_name_map"].get("downloads", 2)), 'sortOrder' : 'descending' },
                    { 'fieldName': 'ga:calcMetric_PercentRangeQueries', 'sortOrder' : 'descending' },
                    { 'fieldName': 'ga:productDetailViews', 'sortOrder' : 'descending' },
                    { 'fieldName': 'ga:productListClicks', 'sortOrder' : 'descending' },
                    { 'fieldName': 'ga:productListViews', 'sortOrder' : 'descending' }
                ]
            }

        @report
        def file_downloads_by_country(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = self.file_download_base_request_json(start_date, end_date)
            report_request_json["dimensions"] = [ { 'name': 'ga:country' } ]
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json

        @report
        def file_downloads_by_filetype(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = self.file_download_base_request_json(start_date, end_date)
            report_request_json["dimensions"] = [ { 'name': 'ga:productVariant' } ] # === 'filetype'
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json

        @report
        def file_downloads_by_experiment_type(self, start_date='yesterday', end_date='yesterday', execute=True):
            report_request_json = self.file_download_base_request_json(start_date, end_date)
            report_request_json["dimensions"] = [
                { "name" : 'ga:dimension' + str(self.owner.extra_config["analytics_dimension_name_map"].get("experimentType", 5)) }
            ]
            if execute:
                return self.query_reports([report_request_json])
            return report_request_json

        @report
        def top_files_downloaded(self, start_date='yesterday', end_date='yesterday', execute=True):
            '''Only gets top 100 results'''
            report_request_json = self.file_download_base_request_json(start_date, end_date)
            report_request_json["dimensions"] = [
                { 'name': 'ga:productName' },
                { 'name': 'ga:productSku' },
                { 'name': 'ga:productCategoryLevel2' },
                { 'name': 'ga:productBrand' }
            ]
            report_request_json["pageSize"] = 100
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




######### CODE TO TEST THE ABOVE #########

'''
The following is code to test the above class(es).
Run this file in interactive mode and continue on:

> python3 -i google_utils.py

'''


if __name__ == "__main__":
    import sys
    import os
    import argparse
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    END_COLOR = "\033[0m"

    parser = argparse.ArgumentParser(description='Supply a fourfront key and key secret from your profile page.')
    parser.add_argument('key', metavar='access_key', type=str, help='Access Key, as obtained from 4DN user profile page.')
    parser.add_argument('secret', metavar='access_key_secret', type=str, help='Access Key Secret, as obtained from 4DN user profile page.')
    parser.add_argument('--server', type=str, default="http://localhost:8000", help='Server to connect to, including protocol and port.')
    parser.add_argument('--command', type=str, help='Command to be executed (for use if not running interactively)')

    args = parser.parse_args()

    if not sys.flags.interactive:
        sys.exit(RED + "Exiting, not in interactive mode.\nRun interactively via `python3 -i google_utils.py` or supply a command.")

    ak = {
        "server": args.server,
        "key": args.key,
        "secret": args.secret
    }

    google = GoogleAPISyncer(ak)
    print(YELLOW + "Checking last tracking item date on " + args.server + "...\n")
    last_tracking_item_date_daily = google.analytics.get_latest_tracking_item_date()
    last_tracking_item_date_monthly = google.analytics.get_latest_tracking_item_date("monthly")
    if not last_tracking_item_date_daily or not last_tracking_item_date_monthly:
        commands = """
    >>> google.analytics.fill_with_tracking_items('daily')
    >>> google.analytics.fill_with_tracking_items('monthly')
        """
        missing_items = (
            "daily nor monthly"
            if (not last_tracking_item_date_daily and not last_tracking_item_date_monthly)
            else "daily" if not last_tracking_item_date_daily
            else "monthly"
        )
        print(
            YELLOW + "No \033[4m" + missing_items + "\033[24m tracking items currently exist on this server.",
            "\nRun the following to fill:", "\033[2m", commands, "\033[22m" + END_COLOR)
    else:
        print(
            GREEN + "Most recent tracking items are from",
            last_tracking_item_date_daily, '(daily)',
            last_tracking_item_date_monthly, '(monthly)',
            END_COLOR
        )

    nextmsg = '''
\033[1mExamples of how to test.\033[0m
No unit tests are setup since data in Google Analytics will vary day-by-day.
Instead, \033[1mmanually compare results of output vs data shown in analytics UI\033[0m to assert.
    >>> google.analytics.file_downloads_by_country()
    <<< \x1b[3m{ ..., "reports" : { "file_downloads_by_country" : [ ..., {'ga:country': 'China', 'ga:metric2': 0, 'ga:metric1': 11877317, 'ga:calcMetric_PercentRangeQueries': 67, 'ga:productDetailViews': 1, 'ga:productListClicks': 0, 'ga:productListViews': 69}, ... ] }, ... }\x1b[23m
    >>> google.analytics.file_downloads_by_filetype()
    <<< \x1b[3m{ ... JSON object with report info ... }\x1b[23m

\033[1mCheck to ensure data aligns with analytics\033[0m, e.g. -
    >>> res_search1 = google.analytics.search_search_queries()
    >>> res_search1["reports"]["search_search_queries"]
    >>> res_facets1 = google.analytics.fields_faceted()
    >>> res_facets1["reports"]["fields_faceted"]

\033[1mUltimately, the following should succeed on localhost\033[0m (if not filled up earlier) -
    >>> google.analytics.fill_with_tracking_items("daily")
    <<< Created 1 TrackingItems so far.
    <<< Created 2 TrackingItems so far.
    <<< ...
    <<< Created 60 TrackingItems so far.
    '''

    print(nextmsg)



