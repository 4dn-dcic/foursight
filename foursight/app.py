from chalice import Chalice, BadRequestError, NotFoundError
import boto3
import json
from botocore.exceptions import ClientError
from chalicelib.ff_connection import FF_Connection

app = Chalice(app_name='foursight')
app.debug = True

TEST_DICT = {
    'ABC': '123',
    'DEF': '456'
}

OBJECTS = {}

S3 = boto3.client('s3')
BUCKET = 'foursight-dev'


@app.route('/')
def index():
    return {'foursight': 'insight into fourfront'}


@app.route('/ff')
def test_FF_connection():
    connection = FF_Connection('https://data.4dnucleome.org/')
    return {'status': str(connection.isUp)}


@app.route('/test/{testval}')
def test_return(testval):
    try:
        return {'return_val': TEST_DICT[testval]}
    except KeyError:
        raise BadRequestError("Unknown test key '%s', valid choices are: %s" % (
            testval, ', '.join(TEST_DICT.keys())))


@app.route('/s3objects/{key}', methods=['GET', 'PUT'])
def s3objects(key):
    request = app.current_request
    if request.method == 'PUT':
        S3.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(request.json_body))
    elif request.method == 'GET':
        try:
            response = S3.get_object(Bucket=BUCKET, Key=key)
            return json.loads(response['Body'].read())
        except ClientError as e:
            raise NotFoundError(key)


@app.route('/introspect')
def introspect():
    return app.current_request.to_dict()


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
