from flask import Flask, Response, send_file, request
from functools import wraps
import boto3
import os


app =  Flask(__name__)

s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
stage = os.environ.get('stage')
if not stage:
    stage = ''
else:
    stage = '{}/'.format(stage)

BUCKET_NAME = 'deb-fluendo'

def authenticate(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = request.authorization
        print(auth)
        if auth:
            table = ddb.Table('Customers')
            resp = table.get_item(Key={'username':auth['username'], 'password': auth['password']})
            is_valid = resp.get('Item', False)
            if not is_valid:
                return Response(
                                        'Could not verify your access level for that URL.\n'
                                        'You have to login with proper credentials', 401,
                                        {'WWW-Authenticate': 'Basic realm="Login Required"'})
        else:
                return Response(
                                        'Could not verify your access level for that URL.\n'
                                        'You have to login with proper credentials', 401,
                                        {'WWW-Authenticate': 'Basic realm="Login Required"'})

        return f(*args, **kwargs)
    return wrapper


@app.route('/', methods=['GET'])
def index():
    return get_package(None)

@app.route('/<path:key>/', methods=['GET'])
def path_0(key):
    return get_package(key)

@app.route('/<path:key>/<path:key1>/')
def path_1(key, key1):
    return get_package(key, key1)

@app.route('/<path:key>/<path:key1>/<path:key2>/')
def path_2(key, key1, key2):
    return get_package(key, key1, key2)

@app.route('/<path:key>/<path:key1>/<path:key2>/<path:key3>/')
def path_3(key, key1, key2, key3):
    return get_package(key, key1, key2, key3)

@app.route('/<path:key>/<path:key1>/<path:key2>/<path:key3>/<path:key4>/')
def path_4(key, key1, key2, key3, key4):
    return get_package(key, key1, key2, key3, key4)

@authenticate
def get_package(*args):
    body = ''
    if args[0] == None:
        key = ''
    else:
        pkg = args[-1]
        key = '/'.join(args)
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            #pack = json.loads(obj['Body'].read().decode('utf-8'))
            return Response(obj['Body'].read(),
                            status=200,
                            headers={'Content-Type':obj['ContentType'],
                                     'Content-Disposition': 'attachment;filename={}'.format(pkg),
                                    },
                            )

        except s3.exceptions.NoSuchKey:
            pass

        key += '/'

    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter='/', Prefix=key, StartAfter=key)

    try:
        for obj in result['CommonPrefixes']:
            body += "<tr><td><a href='/{}{}'>{}</a></td></tr>".format(stage, obj.get('Prefix'), obj.get('Prefix').split('/')[-2])
    except:
        pass
    try:
        for obj in result['Contents']:
            is_valid = check_pkg_policy('pjackson', obj.get('Key').split('/')[-1])
            if is_valid:
                body += "<tr><td><a href='/{}{}'>{}</a></td></tr>".format(stage, obj.get('Key'), obj.get('Key').split('/')[-1])
    except:
        pass
    return Response(_render(body),
                    status=200,
                    headers={'Content-Type': 'text/html'})


def _render(body):
    html = '<!DOCTYPE html>\
            <html><head><title>Debia Repo-Customer Oriented</title></head>\
            <body><table>{}</table></body></html>'.format(body)
    return html

def check_pkg_policy(username, pkg):

    if not pkg in ['Packages', 'gpg.key', 'InRelease', 'Release', 'Release.gpg'] :
        table = ddb.Table('PackagePolicies')
        resp = table.get_item(Key={'username':username, 'package': pkg})
        is_valid = resp.get('Item', False)
        if is_valid:
            return True
        else:
            return is_valid
    else:
        return True

if __name__ == '__main__':
    app.run()
