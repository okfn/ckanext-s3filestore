'''
This script copies all resource files from a local FileStore directory
to a remote S3 bucket.

**It will not work for group images**

It requires SQLalchemy and Boto.

Please update the configuration details, all keys are mandatory except
AWS_STORAGE_PATH.

'''

import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import boto3


# Configuration

BASE_PATH = '/var/lib/ckan/default/resources'
SQLALCHEMY_URL = 'postgresql://user:pass@localhost/db'
AWS_ACCESS_KEY_ID = 'AKIxxxxxx'
AWS_SECRET_ACCESS_KEY = '+NGxxxxxx'
AWS_BUCKET_NAME = 'my-bucket'
AWS_STORAGE_PATH = 'some-path'


resource_ids_and_paths = {}

for root, dirs, files in os.walk(BASE_PATH):
    if files:
        resource_id = root.split('/')[-2] + root.split('/')[-1] + files[0]
        resource_ids_and_paths[resource_id] = os.path.join(root, files[0])

print 'Found {0} resource files in the file system'.format(
    len(resource_ids_and_paths.keys()))

engine = create_engine(SQLALCHEMY_URL)
connection = engine.connect()

resource_ids_and_names = {}

try:
    for resource_id, file_path in resource_ids_and_paths.iteritems():
        resource = connection.execute(text('''
            SELECT id, url, url_type
            FROM resource
            WHERE id = :id
        '''), id=resource_id)
        if resource.rowcount:
            _id, url, _type = resource.first()
            if _type == 'upload' and url:
                file_name = url.split('/')[-1] if '/' in url else url
                resource_ids_and_names[_id] = file_name.lower()
finally:
    connection.close()
    engine.dispose()

print '{0} resources matched on the database'.format(
    len(resource_ids_and_names.keys()))

s3_connection = boto3.client('s3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                config=botocore.client.Config(signature_version='s3v4'))
bucket = s3_connection.create_bucket(AWS_BUCKET_NAME)
obj = s3.Object(AWS_BUCKET_NAME)

uploaded_resources = []
for resource_id, file_name in resource_ids_and_names.iteritems():
    key = 'resources/{resource_id}/{file_name}'.format(
        resource_id=resource_id, file_name=file_name)
    if AWS_STORAGE_PATH:
        key = AWS_STORAGE_PATH + '/' + key

    k.set_contents_from_filename(resource_ids_and_paths[resource_id])
    uploaded_resources.append(resource_id)
    print 'Uploaded resource {0} ({1}) to S3'.format(resource_id, file_name)

print 'Done, uploaded {0} resources to S3'.format(len(uploaded_resources))
