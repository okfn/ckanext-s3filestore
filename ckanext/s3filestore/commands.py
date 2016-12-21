import sys
import boto3, botocore
from pylons import config
from ckan.plugins import toolkit


class TestConnection(toolkit.CkanCommand):
    '''CKAN S3 FileStore utilities

    Usage:

        paster s3 check-config

            Checks if the configuration entered in the ini file is correct

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 1

    def command(self):
        self._load_config()
        if not self.args:
            print self.usage
        elif self.args[0] == 'check-config':
            self.check_config()

    def check_config(self):
        exit = False
        for key in ('ckanext.s3filestore.aws_access_key_id',
                    'ckanext.s3filestore.aws_secret_access_key',
                    'ckanext.s3filestore.aws_bucket_name'):
            if not config.get(key):
                print 'You must set the "{0}" option in your ini file'.format(
                    key)
                exit = True
        if exit:
            sys.exit(1)

        print 'All configuration options defined'
        bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
        public_key = config.get('ckanext.s3filestore.aws_access_key_id')
        secret_key = config.get('ckanext.s3filestore.aws_secret_access_key')
        region = config.get('ckanext.s3filestore.region_name')

        S3_conn = boto3.resource('s3')

        # Check if bucket exists
        bucket = S3_conn.Bucket(bucket_name)
        if bucket is None:
            print 'Bucket {0} does not exist, trying to create it...'.format(
                bucket_name)
            try:
                bucket = S3_conn.create_bucket(bucket_name, CreateBucketConfiguration={
                    'LocationConstraint': region})
            except botocore.exception.ClientError as e:
                print 'An error was found while creating the bucket:'
                print int(e.response['Error']['Code'])
                sys.exit(1)
        print 'Configuration OK!'
