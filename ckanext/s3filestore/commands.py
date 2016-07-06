import sys
import boto3
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

        session = boto3.Session(aws_access_key_id=public_key, aws_secret_access_key=secret_key)
        s3 = session.resource('s3')
        bucket = s3.Bucket(bucket_name)
	exists = True
	try:
	    s3.meta.client.head_bucket(Bucket='mybucket')
	except botocore.exceptions.ClientError as e:
	    # If a client error is thrown, then check that it was a 404 error.
	    # If it was a 404 error, then the bucket does not exist.
	    error_code = int(e.response['Error']['Code'])
	    if error_code == 404:
		exists = False

        # Check if bucket exists
        if not exists:
            print 'Bucket {0} does not exist, trying to create it...'.format(
                bucket_name)
            try:
                bucket = s3.create_bucket(Bucket='mybucket')
            except botocore.exceptions.ClientError as e:
                print 'An error was found while creating the bucket:'
                print str(e)
                sys.exit(1)
        print 'Configuration OK!'
