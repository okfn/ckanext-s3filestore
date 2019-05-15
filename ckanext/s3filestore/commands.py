import sys
import boto
from ckantoolkit import config
import ckantoolkit as toolkit


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
            print(self.usage)
        elif self.args[0] == 'check-config':
            self.check_config()

    def check_config(self):
        exit = False
        required_keys = ('ckanext.s3filestore.aws_bucket_name',)
        if not config.get('ckanext.s3filestore.aws_use_ami_role'):
            required_keys += ('ckanext.s3filestore.aws_access_key_id',
                              'ckanext.s3filestore.aws_secret_access_key')
        for key in required_keys:
            if not config.get(key):
                print
                'You must set the "{0}" option in your ini file'.format(
                    key)
                exit = True
        if exit:
            sys.exit(1)

        print('All configuration options defined')
        bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
        public_key = config.get('ckanext.s3filestore.aws_access_key_id')
        secret_key = config.get('ckanext.s3filestore.aws_secret_access_key')

        S3_conn = boto.connect_s3(public_key, secret_key)

        # Check if bucket exists
        bucket = S3_conn.lookup(bucket_name)
        if bucket is None:
            print('Bucket {0} does not exist, trying to create it...'.format(
                bucket_name))
            try:
                bucket = S3_conn.create_bucket(bucket_name)
            except boto.exception.StandardError as e:
                print('An error was found while creating the bucket:')
                print(str(e))
                sys.exit(1)
        print('Configuration OK!')
