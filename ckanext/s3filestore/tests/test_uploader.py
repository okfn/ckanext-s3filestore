import os

from nose.tools import (assert_equal,
                        assert_true,
                        assert_false)

import ckanapi
import pylons.config as config
import boto
from moto import mock_s3

import ckan.plugins.toolkit as toolkit
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories


class TestS3ResourceUploader(helpers.FunctionalTestBase):

    @mock_s3
    def test_resource_upload(self):
        '''Tests a basic resource file upload'''
        factories.Sysadmin(apikey="my-test-key")

        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey='my-test-key')
        factories.Dataset(name="my-dataset")

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        resource = demo.action.resource_create(package_id='my-dataset',
                                               upload=open(file_path),
                                               url='file.txt')

        key = '{1}/resources/{0}/data.csv' \
            .format(resource['id'],
                    config.get('ckanext.s3filestore.aws_storage_path'))

        conn = boto.connect_s3()
        bucket = conn.get_bucket('my-bucket')
        # test the key exists
        assert_true(bucket.lookup(key))
        # test the file contains what's expected
        assert_equal(bucket.get_key(key).get_contents_as_string(),
                     open(file_path).read())

    @mock_s3
    def test_resource_upload_then_clear(self):
        '''Test that clearing an upload removes the S3 key'''

        sysadmin = factories.Sysadmin(apikey="my-test-key")

        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey='my-test-key')
        dataset = factories.Dataset(name="my-dataset")

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        resource = demo.action.resource_create(package_id='my-dataset',
                                               upload=open(file_path),
                                               url='file.txt')

        key = '{1}/resources/{0}/data.csv' \
            .format(resource['id'],
                    config.get('ckanext.s3filestore.aws_storage_path'))

        conn = boto.connect_s3()
        bucket = conn.get_bucket('my-bucket')
        # test the key exists
        assert_true(bucket.lookup(key))

        # clear upload
        url = toolkit.url_for(controller='package', action='resource_edit',
                              id=dataset['id'], resource_id=resource['id'])
        env = {'REMOTE_USER': sysadmin['name'].encode('ascii')}
        app.post(url, {'clear_upload': True,
                       'url': 'http://asdf', 'save': 'save'},
                 extra_environ=env)

        # key shouldn't exist
        assert_false(bucket.lookup(key))
