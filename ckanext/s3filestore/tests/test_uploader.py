import datetime
import os

import mock
from nose.tools import (assert_equal,
                        assert_true,
                        assert_false)

import ckanapi
from ckantoolkit import config
import boto
from moto import mock_s3
from webtest import Upload

import ckantoolkit as toolkit
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

from ckanext.s3filestore.uploader import (S3Uploader,
                                          S3ResourceUploader)


class Uploader(Upload):

    '''
    Extend webtest's Upload class a bit more so it actually stores file data.
    '''

    def __init__(self, *args, **kwargs):
        self.file = kwargs.pop('file')
        super(Uploader, self).__init__(*args, **kwargs)


class TestS3Uploader(helpers.FunctionalTestBase):

    @mock_s3
    def test_uploader_storage_path(self):
        '''S3Uploader get_storage_path returns as expected'''
        returned_path = S3Uploader.get_storage_path('myfiles')
        assert_equal(returned_path, 'my-path/storage/uploads/myfiles')

    @mock_s3
    def test_group_image_upload(self):
        '''Test a group image file upload'''
        sysadmin = factories.Sysadmin(apikey="my-test-key")

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        file_name = 'somename.png'

        img_uploader = Uploader(file_name, file=open(file_path))

        with mock.patch('ckanext.s3filestore.uploader.datetime') as mock_date:
            mock_date.datetime.utcnow.return_value = \
                datetime.datetime(2001, 1, 29)
            context = {'user': sysadmin['name']}
            helpers.call_action('group_create', context=context,
                                name="my-group",
                                image_upload=img_uploader,
                                image_url=file_name,
                                save='save')

        key = '{0}/storage/uploads/group/2001-01-29-000000{1}' \
            .format(config.get('ckanext.s3filestore.aws_storage_path'), file_name)

        conn = boto.connect_s3()
        bucket = conn.get_bucket('my-bucket')
        # test the key exists
        assert_true(bucket.lookup(key))

        # requesting image redirects to s3
        app = self._get_test_app()
        # attempt redirect to linked url
        image_file_url = '/uploads/group/{0}'.format(file_name)
        r = app.get(image_file_url, status=[302, 301])
        assert_equal(r.location, 'https://my-bucket.s3.amazonaws.com/my-path/storage/uploads/group/{0}'
                                 .format(file_name))

    @mock_s3
    def test_group_image_upload_then_clear(self):
        '''Test that clearing an upload removes the S3 key'''

        sysadmin = factories.Sysadmin(apikey="my-test-key")

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        file_name = "somename.png"

        img_uploader = Uploader(file_name, file=open(file_path))

        with mock.patch('ckanext.s3filestore.uploader.datetime') as mock_date:
            mock_date.datetime.utcnow.return_value = \
                datetime.datetime(2001, 1, 29)
            context = {'user': sysadmin['name']}
            helpers.call_action('group_create', context=context,
                                name="my-group",
                                image_upload=img_uploader,
                                image_url=file_name)

        key = '{0}/storage/uploads/group/2001-01-29-000000{1}' \
            .format(config.get('ckanext.s3filestore.aws_storage_path'), file_name)

        conn = boto.connect_s3()
        bucket = conn.get_bucket('my-bucket')
        # test the key exists
        assert_true(bucket.lookup(key))

        # clear upload
        helpers.call_action('group_update', context=context,
                            id='my-group', name='my-group',
                            image_url="http://asdf", clear_upload=True)

        # key shouldn't exist
        assert_false(bucket.lookup(key))


class TestS3ResourceUploader(helpers.FunctionalTestBase):

    @mock_s3
    def test_resource_upload(self):
        '''Test a basic resource file upload'''
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

    @mock_s3
    def test_uploader_get_path(self):
        '''Uploader get_path returns as expected'''
        dataset = factories.Dataset()
        resource = factories.Resource(package_id=dataset['id'])

        uploader = S3ResourceUploader(resource)
        returned_path = uploader.get_path(resource['id'], 'myfile.txt')
        assert_equal(returned_path,
                     'my-path/resources/{0}/myfile.txt'.format(resource['id']))

    @mock_s3
    def test_resource_upload_with_url_and_clear(self):
        '''Test that clearing an upload and using a URL does not crash'''

        sysadmin = factories.Sysadmin(apikey="my-test-key")

        app = self._get_test_app()
        dataset = factories.Dataset(name="my-dataset")

        url = toolkit.url_for(controller='package', action='new_resource',
                              id=dataset['id'])
        env = {'REMOTE_USER': sysadmin['name'].encode('ascii')}

        app.post(url, {'clear_upload': True,
                       'id': '', # Emtpy id from the form
                       'url': 'http://asdf', 'save': 'save'},
                 extra_environ=env)
