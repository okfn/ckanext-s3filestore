# encoding: utf-8
import six
import pytest

import ckan.tests.factories as factories
from ckan.lib.helpers import url_for


@pytest.mark.usefixtures(u'clean_db', u'clean_index')
class TestS3Controller(object):

    def test_resource_download_url(self, resource):
        '''The resource url is expected for uploaded resource file.'''

        expected_url = u'http://test.ckan.net/dataset/{0}/resource/{1}/download/test.csv'\
            .format(resource[u'package_id'], resource[u'id'])

        assert resource['url'] == expected_url

    def test_resource_download(self, app, resource):
        '''A resource uploaded to S3 can be downloaded.'''

        user = factories.Sysadmin()
        env = {"REMOTE_USER": six.ensure_str(user["name"])}

        response = app.get(
            url_for(
                u'dataset_resource.download',
                id=resource[u'package_id'],
                resource_id=resource[u'id'],
            ),
            extra_environ=env,
            follow_redirects=False
        )
        assert 302 == response.status_code

    def test_resource_download_no_filename(self, app, resource):
        '''A resource uploaded to S3 can be downloaded when no filename in
        url.'''
        resource_file_url = '/dataset/{0}/resource/{1}/download' \
            .format(resource['package_id'], resource['id'])

        response = app.get(resource_file_url,
                           follow_redirects=False)

        assert 302 == response.status_code
#
#     @mock_s3
#     def test_resource_download_url_link(self):
#         '''A resource with a url (not file) is redirected correctly.'''
#         factories.Sysadmin(apikey="my-test-key")
#
#         app = self._get_test_app()
#         demo = ckanapi.TestAppCKAN(app, apikey='my-test-key')
#         dataset = factories.Dataset()
#
#         resource = demo.action.resource_create(package_id=dataset['id'],
#                                                url='http://example')
#         resource_show = demo.action.resource_show(id=resource['id'])
#         resource_file_url = '/dataset/{0}/resource/{1}/download' \
#             .format(resource['package_id'], resource['id'])
#         assert_equal(resource_show['url'], 'http://example')
#
#         conn = boto.connect_s3()
#         bucket = conn.get_bucket('my-bucket')
#         assert_equal(bucket.get_all_keys(), [])
#
#         # attempt redirect to linked url
#         r = app.get(resource_file_url, status=[302, 301])
#         assert_equal(r.location, 'http://example')
