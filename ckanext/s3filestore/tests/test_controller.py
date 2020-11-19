# import os
# import pytest
# import six
#
# from ckantoolkit import config
#
# import ckan.tests.helpers as helpers
# import ckan.tests.factories as factories
# from ckan.lib.helpers import url_for
#
# import ckanapi
# import boto
# from moto import mock_s3
#
# import logging
# log = logging.getLogger(__name__)
#
#
# @pytest.mark.usefixtures('clean_db', 'with_plugins', 'with_request_context')
# class TestS3ControllerResourceDownload(object):
#
#     @pytest.mark.ckan_config('ckan.site_url', 'http://mytest.ckan.net')
#     def test_resource_show_url(self, app):
#         '''The resource_show url is expected for uploaded resource file.'''
#
#         context = {}
#         file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
#         params = {
#             "package_id": factories.Dataset()["id"],
#             "url": "http://data",
#             "upload": open(file_path),
#             "name": "A nice resource",
#         }
#         result = helpers.call_action("resource_create", context, **params)
#
#         id = result.pop("id")
#
#         # does resource_show have the expected resource file url?
#         resource_show = helpers.call_action("resource_show", id=id)
#
#         expected_url = "http://data"
#
#         assert resource_show['url'] == expected_url
#
#     def test_resource_download_s3(self, app):
#         '''A resource uploaded to S3 can be downloaded.'''
#
#         user = factories.User()
#         dataset = factories.Dataset()
#         env = {"REMOTE_USER": six.ensure_str(user["name"])}
#
#         response = app.post(
#             url_for(
#                 "{}_resource.new".format(dataset["type"]), id=dataset["id"]
#             ),
#             extra_environ=env,
#             data={
#                 "id": "",
#                 "url": "http://test.com/",
#                 "save": "go-dataset-complete"
#             }
#         )
#
#         result = helpers.call_action("package_show", id=dataset["id"])
#
#         response = app.get(
#             url_for(
#                 "{}_resource.download".format(dataset["type"]),
#                 id=dataset["id"],
#                 resource_id=result["resources"][0]["id"],
#             ),
#             extra_environ=env,
#             follow_redirects=False
#         )
#         assert 302 == response.status_code
#
#     @mock_s3
#     def test_resource_download_s3_no_filename(self):
#         '''A resource uploaded to S3 can be downloaded when no filename in
#         url.'''
#
#         resource, demo, app = self._upload_resource()
#
#         resource_file_url = '/dataset/{0}/resource/{1}/download' \
#             .format(resource['package_id'], resource['id'])
#
#         file_response = app.get(resource_file_url)
#
#         assert_equal(file_response.content_type, 'text/csv')
#         assert_true('date,price' in file_response.body)
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
