# encoding: utf-8
import pytest

from botocore.exceptions import ClientError

from ckantoolkit import config
import ckan.logic as logic
import ckan.tests.factories as factories

from ckanext.s3filestore.uploader import S3ResourceUploader


@pytest.mark.usefixtures(u'clean_db', u'clean_index')
class TestS3ResourceUpload(object):

    @classmethod
    def setup_class(cls):
        cls.bucket_name = config.get(u'ckanext.s3filestore.aws_bucket_name')

    def test_resource_upload(self, s3_client, resource, ckan_config):
        u'''Test a basic resource file upload'''

        key = u'resources/{0}/test.csv' \
            .format(resource[u'id'])

        assert s3_client.head_object(Bucket=self.bucket_name, Key=key)

    def test_resource_upload_then_clear(self, s3_client, resource, ckan_config):
        u'''Test that clearing an upload removes the S3 key'''

        key = u'resources/{0}/test.csv' \
            .format(resource[u'id'])

        # key must exist
        assert s3_client.head_object(Bucket=self.bucket_name, Key=key)

        context = {u'user': factories.Sysadmin()[u'name']}
        logic.get_action(u'resource_update')(context,
                                             {u'clear_upload': True,
                                              u'id': resource[u'id']})

        # key shouldn't exist, this raises ClientError
        with pytest.raises(ClientError) as e:
            s3_client.head_object(Bucket=self.bucket_name, Key=key)

        assert e.value.response[u'Error'][u'Code'] == u'404'

    def test_uploader_get_path(self):
        u'''Uploader get_path returns as expected'''
        dataset = factories.Dataset()
        resource = factories.Resource(package_id=dataset['id'],
                                      name=u'myfile.txt')

        uploader = S3ResourceUploader(resource)
        returned_path = uploader.get_path(resource[u'id'], resource[u'name'])
        assert returned_path == u'resources/{0}/{1}'.format(resource[u'id'],
                                                            resource[u'name'])

