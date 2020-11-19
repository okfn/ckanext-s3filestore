# encoding: utf-8
import pytest

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from ckantoolkit import config
import ckan.logic as logic
import ckan.tests.factories as factories

from ckanext.s3filestore.uploader import S3ResourceUploader


class TestS3ResourceUpload(object):

    @classmethod
    def setup_class(cls):
        cls.content = u"""
                    Snow Course Name, Number, Elev. metres, Date of Survey, Snow Depth cm,\
                    Water Equiv. mm, Survey Code, % of Normal, Density %, Survey Period, \
                    Normal mm
                    SKINS LAKE,1B05,890,2015/12/30,34,53,,98,16,JAN-01,54
                    MCGILLIVRAY PASS,1C05,1725,2015/12/31,88,239,,87,27,JAN-01,274
                    NAZKO,1C08,1070,2016/01/05,20,31,,76,16,JAN-01,41
                    """

        p_key = config.get(u'ckanext.s3filestore.aws_access_key_id')
        s_key = config.get(u'ckanext.s3filestore.aws_secret_access_key')
        region = config.get(u'ckanext.s3filestore.region_name')
        host_name = config.get(u'ckanext.s3filestore.host_name')
        signature = config.get(u'ckanext.s3filestore.signature_version')
        addressing_style = config.get(u'ckanext.s3filestore.addressing_style', u'auto')
        cls.bucket_name = config.get(u'ckanext.s3filestore.aws_bucket_name')

        cls.s3 = boto3.session.Session(aws_access_key_id=p_key,
                                       aws_secret_access_key=s_key,
                                       region_name=region)

        cls.resource_obj = cls.s3.resource(u's3',
                                           endpoint_url=host_name,
                                           config=Config(signature_version=signature,
                                                         s3={u'addressing_style': addressing_style})
                                           )

        cls.client = cls.s3.client(service_name=u's3',
                                   endpoint_url=host_name,
                                   config=Config(signature_version=signature,
                                                 s3={u'addressing_style': addressing_style}),
                                   region_name=region)

    def test_resource_upload(self, ckan_config, create_with_upload):
        u'''Test a basic resource file upload'''

        resource = create_with_upload(
            self.content, u'test.csv', url=u'http://data',
            package_id=factories.Dataset()[u"id"]
        )
        key = u'resources/{0}/test.csv' \
            .format(resource[u'id'])

        assert self.client.head_object(Bucket=self.bucket_name, Key=key)

    def test_resource_upload_then_clear(self, ckan_config, create_with_upload):
        u'''Test that clearing an upload removes the S3 key'''

        resource = create_with_upload(
            self.content, 'test-1.csv', url=u'http://data1',
            package_id=factories.Dataset()[u"id"]
        )
        key = u'resources/{0}/test-1.csv' \
            .format(resource[u'id'])

        # key must exist
        assert self.client.head_object(Bucket=self.bucket_name, Key=key)

        context = {u'user': factories.Sysadmin()[u'name']}
        logic.get_action(u'resource_update')(context,
                                             {u'clear_upload': True,
                                              u'id': resource[u'id']})

        # key shouldn't exist, this raises ClientError
        with pytest.raises(ClientError) as e:
            self.client.head_object(Bucket=self.bucket_name, Key=key)

        assert e.value.response[u'Error'][u'Code'] == u'404'

    def test_uploader_get_path(self):
        u'''Uploader get_path returns as expected'''
        dataset = factories.Dataset()
        resource = factories.Resource(package_id=dataset['id'])

        uploader = S3ResourceUploader(resource)
        returned_path = uploader.get_path(resource[u'id'], u'myfile.txt')
        assert returned_path == u'resources/{0}/myfile.txt'.format(resource[u'id'])

