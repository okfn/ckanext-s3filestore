# encoding: utf-8
import pytest

import boto3
from botocore.client import Config

import ckan.tests.factories as factories


@pytest.fixture
def s3_session(ckan_config):
    p_key = ckan_config.get(u'ckanext.s3filestore.aws_access_key_id')
    s_key = ckan_config.get(u'ckanext.s3filestore.aws_secret_access_key')
    region = ckan_config.get(u'ckanext.s3filestore.region_name')
    return boto3.session.Session(aws_access_key_id=p_key,
                                 aws_secret_access_key=s_key,
                                 region_name=region)


@pytest.fixture
def s3_resource(ckan_config, s3_session):
    host_name = ckan_config.get(u'ckanext.s3filestore.host_name')
    signature = ckan_config.get(u'ckanext.s3filestore.signature_version')
    addressing_style = ckan_config.get(u'ckanext.s3filestore.addressing_style', u'auto')

    return s3_session.resource(u's3',
                               endpoint_url=host_name,
                               config=Config(signature_version=signature,
                                             s3={u'addressing_style': addressing_style}))


@pytest.fixture
def s3_client(ckan_config, s3_session):
    host_name = ckan_config.get(u'ckanext.s3filestore.host_name')
    signature = ckan_config.get(u'ckanext.s3filestore.signature_version')
    addressing_style = ckan_config.get(u'ckanext.s3filestore.addressing_style', u'auto')
    region = ckan_config.get(u'ckanext.s3filestore.region_name')
    return s3_session.client(service_name=u's3',
                             endpoint_url=host_name,
                             config=Config(signature_version=signature,
                                           s3={u'addressing_style': addressing_style}),
                             region_name=region)


@pytest.fixture
def resource_with_upload(create_with_upload):
    content = u"""
            Snow Course Name, Number, Elev. metres, Date of Survey, Snow Depth cm,\
            Water Equiv. mm, Survey Code, % of Normal, Density %, Survey Period, \
            Normal mm
            SKINS LAKE,1B05,890,2015/12/30,34,53,,98,16,JAN-01,54
            MCGILLIVRAY PASS,1C05,1725,2015/12/31,88,239,,87,27,JAN-01,274
            NAZKO,1C08,1070,2016/01/05,20,31,,76,16,JAN-01,41
            """
    resource = create_with_upload(
        content, u'test.csv',
        package_id=factories.Dataset()[u"id"]
    )
    return resource


@pytest.fixture
def organization_with_image(create_with_upload):
    user = factories.Sysadmin()
    context = {
        u"user": user["name"]
    }
    org = create_with_upload(
        b"\0\0\0", u"image.png",
        context=context,
        action=u"organization_create",
        upload_field_name=u"image_upload",
        name=u"test-org"
    )
    return org
