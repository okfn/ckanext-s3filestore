import os
import cgi
import pylons
import logging

import boto

import ckan.model as model
import ckan.lib.munge as munge
import ckan.plugins.toolkit as toolkit


config = pylons.config
log = logging.getLogger(__name__)

_storage_path = None
_max_resource_size = None
_max_image_size = None


def get_storage_path():
    '''Function to cache storage path'''
    global _storage_path

    if _storage_path is None:
        storage_path = config.get('ckanext.s3filestore.aws_storage_path')
        if storage_path:
            _storage_path = storage_path
    return _storage_path


class S3Uploader(object):
    def __init__(self, resource):
        '''resource dict must contain ``url`` or ``clear_upload``.'''

        path = get_storage_path()
        if not path:
            path = ''
        self.storage_path = os.path.join(path, 'resources')

        self.bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
        self.bucket = self.get_s3_bucket(self.bucket_name)

        self.filename = None

        upload_field_storage = resource.pop('upload', None)
        self.clear = resource.pop('clear_upload', None)

        if isinstance(upload_field_storage, cgi.FieldStorage):
            self.filename = upload_field_storage.filename
            self.filename = munge.munge_filename(self.filename)
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
            self.upload_file = upload_field_storage.file
        elif self.clear:
            old_resource = model.Session.query(model.Resource) \
                .get(resource['id'])
            self.key = old_resource.url
            resource['url_type'] = ''

    def get_directory(self, id):
        directory = os.path.join(self.storage_path, id)
        return directory

    def get_path(self, id):
        '''Return the key used for this resource in S3.

        Keys are in the form:
        <site_id>-<app_instance_uuid>/resources/<resource id>/<filename with extension>

        e.g.:
        my_ckan_site-d4cc1a3f-3e02-4fe9-ac74-10ff2e952f32/resources/165900ba-3c60-43c5-9e9c-9f8acd0aa93f/data.csv
        '''
        resource = model.Resource.get(id)
        directory = self.get_directory(id)
        filepath = os.path.join(directory, resource.url)
        return filepath

    def get_s3_bucket(self, bucket_name):
        '''Return a boto bucket, creating it if it doesn't exist.'''
        p_key = config.get('ckanext.s3filestore.aws_access_key_id')
        s_key = config.get('ckanext.s3filestore.aws_secret_access_key')

        # make s3 connection
        S3_conn = boto.connect_s3(p_key, s_key)

        # make sure bucket exists
        bucket = S3_conn.lookup(bucket_name)
        if bucket is None:
            try:
                bucket = S3_conn.create_bucket(bucket_name)
            except boto.exception.S3CreateError as e:
                raise e
        return bucket

    def upload(self, id, max_size=10):
        '''Upload the file to S3, and map the resource id to the S3 url.'''

        # If a filename has been provided (a file is being uploaded) write the
        # file to the appropriate key in the AWS bucket.
        if self.filename:
            filepath = self.get_path(id)
            self.upload_file.seek(0)
            k = boto.s3.key.Key(self.bucket)
            try:
                k.key = filepath
                k.set_contents_from_file(self.upload_file)
            except Exception as e:
                raise e
            finally:
                k.close()
            return

        # The resource form only sets self.clear (via the input clear_upload)
        # to True when an uploaded file is not replaced by another uploaded
        # file, only if it is replaced by a link. If the uploaded file is
        # replaced by a link, we should remove the previously uploaded file to
        # clean up the file system.
        if self.clear:
            filepath = self.get_path(id, self.key)
            k = boto.s3.key.Key(self.bucket)
            try:
                k.key = filepath
                k.delete()
            except Exception as e:
                raise e
            finally:
                k.close()
