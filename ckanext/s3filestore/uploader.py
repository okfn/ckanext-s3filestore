import os
import cgi
import pylons
import logging

import ckan.lib.munge as munge
import ckan.plugins.toolkit as toolkit

import boto


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

        p_key = config.get('ckanext.s3filestore.aws_access_key_id')
        s_key = config.get('ckanext.s3filestore.aws_secret_access_key')
        bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')

        # make s3 connection
        S3_conn = boto.connect_s3(p_key, s_key)

        # make sure bucket exists
        self.bucket = S3_conn.lookup(bucket_name)
        if self.bucket is None:
            try:
                self.bucket = S3_conn.create_bucket(bucket_name)
            except boto.exception.S3CreateError as e:
                raise e

        # check if the parent dataset is private
        pkg_dict = toolkit.get_action('package_show')(data_dict={'id':
                                                      resource['package_id']})
        self.is_private = pkg_dict['private']

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
            resource['url_type'] = ''

    def get_directory(self, id):
        directory = os.path.join(self.storage_path, id[0:3], id[3:6])
        return directory

    def get_path(self, id):
        directory = self.get_directory(id)
        filepath = os.path.join(directory, id[6:])
        return filepath

    def upload(self, id, max_size=10):
        '''Upload the file to S3.'''

        filepath = self.get_path(id)

        # If a filename has been provided (a file is being uploaded) write the
        # file to the appropriate key in the AWS bucket.
        if self.filename:
            self.upload_file.seek(0)
            k = boto.s3.key.Key(self.bucket)
            try:
                k.key = filepath
                k.set_contents_from_file(self.upload_file)
                if not self.is_private:
                    k.make_public()
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
            k = boto.s3.key.Key(self.bucket)
            try:
                k.key = filepath
                k.delete()
            except Exception as e:
                raise e
            finally:
                k.close()
