import os
import cgi
import pylons
import logging
import datetime
import mimetypes

import boto

import ckan.model as model
import ckan.lib.munge as munge


config = pylons.config
log = logging.getLogger(__name__)

_storage_path = None
_max_resource_size = None
_max_image_size = None


class S3FileStoreException(Exception):
    pass

class BaseS3Uploader(object):

    def __init__(self):
        self.bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
        self.bucket = self.get_s3_bucket(self.bucket_name)

    def get_directory(self, id, storage_path):
        directory = os.path.join(storage_path, id)
        return directory

    def get_s3_bucket(self, bucket_name):
        '''Return a boto bucket, creating it if it doesn't exist.'''
        if not config.get('ckanext.s3filestore.aws_use_ami_role'):
            p_key = config.get('ckanext.s3filestore.aws_access_key_id')
            s_key = config.get('ckanext.s3filestore.aws_secret_access_key')
        else:
            p_key, s_key = (None, None)

        # make s3 connection
        S3_conn = boto.connect_s3(p_key, s_key)

        # make sure bucket exists and that we can access
        try:
            bucket = S3_conn.get_bucket(bucket_name)
        except boto.exception.S3ResponseError as e:
            if e.status == 404:
                log.warning('Bucket {0} could not be found, ' +
                            'attempting to create it...'.format(bucket_name))
                try:
                    bucket = S3_conn.create_bucket(bucket_name)
                except (boto.exception.S3CreateError,
                        boto.exception.S3ResponseError) as e:
                    raise S3FileStoreException(
                        'Could not create bucket {0}: {1}'.format(bucket_name,
                                                                  str(e)))
            elif e.status == 403:
                raise S3FileStoreException(
                    'Access to bucket {0} denied'.format(bucket_name))
            else:
                raise

        return bucket

    def upload_to_key(self, filepath, upload_file, make_public=False):
        '''Uploads the `upload_file` to `filepath` on `self.bucket`.'''
        upload_file.seek(0)
        content_type, x = mimetypes.guess_type(filepath)
        headers = {}
        if content_type:
            headers.update({'Content-Type': content_type})
        k = boto.s3.key.Key(self.bucket)
        try:
            k.key = filepath
            k.set_contents_from_file(upload_file, headers=headers)
            if make_public:
                k.make_public()
        except Exception as e:
            raise e
        finally:
            k.close()

    def clear_key(self, filepath):
        '''Deletes the contents of the key at `filepath` on `self.bucket`.'''
        k = boto.s3.key.Key(self.bucket)
        try:
            k.key = filepath
            k.delete()
        except Exception as e:
            raise e
        finally:
            k.close()


class S3Uploader(BaseS3Uploader):

    '''
    An uploader class to replace local file storage with Amazon Web Services
    S3 for general files (e.g. Group cover images).
    '''

    def __init__(self, upload_to, old_filename=None):
        '''Setup the uploader. Additional setup is performed by
        update_data_dict(), and actual uploading performed by `upload()`.

        Create a storage path in the format:
        <ckanext.s3filestore.aws_storage_path>/storage/uploads/<upload_to>/
        '''

        super(S3Uploader, self).__init__()

        self.storage_path = self.get_storage_path(upload_to)

        self.filename = None
        self.filepath = None

        self.old_filename = old_filename
        if old_filename:
            self.old_filepath = os.path.join(self.storage_path, old_filename)

    @classmethod
    def get_storage_path(cls, upload_to):
        path = config.get('ckanext.s3filestore.aws_storage_path', '')
        return os.path.join(path, 'storage', 'uploads', upload_to)

    def update_data_dict(self, data_dict, url_field, file_field, clear_field):
        '''Manipulate data from the data_dict. This needs to be called before it
        reaches any validators.

        `url_field` is the name of the field where the upload is going to be.

        `file_field` is name of the key where the FieldStorage is kept (i.e
        the field where the file data actually is).

        `clear_field` is the name of a boolean field which requests the upload
        to be deleted.
        '''

        self.url = data_dict.get(url_field, '')
        self.clear = data_dict.pop(clear_field, None)
        self.file_field = file_field
        self.upload_field_storage = data_dict.pop(file_field, None)

        if not self.storage_path:
            return

        if hasattr(self.upload_field_storage, 'filename'):
            self.filename = self.upload_field_storage.filename
            self.filename = str(datetime.datetime.utcnow()) + self.filename
            self.filename = munge.munge_filename_legacy(self.filename)
            self.filepath = os.path.join(self.storage_path, self.filename)
            data_dict[url_field] = self.filename
            self.upload_file = self.upload_field_storage.file
        # keep the file if there has been no change
        elif self.old_filename and not self.old_filename.startswith('http'):
            if not self.clear:
                data_dict[url_field] = self.old_filename
            if self.clear and self.url == self.old_filename:
                data_dict[url_field] = ''

    def upload(self, max_size=2):
        '''Actually upload the file.

        This should happen just before a commit but after the data has been
        validated and flushed to the db. This is so we do not store anything
        unless the request is actually good. max_size is size in MB maximum of
        the file'''

        # If a filename has been provided (a file is being uploaded) write the
        # file to the appropriate key in the AWS bucket.
        if self.filename:
            self.upload_to_key(self.filepath, self.upload_file,
                               make_public=True)
            self.clear = True

        if (self.clear and self.old_filename
                and not self.old_filename.startswith('http')):
            self.clear_key(self.old_filepath)


class S3ResourceUploader(BaseS3Uploader):

    '''
    An uploader class to replace local file storage with Amazon Web Services
    S3 for resource files.
    '''

    def __init__(self, resource):
        '''Setup the resource uploader. Actual uploading performed by
        `upload()`.

        Create a storage path in the format:
        <ckanext.s3filestore.aws_storage_path>/resources/
        '''

        super(S3ResourceUploader, self).__init__()

        path = config.get('ckanext.s3filestore.aws_storage_path', '')
        self.storage_path = os.path.join(path, 'resources')
        self.filename = None
        self.old_filename = None

        upload_field_storage = resource.pop('upload', None)
        self.clear = resource.pop('clear_upload', None)

        if isinstance(upload_field_storage, cgi.FieldStorage):
            self.filename = upload_field_storage.filename
            self.filename = munge.munge_filename(self.filename)
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
            self.upload_file = upload_field_storage.file
        elif self.clear and resource.get('id'):
            # New, not yet created resources can be marked for deletion if the
            # users cancels an upload and enters a URL instead.
            old_resource = model.Session.query(model.Resource) \
                .get(resource['id'])
            self.old_filename = old_resource.url
            resource['url_type'] = ''

    def get_path(self, id, filename):
        '''Return the key used for this resource in S3.

        Keys are in the form:
        <ckanext.s3filestore.aws_storage_path>/resources/<resource id>/<filename>

        e.g.:
        my_storage_path/resources/165900ba-3c60-43c5-9e9c-9f8acd0aa93f/data.csv
        '''
        directory = self.get_directory(id, self.storage_path)
        filepath = os.path.join(directory, filename)
        return filepath

    def upload(self, id, max_size=10):
        '''Upload the file to S3.'''

        # If a filename has been provided (a file is being uploaded) write the
        # file to the appropriate key in the AWS bucket.
        if self.filename:
            filepath = self.get_path(id, self.filename)
            self.upload_to_key(filepath, self.upload_file)

        # The resource form only sets self.clear (via the input clear_upload)
        # to True when an uploaded file is not replaced by another uploaded
        # file, only if it is replaced by a link. If the uploaded file is
        # replaced by a link, we should remove the previously uploaded file to
        # clean up the file system.
        if self.clear and self.old_filename:
            filepath = self.get_path(id, self.old_filename)
            self.clear_key(filepath)
