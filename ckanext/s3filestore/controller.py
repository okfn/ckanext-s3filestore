import os
import mimetypes
import paste.fileapp
import pylons.config as config

import ckan.plugins.toolkit as toolkit
import ckan.logic as logic
import ckan.lib.base as base
import ckan.model as model
import ckan.lib.uploader as uploader
from ckan.common import _, request, c, response

from ckanext.s3filestore.uploader import S3Uploader

import logging
log = logging.getLogger(__name__)

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = toolkit.redirect_to


class S3Controller(base.BaseController):

    def resource_download(self, id, resource_id, filename=None):
        '''
        Provide a download by either redirecting the user to the url stored or
        downloading the uploaded file from S3.
        '''
        context = {'model': model, 'session': model.Session,
                   'user': c.user or c.author, 'auth_user_obj': c.userobj}

        try:
            rsc = get_action('resource_show')(context, {'id': resource_id})
            get_action('package_show')(context, {'id': id})
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % id)

        if rsc.get('url_type') == 'upload':
            upload = uploader.get_resource_uploader(rsc)
            bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
            bucket = upload.get_s3_bucket(bucket_name)

            if filename is None:
                filename = os.path.basename(rsc['url'])
            key_path = upload.get_path(rsc['id'], filename)

            try:
                key = bucket.get_key(key_path)
            except Exception as e:
                raise e

            if key is None:
                log.warn('Key \'{0}\' not found in bucket \'{1}\''
                         .format(key_path, bucket_name))

                # attempt fallback
                if config.get('ckanext.s3filestore.filesystem_download_fallback',
                              False):
                    log.info('Attempting filesystem fallback for resource {0}'
                             .format(resource_id))
                    url = toolkit.url_for(controller='ckanext.s3filestore.controller:S3Controller',
                                          action='filesystem_resource_download',
                                          id=id,
                                          resource_id=resource_id,
                                          filename=filename)
                    redirect(url)

                abort(404, _('Resource data not found'))
            contents = key.get_contents_as_string()
            key.close()

            dataapp = paste.fileapp.DataApp(contents)

            try:
                status, headers, app_iter = request.call_application(dataapp)
            except OSError:
                abort(404, _('Resource data not found'))

            response.headers.update(dict(headers))
            response.status = status
            content_type, x = mimetypes.guess_type(rsc.get('url', ''))
            if content_type:
                response.headers['Content-Type'] = content_type
            return app_iter

        elif 'url' not in rsc:
            abort(404, _('No download is available'))
        redirect(rsc['url'])

    def filesystem_resource_download(self, id, resource_id, filename=None):
        """
        A fallback controller action to download resources from the
        filesystem. A copy of the action from
        `ckan.controllers.package:PackageController.resource_download`.

        Provide a direct download by either redirecting the user to the url
        stored or downloading an uploaded file directly.
        """
        context = {'model': model, 'session': model.Session,
                   'user': c.user or c.author, 'auth_user_obj': c.userobj}

        try:
            rsc = get_action('resource_show')(context, {'id': resource_id})
            get_action('package_show')(context, {'id': id})
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % id)

        if rsc.get('url_type') == 'upload':
            upload = uploader.ResourceUpload(rsc)
            filepath = upload.get_path(rsc['id'])
            fileapp = paste.fileapp.FileApp(filepath)
            try:
                status, headers, app_iter = request.call_application(fileapp)
            except OSError:
                abort(404, _('Resource data not found'))
            response.headers.update(dict(headers))
            content_type, content_enc = mimetypes.guess_type(rsc.get('url',
                                                                     ''))
            if content_type:
                response.headers['Content-Type'] = content_type
            response.status = status
            return app_iter
        elif 'url' not in rsc:
            abort(404, _('No download is available'))
        redirect(rsc['url'])

    def uploaded_file_redirect(self, upload_to, filename):
        '''Redirect static file requests to their location on S3.'''
        storage_path = S3Uploader.get_storage_path(upload_to)
        filepath = os.path.join(storage_path, filename)
        redirect_url = 'https://{bucket_name}.s3.amazonaws.com/{filepath}' \
            .format(bucket_name=config.get('ckanext.s3filestore.aws_bucket_name'),
                    filepath=filepath)
        redirect(redirect_url)
