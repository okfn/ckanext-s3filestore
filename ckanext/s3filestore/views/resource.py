# encoding: utf-8
import os
import logging

import flask

from botocore.exceptions import ClientError
from botocore.client import Config

from ckantoolkit import config as ckan_config
from ckantoolkit import _, request, c, g
import ckantoolkit as toolkit
import ckan.logic as logic
import ckan.lib.base as base
import ckan.lib.uploader as uploader
import ckan.model as model

log = logging.getLogger(__name__)

Blueprint = flask.Blueprint
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = toolkit.redirect_to


s3_resource = Blueprint(
    u's3_resource',
    __name__,
    url_prefix=u'/dataset/<id>/resource',
    url_defaults={u'package_type': u'dataset'}
)


def resource_download(package_type, id, resource_id, filename=None):
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
        return abort(404, _('Resource not found'))
    except NotAuthorized:
        return abort(401, _('Unauthorized to read resource %s') % id)

    if rsc.get('url_type') == 'upload':

        upload = uploader.get_resource_uploader(rsc)
        bucket_name = ckan_config.get('ckanext.s3filestore.aws_bucket_name')
        region = ckan_config.get('ckanext.s3filestore.region_name')
        host_name = ckan_config.get('ckanext.s3filestore.host_name')
        signature = ckan_config.get('ckanext.s3filestore.signature_version')
        addressing_style = ckan_config.get('ckanext.s3filestore.addressing_style', 'auto')
        bucket = upload.get_s3_bucket(bucket_name)

        if filename is None:
            filename = os.path.basename(rsc['url'])
        key_path = upload.get_path(rsc['id'], filename)
        key = filename

        if key is None:
            log.warn('Key \'{0}\' not found in bucket \'{1}\''
                     .format(key_path, bucket_name))

        try:
            # Small workaround to manage downloading of large files
            # We are using redirect to minio's resource public URL
            s3 = upload.get_s3_session()
            client = s3.client(service_name='s3', endpoint_url=host_name,
                               config=Config(signature_version=signature,
                                             s3={'addressing_style': addressing_style}),
                               region_name=region)
            url = client.generate_presigned_url(ClientMethod='get_object',
                                                Params={'Bucket': bucket.name,
                                                        'Key': key_path},
                                                ExpiresIn=60)
            return redirect(url)

        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                # attempt fallback
                if ckan_config.get(
                        'ckanext.s3filestore.filesystem_download_fallback',
                        False):
                    log.info('Attempting filesystem fallback for resource {0}'
                             .format(resource_id))
                    url = toolkit.url_for(
                        u's3_resource.filesystem_resource_download',
                        id=id,
                        resource_id=resource_id,
                        filename=filename)
                    return redirect(url)

                return abort(404, _('Resource data not found'))
            else:
                raise ex


def filesystem_resource_download(package_type, id, resource_id, filename=None):
    """
    A fallback view action to download resources from the
    filesystem. A copy of the action from
    `ckan.views.resource:download`.

    Provides a direct download by either redirecting the user to the url
    stored or downloading an uploaded file directly.
    """
    context = {
        u'model': model,
        u'session': model.Session,
        u'user': g.user,
        u'auth_user_obj': g.userobj
    }

    try:
        rsc = get_action(u'resource_show')(context, {u'id': resource_id})
        get_action(u'package_show')(context, {u'id': id})
    except (NotFound, NotAuthorized):
        return abort(404, _(u'Resource not found'))

    if rsc.get(u'url_type') == u'upload':
        upload = uploader.get_resource_uploader(rsc)
        filepath = upload.get_path(rsc[u'id'])
        return flask.send_file(filepath)
    elif u'url' not in rsc:
        return abort(404, _(u'No download is available'))
    return redirect(rsc[u'url'])


s3_resource.add_url_rule(u'/<resource_id>/download', view_func=resource_download)
s3_resource.add_url_rule(
        u'/<resource_id>/download/<filename>', view_func=resource_download
    )
s3_resource.add_url_rule(
        u'/<resource_id>/fs_download/<filename>', view_func=filesystem_resource_download
    )


def get_blueprints():
    return [s3_resource]