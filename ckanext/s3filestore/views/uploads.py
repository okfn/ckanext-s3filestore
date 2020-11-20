# encoding: utf-8
import os
import logging

import flask

from ckantoolkit import config
import ckantoolkit as toolkit

from ckanext.s3filestore.uploader import S3Uploader


Blueprint = flask.Blueprint
redirect = toolkit.redirect_to
log = logging.getLogger(__name__)

s3_uploads = Blueprint(
    u's3_uploads',
    __name__
)


def uploaded_file_redirect(upload_to, filename):
    '''Redirect static file requests to their location on S3.'''
    host_name = config.get('ckanext.s3filestore.host_name')
    # Remove last characted if it's a slash
    if host_name[-1] == '/':
        host_name = host_name[:-1]
    storage_path = S3Uploader.get_storage_path(upload_to)
    filepath = os.path.join(storage_path, filename)

    redirect_url = '{host_name}/{bucket_name}/{filepath}' \
        .format(bucket_name=config.get('ckanext.s3filestore.aws_bucket_name'),
                filepath=filepath,
                host_name=host_name)
    return redirect(redirect_url)


s3_uploads.add_url_rule(u'/uploads/<upload_to>/<filename>', view_func=uploaded_file_redirect)


def get_blueprints():
    return [s3_uploads]
