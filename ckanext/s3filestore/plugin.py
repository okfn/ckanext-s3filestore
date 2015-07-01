import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import ckanext.s3filestore.uploader
import ckanext.s3filestore.model


class S3FileStorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')

    # IConfigurable

    def configure(self, config):
        ckanext.s3filestore.model.model_setup()
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = "{0} is not configured. Please amend your .ini file."
        config_options = (
            'ckanext.s3filestore.aws_access_key_id',
            'ckanext.s3filestore.aws_secret_access_key',
            'ckanext.s3filestore.aws_bucket_name'
        )
        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

    # IUploader

    def get_uploader(self, data_dict):
        '''Return an uploader object used to upload files.'''
        return ckanext.s3filestore.uploader.S3Uploader(data_dict)

    # IRoutes

    def before_map(self, map):
        map.connect('resource_download',
                    '/dataset/{id}/resource/{resource_id}/download',
                    controller='ckanext.s3filestore.controller:S3Controller',
                    action='resource_download')

        map.connect('resource_download',
                    '/dataset/{id}/resource/{resource_id}/download/{filename}',
                    controller='ckanext.s3filestore.controller:S3Controller',
                    action='resource_download')

        return map
