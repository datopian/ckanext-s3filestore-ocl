# encoding: utf-8
import ckan.plugins as plugins
import ckantoolkit as toolkit

from ckanext.s3filestore.actions import get_signed_url
import ckanext.s3filestore.uploader
from ckanext.s3filestore.views import resource, uploads
from ckanext.s3filestore.click_commands import upload_resources
from ckan.types import Action, AuthFunction, Context, DataDict, AuthResult

class S3FileStorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)
    plugins.implements(plugins.IResourceController)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        # We need to register the following templates dir in order
        # to fix downloading the HTML file instead of previewing when
        # 'webpage_view' is enabled
        toolkit.add_template_directory(config_, 'theme/templates')

    # IConfigurable

    def configure(self, config):
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = "{0} is not configured. Please amend your .ini file."
        config_options = (
            'ckanext.s3filestore.aws_bucket_name',
            'ckanext.s3filestore.region_name',
            'ckanext.s3filestore.signature_version'
        )

        if not config.get('ckanext.s3filestore.aws_use_ami_role'):
            config_options += ('ckanext.s3filestore.aws_access_key_id',
                               'ckanext.s3filestore.aws_secret_access_key')

        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

        # Check that options actually work, if not exceptions will be raised
        if toolkit.asbool(
                config.get('ckanext.s3filestore.check_access_on_startup',
                           True)):
            ckanext.s3filestore.uploader.BaseS3Uploader().get_s3_bucket(
                config.get('ckanext.s3filestore.aws_bucket_name'))

    # IUploader
    def get_resource_uploader(self, data_dict):
        '''Return an uploader object used to upload resource files.'''
        return ckanext.s3filestore.uploader.S3ResourceUploader(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        '''Return an uploader object used to upload general files.'''
        return ckanext.s3filestore.uploader.S3Uploader(upload_to,
                                                       old_filename)

    # IBlueprint
    def get_blueprint(self):
        blueprints = resource.get_blueprints() +\
            uploads.get_blueprints()
        return blueprints

    # IClick
    def get_commands(self):
        return [upload_resources]

    # IAuthFunctions
    def get_auth_functions(self) -> dict[str, AuthFunction]:
        def get_signed_url(context: Context, data_dict: DataDict) -> AuthResult:
            return toolkit.check_access("package_create", context, data_dict)
        return {
            "get_signed_url": get_signed_url
        }

    # IActions
    def get_actions(self):
        return {
            'get_signed_url': get_signed_url
        }

    # IResourceController
    def before_resource_create(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_create(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_resource_show(self, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_show(self, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_resource_create(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_create(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_resource_delete(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_delete(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_resource_update(self, context, current, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_update(self, context, current, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_resource_update(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_update(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_delete(self, context, resource, resources):
        # Delete the resource from the storage
        for rs in resources:
            if rs.get('id') == resource.get('id'):
                ckanext.s3filestore.uploader.delete_from_bucket(rs)

    def before_resource_delete(self, context, resource, resources):
        '''Required by IResourceController'''
        pass
