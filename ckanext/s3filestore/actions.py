import logging
from ckan.types import Context, DataDict, AuthResult
from ckan.model.types import make_uuid
import ckan.lib.uploader as uploader
import ckan.plugins.toolkit as toolkit
from botocore.exceptions import ClientError
from ckan.common import _
from ckan.logic import ValidationError, NotAuthorized, NotFound

log = logging.getLogger(__name__)

@toolkit.chained_action
def resource_create(up_func, context: Context, data_dict: DataDict):
    res = up_func(context, data_dict)
    res['url'] = res['url'].replace("REPLACE_HERE", res['id'])
    toolkit.get_action('resource_update')(context, res)
    return res

def get_signed_url(context: Context, data_dict: DataDict) -> AuthResult:
    """Generate a signed URL for single file upload"""

    user = context.get('user')
    if not user:
        raise NotAuthorized(_('You must be logged in to upload files'))
    
    package_id = data_dict.get("package_id", None)
    filename = data_dict.get("filename", None)

    if package_id is None:
        raise ValidationError({"package_id": _("Package ID is required")})
    if filename is None:
        raise ValidationError({"filename": _("Filename is required")})

    try:
        toolkit.get_action('package_show')(context, {'id': package_id})
    except NotFound:
        raise NotFound(_('Package not found'))
    except NotAuthorized:
        raise NotAuthorized(_('You are not authorized to access this package'))

    try:
        toolkit.check_access('resource_create', context, {'package_id': package_id})
    except NotAuthorized:
        raise NotAuthorized(_('You are not authorized to create resources for this package'))

    if not _is_valid_filename(filename):
        raise ValidationError({"filename": _("Invalid filename")})

    url_type = 'upload'
    resource_id = make_uuid()

    try:
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": resource_id,
            "url_type": url_type,
        })

        key_path = upload.get_path(resource_id, filename)
        signed_url = upload.generate_put_presigned_url(key_path)
        
        log.info(f"Generated signed URL for user {user} on package {package_id}: {key_path}")

        return {
            "resource_id": resource_id,
            "signed_url": signed_url,
        }
    
    except Exception as e:
        log.error(f"Failed to generate signed URL: {str(e)}")
        raise ValidationError({"upload": _("Failed to generate upload URL")})


def _is_valid_filename(filename: str) -> bool:
    """
    Validate filename to prevent security issues
    """
    import os
    import re
    
    # Basic checks
    if not filename or len(filename) > 255:
        return False
    
    # Check for dangerous characters or patterns
    dangerous_patterns = [
        r'\.\./',  # Path traversal
        r'[<>:"|?*]',  # Invalid characters
        r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)',  # Windows reserved names
    ]
    
    for pattern in dangerous_patterns:
        if re.search(pattern, filename, re.IGNORECASE):
            return False
    
    # Check file extension if needed (optional - depends on your requirements)
    # allowed_extensions = ['.csv', '.json', '.xml', '.txt', '.pdf', '.xlsx']
    # if not any(filename.lower().endswith(ext) for ext in allowed_extensions):
    #     return False
    
    return True


@toolkit.side_effect_free
def create_multipart_upload(context, data_dict):
    """
    Create a multipart upload session using your S3 uploader class.

    :param filename: Name of the file to upload
    :param content_type: MIME type of the file
    :param resource_id: Optional resource ID to associate with upload
    :param package_id: Package ID for the resource

    :returns: Dictionary containing uploadId and key
    """
    # Check authorization
    toolkit.check_access('resource_create', context, data_dict)

    # Validate input
    filename = data_dict.get('name')
    content_type = data_dict.get('content_type', 'application/octet-stream')
    resource_id = data_dict.get('resource_id')
    package_id = data_dict.get('package_id')

    if not filename:
        raise toolkit.ValidationError({'filename': ['Filename is required']})
    if not package_id:
        raise toolkit.ValidationError(
            {'package_id': ['Package ID is required']})

    try:
        # Create uploader instance using your factory function
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": resource_id or make_uuid(),
            "url_type": 'upload',
        })

        # Generate the S3 key path
        key = upload.get_path(resource_id or make_uuid(), filename)

        # Use your class method to create multipart upload
        response = upload.create_multipart_upload(key, content_type)

        return {
            'uploadId': response['UploadId'],
            'key': response['Key'],
            'resourceId': resource_id or response.get('resource_id'),
            'success': True
        }

    except ClientError as e:
        log.error(f"Error creating multipart upload: {e}")
        raise toolkit.ValidationError(
            {'error': [f'Failed to create upload: {str(e)}']})


@toolkit.side_effect_free
def prepare_upload_parts(context, data_dict):
    """
    Generate presigned URLs for uploading parts using your S3 uploader class.

    :param upload_id: Upload ID from create_multipart_upload
    :param key: Object key from create_multipart_upload
    :param parts: List of part numbers to prepare
    :param package_id: Package ID for authorization

    :returns: Dictionary with presigned URLs for each part
    """
    # Check authorization
    toolkit.check_access('resource_create', context, data_dict)

    # Validate input
    upload_id = data_dict.get('uploadId')
    key = data_dict.get('key')
    parts = data_dict.get('parts', [])
    package_id = data_dict.get('package_id')

    if not upload_id or not key:
        raise toolkit.ValidationError({
            'upload_id': ['Upload ID is required'],
            'key': ['Key is required']
        })

    try:
        # Create uploader instance
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": data_dict.get('resource_id'),
            "url_type": 'upload',
        })

        presigned_urls = {}

        for part_info in parts:
            part_number = part_info.get('number')
            if not part_number:
                continue

            # Use your class method to generate presigned URL
            presigned_url = upload.generate_multipart_presigned_url(
                key=key,
                upload_id=upload_id,
                part_number=part_number,
                expires_in=3600  # 1 hour
            )

            presigned_urls[part_number] = presigned_url

        return {
            'presignedUrls': presigned_urls,
            'success': True
        }

    except ClientError as e:
        log.error(f"Error preparing upload parts: {e}")
        raise toolkit.ValidationError(
            {'error': [f'Failed to prepare parts: {str(e)}']})


@toolkit.side_effect_free
def list_parts(context, data_dict):
    """
    List uploaded parts for a multipart upload using your S3 uploader class.

    :param upload_id: Upload ID
    :param key: Object key
    :param package_id: Package ID for authorization

    :returns: List of uploaded parts with metadata
    """
    # Check authorization
    toolkit.check_access('resource_show', context, data_dict)

    upload_id = data_dict.get('uploadId')
    key = data_dict.get('key')
    package_id = data_dict.get('package_id')

    if not upload_id or not key:
        raise toolkit.ValidationError({
            'upload_id': ['Upload ID is required'],
            'key': ['Key is required']
        })

    try:
        # Create uploader instance
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": data_dict.get('resource_id'),
            "url_type": 'upload',
        })

        # Use your class method to list parts
        response = upload.list_multipart_parts(key, upload_id)

        return {
            'parts': response.get('Parts', []),
            'success': True
        }

    except ClientError as e:
        log.error(f"Error listing parts: {e}")
        raise toolkit.ValidationError(
            {'error': [f'Failed to list parts: {str(e)}']})


def complete_multipart_upload(context, data_dict):
    """
    Complete a multipart upload and create the final object using your S3 uploader class.

    :param upload_id: Upload ID
    :param key: Object key
    :param parts: List of parts with PartNumber and ETag
    :param resource_id: Optional resource ID to update
    :param package_id: Package ID for authorization

    :returns: Upload completion information
    """
    # Check authorization
    toolkit.check_access('resource_update', context, data_dict)

    upload_id = data_dict.get('uploadId')
    key = data_dict.get('key')
    parts = data_dict.get('parts', [])
    resource_id = data_dict.get('resource_id')
    package_id = data_dict.get('package_id')

    if not upload_id or not key or not parts:
        raise toolkit.ValidationError({
            'upload_id': ['Upload ID is required'],
            'key': ['Key is required'],
            'parts': ['Parts list is required']
        })

    try:
        # Create uploader instance
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": resource_id,
            "url_type": 'upload',
        })

        # Use your class method to complete multipart upload
        response = upload.complete_multipart_upload(key, upload_id, parts)

        result = {
            'location': response.get('Location'),
            'bucket': response.get('Bucket'),
            'key': response.get('Key'),
            'etag': response.get('ETag'),
            'success': True
        }

        # If resource_id provided, update the resource
        if resource_id:
            try:
                # Get current resource
                resource = toolkit.get_action('resource_show')(
                    context, {'id': resource_id}
                )

                # Update resource with new URL
                resource.update({
                    'url': result['location'],
                    'upload_complete': True,
                    'multipart_upload_id': upload_id
                })

                toolkit.get_action('resource_update')(context, resource)
                result['resource_updated'] = True

            except Exception as e:
                log.warning(f"Failed to update resource {resource_id}: {e}")
                result['resource_updated'] = False

        return result

    except ClientError as e:
        log.error(f"Error completing multipart upload: {e}")
        raise toolkit.ValidationError(
            {'error': [f'Failed to complete upload: {str(e)}']})


def abort_multipart_upload(context, data_dict):
    """
    Abort a multipart upload and clean up parts using your S3 uploader class.

    :param upload_id: Upload ID
    :param key: Object key
    :param package_id: Package ID for authorization

    :returns: Abort confirmation
    """
    # Check authorization
    toolkit.check_access('resource_delete', context, data_dict)

    upload_id = data_dict.get('uploadId')
    key = data_dict.get('key')
    package_id = data_dict.get('package_id')

    if not upload_id or not key:
        raise toolkit.ValidationError({
            'upload_id': ['Upload ID is required'],
            'key': ['Key is required']
        })

    try:
        # Create uploader instance
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": data_dict.get('resource_id'),
            "url_type": 'upload',
        })

        # Use your class method to abort multipart upload
        upload.abort_multipart_upload(key, upload_id)

        return {
            'message': 'Multipart upload aborted successfully',
            'success': True
        }

    except ClientError as e:
        log.error(f"Error aborting multipart upload: {e}")
        raise toolkit.ValidationError(
            {'error': [f'Failed to abort upload: {str(e)}']})


@toolkit.side_effect_free
def sign_part(context, data_dict):
    """
    Generate a presigned URL for a single part upload using your S3 uploader class.

    :param upload_id: Upload ID
    :param key: Object key
    :param part_number: Part number to sign
    :param package_id: Package ID for authorization

    :returns: Presigned URL for the part
    """
    # Check authorization
    toolkit.check_access('resource_create', context, data_dict)

    upload_id = data_dict.get('uploadId')
    key = data_dict.get('key')
    part_number = data_dict.get('partNumber')
    package_id = data_dict.get('package_id')

    if not upload_id or not key or not part_number:
        raise toolkit.ValidationError({
            'upload_id': ['Upload ID is required'],
            'key': ['Key is required'],
            'part_number': ['Part number is required']
        })

    try:
        # Create uploader instance
        upload = uploader.get_resource_uploader({
            "package_id": package_id,
            "resource_id": data_dict.get('resource_id'),
            "url_type": 'upload',
        })

        # Use your class method to generate presigned URL for the part
        url = upload.generate_multipart_presigned_url(
            key=key,
            upload_id=upload_id,
            part_number=int(part_number),
            expires_in=3600  # 1 hour
        )

        return {
            'url': url,
            'success': True
        }

    except ClientError as e:
        log.error(f"Error signing part: {e}")
        raise toolkit.ValidationError(
            {'error': [f'Failed to sign part: {str(e)}']})

# Helper function to get bucket name from uploader config


def get_bucket_name():
    """Get the S3 bucket name from the uploader configuration"""
    try:
        # Create a temporary uploader instance to get bucket name
        from ckanext.s3filestore.uploader import BaseS3Uploader
        uploader_instance = BaseS3Uploader()
        return uploader_instance.bucket_name
    except Exception as e:
        log.error(f"Error getting bucket name: {e}")
        return None
