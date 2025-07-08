import logging
import ckan.plugins.toolkit as toolkit

log = logging.getLogger(__name__)

def get_package_by_name(pkg_name: str): 
    return toolkit.get_action('package_show')({'ignore_auth': True}, {'id': pkg_name})


log = logging.getLogger(__name__)

def get_or_create_user_api_key_safe():
    """
    Safe version that handles database transactions more carefully.
    
    Returns:
        str: The user's API key, or None if no user is logged in or error occurs
    """
    try:
        # Get current user
        user = toolkit.c.userobj
        
        if not user:
            log.info(f'user not signed in')
            return None
        
        if user.apikey:
            log.info(f'user already have an api key')
            return user.apikey
        
        return generate_token(user)
    except Exception as e:
        log.error(f"Error in get_or_create_user_api_key_safe: {str(e)}")
        return None

def generate_token(user):
    context = {}
    context['ignore_auth'] = True
    log.info(user)
    log.info(user.name)
    try:
        api_tokens = toolkit.get_action('api_token_list')(
            context, {'user_id': user.name}
        )

        for token in api_tokens:
            if token['name'] == 'frontend_token':
                toolkit.get_action('api_token_revoke')(
                    context, {'jti': token['id']})

        frontend_token = toolkit.get_action('api_token_create')(
            context, {'user': user.name, 'name': 'frontend_token'}
        )

        return frontend_token.get('token')

    except Exception as e:
        log.error(e)

    return None