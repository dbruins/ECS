from django.conf import settings

def template_settings(request):
    """make needed settings values available in all templates"""
    return {
        'ENCRYPT_WEBSOCKET': settings.ENCRYPT_WEBSOCKET,
        'WEB_SOCKET_PORT': settings.WEB_SOCKET_PORT
    }
