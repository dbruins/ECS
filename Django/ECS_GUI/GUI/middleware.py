#Session model stores the session data
from django.contrib.sessions.models import Session

class OneSessionPerUserMiddleware:
    """prevents users from having multiple sessions"""
    # Called only once when the web server starts
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Code to be executed for each request before the view (and later middleware) are called.

        #this doesn't work for superusers because the login and logout signals are not called
        #skip for unauthenticated users and superusers
        if request.user.is_authenticated and not request.user.is_superuser:
            #get user associated session key from database
            stored_session_key = request.user.logged_in_user.session_key

            #if session keys don't match destroy currently stored session
            if stored_session_key and stored_session_key != request.session.session_key:
                try:
                    Session.objects.get(session_key=stored_session_key).delete()
                except Session.DoesNotExist:
                    pass


            request.user.logged_in_user.session_key = request.session.session_key
            request.user.logged_in_user.save()

        response = self.get_response(request)

        return response
