"""
WSGI entry point for LemurDX Data Check application
Supports URL prefix configuration via URL_PREFIX environment variable
"""
import os
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.wrappers import Response
from app import app as flask_app

# Get URL prefix from environment variable, default to /lemurdx-dashboard-data
URL_PREFIX = os.environ.get('URL_PREFIX', '/lemurdx-dashboard-data')

# Create WSGI application with URL prefix
application = DispatcherMiddleware(
    Response('Not Found', status=404),
    {URL_PREFIX: flask_app}
)

# For gunicorn
app = application

