from .test_routes import *
from .merge_routes import *
from .routines import *
from .query import *
from .getLogs import *
from .getDownloads import *

from flask import Flask, abort, send_from_directory
from .extensions import *

import os
from flask_cors import CORS
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


def create_app():
    app = Flask(__name__,
                template_folder=os.path.join(
                    os.path.dirname(__file__), '../templates'),
                static_folder=os.path.join(os.path.dirname(__file__), '../static'))

    CORS(app)
    app.config['CORS_HEADERS'] = 'Content-Type'
    app.config['PERMANENT_SESSION_LIFETIME'] = 999999999
    app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

    app.config['MAIL_SERVER'] = config['MAIL']['mail_server']
    app.config['MAIL_PORT'] = config['MAIL']['mail_port']
    app.config['MAIL_USE_TLS'] = True  # Use TLS
    app.config['MAIL_USE_SSL'] = False  # Use SSL (False if using TLS)
    app.config['MAIL_USERNAME'] = config['MAIL']['mail_address']  # Your email
    # Your email password
    app.config['MAIL_PASSWORD'] = config['MAIL']['mail_pwd']
    # Default sender
    app.config['MAIL_DEFAULT_SENDER'] = config['MAIL']['mail_default']
    mail.init_app(app)

    return app
