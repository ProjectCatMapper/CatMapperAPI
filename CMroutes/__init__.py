from .test_routes import *
from .merge_routes import *
from .routines import *
from .query import *

from flask import Flask, abort, send_from_directory
from .extensions import *

from dotenv import load_dotenv, find_dotenv
import os
from flask_cors import CORS
load_dotenv(find_dotenv())


def create_app():
    app = Flask(__name__,
                template_folder=os.path.join(
                    os.path.dirname(__file__), '../templates'),
                static_folder=os.path.join(os.path.dirname(__file__), '../static'))

    CORS(app)
    app.config['CORS_HEADERS'] = 'Content-Type'
    app.config['PERMANENT_SESSION_LIFETIME'] = 999999999
    app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

    app.config['MAIL_SERVER'] = os.getenv(
        "mail_server")
    app.config['MAIL_PORT'] = os.getenv("mail_port")
    app.config['MAIL_USE_TLS'] = True  # Use TLS
    app.config['MAIL_USE_SSL'] = False  # Use SSL (False if using TLS)
    app.config['MAIL_USERNAME'] = os.getenv("mail_address")  # Your email
    app.config['MAIL_PASSWORD'] = os.getenv("mail_pwd")  # Your email password
    app.config['MAIL_DEFAULT_SENDER'] = os.getenv(
        "mail_default")  # Default sender
    mail.init_app(app)

    return app
