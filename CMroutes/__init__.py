from .dev_routes import *
from .merge_routes import *
from .routine_routes import *
from .download_routes import *
from .log_routes import *
from .docs_routes import *
from .explore_routes import *
from .upload_routes import *
from .user_routes import *
from .admin_routes import *
from .homepage_routes import *
from .metadata_routes import *
from flask import Flask
from .extensions import *
from .search_routes import *

import os
from flask_cors import CORS
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')
base_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()

def create_app():
    app = Flask(__name__,
                template_folder=os.path.join(
                    os.path.dirname(base_dir), 'templates'),
                static_folder=os.path.join(os.path.dirname(base_dir), 'static'))

    allowed_origins = [
    "https://catmapper.org",
    "https://test.catmapper.org",
    r"http://localhost:\d+"
    ]

    CORS(
        app,
        resources={
            r"/*": {
                "origins": allowed_origins,
                "allow_headers": ["Content-Type", "Authorization"],
            }
        },
    )

    app.config['CORS_HEADERS'] = 'Content-Type, Authorization'
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
