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
from .extensions import mail
from .search_routes import *

import os
from flask_cors import CORS
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')
base_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()


def _as_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_or_config(env_name, section, option, fallback=None):
    if env_name in os.environ:
        return os.environ.get(env_name)
    if config.has_option(section, option):
        return config.get(section, option)
    return fallback

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

    mail_server = _env_or_config("MAIL_SERVER", "MAIL", "mail_server", "smtp-relay.sendinblue.com")
    mail_port_raw = _env_or_config("MAIL_PORT", "MAIL", "mail_port", "587")
    try:
        mail_port = int(mail_port_raw)
    except (TypeError, ValueError):
        mail_port = 587

    # Default transport behavior remains TLS/587 unless overridden via env.
    mail_use_tls = _as_bool(_env_or_config("MAIL_USE_TLS", "MAIL", "mail_use_tls", "true"), default=True)
    mail_use_ssl = _as_bool(_env_or_config("MAIL_USE_SSL", "MAIL", "mail_use_ssl", "false"), default=False)
    mail_username = _env_or_config("MAIL_USERNAME", "MAIL", "mail_address", "")
    mail_password = _env_or_config("MAIL_PASSWORD", "MAIL", "mail_pwd", "")
    mail_default_sender = _env_or_config("MAIL_DEFAULT_SENDER", "MAIL", "mail_default", "admin@catmapper.org")

    app.config['MAIL_SERVER'] = mail_server
    app.config['MAIL_PORT'] = mail_port
    app.config['MAIL_USE_TLS'] = mail_use_tls
    app.config['MAIL_USE_SSL'] = mail_use_ssl
    app.config['MAIL_USERNAME'] = mail_username or None
    app.config['MAIL_PASSWORD'] = mail_password or None
    app.config['MAIL_DEFAULT_SENDER'] = mail_default_sender
    mail.init_app(app)

    return app
