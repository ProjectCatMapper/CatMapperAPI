#!/usr/bin/env python3
"""Send mail using the same configured CatMapper mail helper as the API."""

import argparse
import json
import os
from configparser import ConfigParser
from pathlib import Path

from flask import Flask
from flask_mail import Mail


SCRIPT_DIR = Path(__file__).resolve().parent
os.chdir(SCRIPT_DIR)

from CM.email import get_alert_recipients, get_default_sender, sendEmail  # noqa: E402


def _as_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_or_config(config, env_name, section, option, fallback=""):
    if env_name in os.environ:
        return os.environ.get(env_name)
    if config.has_option(section, option):
        return config.get(section, option)
    return fallback


def _build_mail():
    config = ConfigParser()
    config.read(SCRIPT_DIR / "config.ini")

    app = Flask(__name__)
    mail_port_raw = _env_or_config(config, "MAIL_PORT", "MAIL", "mail_port", "25")
    try:
        mail_port = int(mail_port_raw)
    except (TypeError, ValueError):
        mail_port = 25

    mail_open_timeout_raw = _env_or_config(config, "MAIL_OPEN_TIMEOUT", "MAIL", "mail_open_timeout", "15")
    mail_read_timeout_raw = _env_or_config(config, "MAIL_READ_TIMEOUT", "MAIL", "mail_read_timeout", "15")
    try:
        mail_open_timeout = int(mail_open_timeout_raw)
    except (TypeError, ValueError):
        mail_open_timeout = 15
    try:
        mail_read_timeout = int(mail_read_timeout_raw)
    except (TypeError, ValueError):
        mail_read_timeout = 15

    app.config.update(
        MAIL_SERVER=_env_or_config(config, "MAIL_SERVER", "MAIL", "mail_server", ""),
        MAIL_PORT=mail_port,
        MAIL_USE_TLS=_as_bool(_env_or_config(config, "MAIL_USE_TLS", "MAIL", "mail_use_tls", "false")),
        MAIL_USE_SSL=_as_bool(_env_or_config(config, "MAIL_USE_SSL", "MAIL", "mail_use_ssl", "false")),
        MAIL_USERNAME=_env_or_config(config, "MAIL_USERNAME", "MAIL", "mail_address", "") or None,
        MAIL_PASSWORD=_env_or_config(config, "MAIL_PASSWORD", "MAIL", "mail_pwd", "") or None,
        MAIL_DEFAULT_SENDER=_env_or_config(config, "MAIL_DEFAULT_SENDER", "MAIL", "mail_default", ""),
        MAIL_DOMAIN=_env_or_config(config, "MAIL_DOMAIN", "MAIL", "mail_domain", "") or None,
        MAIL_TIMEOUT=max(mail_open_timeout, mail_read_timeout),
    )

    mail = Mail()
    mail.init_app(app)
    return app, mail


def main():
    parser = argparse.ArgumentParser(description="Send email through CatMapper's configured mail path.")
    parser.add_argument("--to", action="append", default=[], help="Recipient email address. Defaults to alert recipients.")
    parser.add_argument("--subject", required=True, help="Email subject.")
    parser.add_argument("--sender", default="", help="Sender email address. Defaults to configured MAIL default sender.")
    parser.add_argument("--text", default="", help="Plain-text body.")
    parser.add_argument("--html", default="", help="HTML body.")
    parser.add_argument("--attachment", action="append", default=[], help="Attachment path. Repeat for multiple files.")
    args = parser.parse_args()

    sender = args.sender.strip() or get_default_sender()
    recipients = args.to or get_alert_recipients()
    if not sender:
        raise RuntimeError("No sender configured. Set MAIL_DEFAULT_SENDER or MAIL.mail_default.")
    if not recipients:
        raise RuntimeError("No recipients configured. Set MAIL_ALERT_RECIPIENTS or pass --to.")

    body = args.html or args.text or "(empty message)"
    app, mail = _build_mail()
    with app.app_context():
        result = sendEmail(
            mail=mail,
            subject=args.subject,
            recipients=recipients,
            body=body,
            sender=sender,
            attachments=args.attachment,
            html=bool(args.html),
            return_metadata=True,
        )

    print(json.dumps(result, sort_keys=True))
    status = result.get("status", "") if isinstance(result, dict) else str(result)
    return 0 if status == "Email sent successfully" else 1


if __name__ == "__main__":
    raise SystemExit(main())
