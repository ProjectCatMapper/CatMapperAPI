# email.py

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4
from configparser import ConfigParser
from flask_mail import Mail, Message

_config = ConfigParser()
_config.read("config.ini")


def _env_or_config(env_name: str, section: str, option: str, fallback: str = "") -> str:
    if env_name in os.environ:
        return str(os.environ.get(env_name) or fallback).strip()
    if _config.has_option(section, option):
        return str(_config.get(section, option) or fallback).strip()
    return fallback


def _parse_recipients(value: str) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item and item.strip()]


def get_default_sender() -> str:
    return _env_or_config("MAIL_DEFAULT_SENDER", "MAIL", "mail_default", "")


def get_alert_recipients() -> List[str]:
    value = _env_or_config("MAIL_ALERT_RECIPIENTS", "MAIL", "mail_alert_recipients", "")
    if value:
        return _parse_recipients(value)
    default_sender = get_default_sender()
    return [default_sender] if default_sender else []


def get_weekly_recipients() -> List[str]:
    value = _env_or_config("MAIL_WEEKLY_RECIPIENTS", "MAIL", "mail_weekly_recipients", "")
    if value:
        return _parse_recipients(value)
    return get_alert_recipients()


def get_support_email() -> str:
    support = _env_or_config("MAIL_SUPPORT_EMAIL", "MAIL", "mail_support_email", "")
    if support:
        return support
    return get_default_sender()


def _build_mail_audit(sender: str, recipients: List[str]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    trace_id = uuid4().hex
    sender_domain = sender.split("@", 1)[1] if "@" in sender else "catmapper.org"
    message_id = f"<{trace_id}.{int(now.timestamp())}@{sender_domain}>"
    return {
        "trace_id": trace_id,
        "message_id": message_id,
        "sender": sender,
        "recipients": recipients or [],
        "sent_at_utc": now.isoformat(),
    }


def _log_mail_audit(audit: Dict[str, Any], status: str, error: Optional[str] = None) -> None:
    recipients = ",".join(audit.get("recipients", []))
    msg = (
        "MAIL_AUDIT "
        f"status={status} "
        f"trace_id={audit.get('trace_id')} "
        f"message_id={audit.get('message_id')} "
        f"sender={audit.get('sender')} "
        f"recipients={recipients} "
        f"sent_at_utc={audit.get('sent_at_utc')}"
    )
    if error:
        clean_error = " ".join(str(error).split())
        msg += f' error="{clean_error}"'
    print(msg)


def sendEmail(
    mail: Mail,
    subject: str,
    recipients: List[str],
    body: str,
    sender: str,
    attachments: Optional[List[str]] = None,
    html: bool = False,
    return_metadata: bool = False,
):
    """
    Send an email using Flask-Mail with optional attachments.

    Args:
        mail (Mail): The Flask-Mail instance.
        subject (str): Subject of the email.
        recipients (List[str]): List of recipient email addresses.
        body (str): Body text of the email.
        sender (str): Sender's email address.
        attachments (Optional[List[str]]): List of file paths to attach to the email.
        html (bool): If True, send `body` as HTML and include a text fallback.

    Returns:
        str | dict: Success/error string, or audit dict when
        `return_metadata=True`.
    """
    audit = _build_mail_audit(sender, recipients)
    try:
        # Create the email message
        msg = Message(subject, recipients=recipients, sender=sender)
        if msg.extra_headers is None:
            msg.extra_headers = {}
        msg.extra_headers["Message-ID"] = audit["message_id"]
        msg.extra_headers["X-CatMapper-Trace-ID"] = audit["trace_id"]
        msg.extra_headers["X-CatMapper-Sent-At"] = audit["sent_at_utc"]

        if html:
            # Keep a plain-text fallback for clients that do not render HTML.
            text_body = re.sub(r"<\s*br\s*/?\s*>", "\n", body, flags=re.IGNORECASE)
            text_body = re.sub(r"</\s*(p|tr|table|h[1-6])\s*>", "\n", text_body, flags=re.IGNORECASE)
            text_body = re.sub(r"<[^>]+>", "", text_body)
            msg.body = text_body
            msg.html = body
        else:
            msg.body = body

        # Attach files if provided
        if attachments:
            for file_path in attachments:
                try:
                    with open(file_path, "rb") as file:
                        # Extract filename and MIME type
                        filename = file_path.split("/")[-1]
                        mime_type = "application/octet-stream"  # Default MIME type
                        
                        # Attach the file
                        msg.attach(
                            filename=filename,
                            content_type=mime_type,
                            data=file.read()
                        )
                except FileNotFoundError:
                    status = f"Error: Attachment file '{file_path}' not found."
                    _log_mail_audit(audit, "error", status)
                    if return_metadata:
                        return {**audit, "status": status}
                    return status

        # Send the email
        mail.send(msg)
        status = "Email sent successfully"
        _log_mail_audit(audit, "success")
        if return_metadata:
            return {**audit, "status": status}
        return status
    except Exception as e:
        status = f"Error sending email: {str(e)}"
        _log_mail_audit(audit, "error", status)
        if return_metadata:
            return {**audit, "status": status}
        return status
