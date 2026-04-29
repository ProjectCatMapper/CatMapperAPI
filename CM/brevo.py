import base64
import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib import error, request


BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"


def get_brevo_api_key() -> str:
    for env_name in ("BREVO_API_KEY", "API_KEY"):
        value = str(os.getenv(env_name, "") or "").strip()
        if value:
            return value
    return ""


def brevo_enabled() -> bool:
    return bool(get_brevo_api_key())


def _attachment_payload(attachments: Optional[Iterable[str]]) -> List[Dict[str, str]]:
    payload: List[Dict[str, str]] = []
    for file_path in attachments or []:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Attachment file '{file_path}' not found.")
        payload.append(
            {
                "name": path.name,
                "content": base64.b64encode(path.read_bytes()).decode("ascii"),
            }
        )
    return payload


def send_transactional_email(
    *,
    sender_email: str,
    sender_name: str,
    recipients: List[str],
    subject: str,
    text_content: str,
    html_content: Optional[str] = None,
    attachments: Optional[Iterable[str]] = None,
    headers: Optional[Dict[str, str]] = None,
    reply_to: Optional[str] = None,
    tags: Optional[List[str]] = None,
    sandbox: bool = False,
    timeout: int = 30,
) -> Dict[str, object]:
    api_key = get_brevo_api_key()
    if not api_key:
        raise RuntimeError("Brevo API key is not configured.")

    payload: Dict[str, object] = {
        "sender": {
            "email": sender_email,
            "name": sender_name,
        },
        "to": [{"email": item} for item in recipients or []],
        "subject": subject,
        "textContent": text_content,
    }
    if html_content:
        payload["htmlContent"] = html_content

    attachment_payload = _attachment_payload(attachments)
    if attachment_payload:
        payload["attachment"] = attachment_payload

    merged_headers = dict(headers or {})
    if sandbox:
        merged_headers["X-Sib-Sandbox"] = "drop"
    if merged_headers:
        payload["headers"] = merged_headers

    if reply_to:
        payload["replyTo"] = {"email": reply_to}
    if tags:
        payload["tags"] = tags

    req = request.Request(
        BREVO_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "accept": "application/json",
            "api-key": api_key,
            "content-type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Brevo API error {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Brevo API request failed: {exc.reason}") from exc

    try:
        parsed = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        parsed = {"raw": raw}

    return parsed
