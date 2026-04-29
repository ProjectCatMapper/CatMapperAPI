#!/usr/bin/env python3

import argparse
import sys

from CM.brevo import send_transactional_email


def main() -> int:
    parser = argparse.ArgumentParser(description="Send transactional email through Brevo.")
    parser.add_argument("--to", action="append", required=True, help="Recipient email address. Repeat for multiple recipients.")
    parser.add_argument("--subject", required=True, help="Email subject.")
    parser.add_argument("--sender", default="admin@catmapper.org", help="Sender email address.")
    parser.add_argument("--sender-name", default="CatMapper", help="Sender display name.")
    parser.add_argument("--text", default="", help="Plain-text body.")
    parser.add_argument("--html", default="", help="HTML body.")
    parser.add_argument("--attachment", action="append", default=[], help="Attachment path. Repeat for multiple files.")
    parser.add_argument("--tag", action="append", default=[], help="Brevo tag. Repeat for multiple tags.")
    parser.add_argument("--sandbox", action="store_true", help="Use Brevo sandbox mode without delivering email.")
    args = parser.parse_args()

    text_content = args.text or args.html or "(empty message)"
    html_content = args.html or None

    result = send_transactional_email(
        sender_email=args.sender,
        sender_name=args.sender_name,
        recipients=args.to,
        subject=args.subject,
        text_content=text_content,
        html_content=html_content,
        attachments=args.attachment,
        tags=args.tag or None,
        sandbox=args.sandbox,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
