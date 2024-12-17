# email.py

from typing import List, Optional
from flask_mail import Mail, Message

def sendEmail(
    mail: Mail,
    subject: str,
    recipients: List[str],
    body: str,
    sender: str,
    attachments: Optional[List[str]] = None
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

    Returns:
        str: Success or error message.
    """
    try:
        # Create the email message
        msg = Message(subject, recipients=recipients, sender=sender)
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
                    return f"Error: Attachment file '{file_path}' not found."

        # Send the email
        mail.send(msg)

        return "Email sent successfully"
    except Exception as e:
        return f"Error sending email: {str(e)}"
