# email.py

from typing import List
from flask_mail import Mail, Message

def sendEmail(mail, subject: str, recipients: List[str], body: str, sender: str):
    try:
        msg = Message(subject, recipients=recipients,sender=sender)
        msg.body = body
        mail.send(msg)

        return "Email sent"
    except Exception as e:
        return f"Error sending email: {str(e)}"