from CM import *
from flask import request
from flask_mail import Mail
from .extensions import mail


def testmsg(database, msg):
    return "This is a test message from the " + database + " database that says: " + msg


def send_test_email():
    try:
        msg = sendEmail(mail, "Test Email", [
            "bischrob@gmail.com"], "This is a test email sent from a Flask application. Have fun.", "admin@catmapper.org")
        return msg
    except Exception as e:
        return str(e), 500
