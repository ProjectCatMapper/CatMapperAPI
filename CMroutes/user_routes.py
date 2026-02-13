from flask import Blueprint, request, jsonify
from CM import getDriver, password_hash, sendEmail, verifyUser, login, enableUser, unlist, getQuery, verifyPassword
import json
from datetime import datetime, timedelta
from threading import Lock
import secrets
import uuid
import os
import logging
from .extensions import mail

user_bp = Blueprint('user', __name__)

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

PROFILE_UPDATE_REQUESTS = {}
PASSWORD_CHANGE_REQUESTS = {}
REQUEST_LOCK = Lock()
REQUEST_TTL_MINUTES = 15
logger = logging.getLogger(__name__)


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _mask_email(email):
    if not email or "@" not in email:
        return "your registered email"
    name, domain = email.split("@", 1)
    if not name:
        return "your registered email"
    if len(name) == 1:
        prefix = "*"
    elif len(name) == 2:
        prefix = f"{name[0]}*"
    else:
        prefix = f"{name[:2]}***"
    return f"{prefix}@{domain}"


def _include_debug_verification_code():
    return os.getenv("PROFILE_DEBUG_CODES", "false").lower() in {"1", "true", "yes", "on"}


def _send_verification_email(email, verification_code, action_label):
    if not email:
        raise Exception("User email is missing; cannot send verification code.")

    sender = config['MAIL']['mail_default']
    subject = f"CatMapper {action_label} Verification Code"
    body = (
        "Hello,\n\n"
        f"We received a request for: {action_label}.\n"
        f"Your verification code is: {verification_code}\n\n"
        f"This code expires in {REQUEST_TTL_MINUTES} minutes.\n\n"
        "If you did not request this change, please ignore this message.\n\n"
        "CatMapper Team"
    )
    result = sendEmail(
        mail=mail,
        subject=subject,
        recipients=[email],
        body=body,
        sender=sender,
    )
    if isinstance(result, str) and result.lower().startswith("error"):
        raise Exception(result)


def _cleanup_requests():
    now = datetime.utcnow()
    with REQUEST_LOCK:
        for store in (PROFILE_UPDATE_REQUESTS, PASSWORD_CHANGE_REQUESTS):
            expired = [key for key, value in store.items() if value["expires_at"] < now]
            for key in expired:
                store.pop(key, None)


def _normalize_database(database_value):
    if isinstance(database_value, list):
        return "|".join(str(item) for item in database_value if item)
    if database_value is None:
        return ""
    return str(database_value)


def _format_profile(row):
    databases = row.get("database") or []
    if isinstance(databases, list):
        database = "|".join(databases)
    else:
        database = str(databases)
    return {
        "userId": row.get("userid", ""),
        "firstName": row.get("first", ""),
        "lastName": row.get("last", ""),
        "username": row.get("username", ""),
        "email": row.get("email", ""),
        "database": database,
        "intendedUse": row.get("intendedUse", ""),
        "createdAt": row.get("createdAt") or _now_iso(),
        "updatedAt": row.get("updatedAt") or _now_iso(),
        "passwordLastChangedAt": row.get("passwordLastChangedAt") or _now_iso(),
    }


def _password_meets_policy(password):
    # New policy: letters only, minimum length 6.
    if not isinstance(password, str):
        return False
    return len(password) >= 6 and password.isalpha()


def _load_user(userid):
    driver = getDriver("userdb")
    query = """
    MATCH (u:USER {userid: toString($userid)})
    RETURN
      u.userid as userid,
      u.first as first,
      u.last as last,
      u.username as username,
      u.email as email,
      u.database as database,
      u.intendedUse as intendedUse,
      u.createdAt as createdAt,
      u.updatedAt as updatedAt,
      u.passwordLastChangedAt as passwordLastChangedAt,
      u.password as password
    """
    data = getQuery(query, driver=driver, params={"userid": userid})
    if not data:
        raise Exception("User not found")
    return data[0]


def _verify_profile_credentials(userid, credentials):
    if not credentials:
        raise Exception("Missing credentials")
    credential_userid = credentials.get("userid")
    credential_key = credentials.get("key")
    if not credential_userid or not credential_key:
        raise Exception("Missing credential fields")
    if str(credential_userid) != str(userid):
        raise Exception("Credentials do not match requested user")
    verified = verifyUser(str(credential_userid), credential_key)
    if verified != "verified":
        raise Exception("User is not verified")
    return True

@user_bp.route('/newuser', methods=['POST'])
def getnewuser():
    try:

        mail_default = config['MAIL']['mail_default']
        data = request.get_data()
        data = json.loads(data)
        database = data.get("database")
        firstName = data.get("firstName")
        lastName = data.get("lastName")
        email = data.get("email")
        username = data.get("username")
        password = data.get("password")
        password = password_hash(password)
        intendedUse = data.get("intendedUse")

        if database.lower() == "sociomap":
            database = "SocioMap"
        elif database.lower() == "archamap":
            database = "ArchaMap"
        else:
            raise Exception("database must be 'SocioMap' or 'ArchaMap'")

        driver = getDriver("userdb")

        queryExists = """
        MATCH (u:USER {username: $username})
        return true as exists
        """
        data = getQuery(
            queryExists, driver = driver, username=username, database=database)

        if isinstance(data, list) and data and data[0].get("exists") is not None:
            raise Exception(
                "Username already exists. Please try another username.")

        queryExists = """
        MATCH (u:USER {email: $email})
        return true as exists
        """
        data = getQuery(
            queryExists, driver = driver, email=email, database=database)

        if isinstance(data, list) and data and data[0].get("exists") is not None:
            raise Exception(
                "Account with this email already exists. Please contact admin@catmapper.org to reset password.")

        query = """
                match (p:USER) with toInteger(p.userid) + 1 as id order by id desc limit 1
                merge (u:USER {username: $username})
                on create set u.username = $username,
                u.first = $firstName,
                u.last = $lastName,
                u.email = $email,
                u.access = "pending",
                u.log = [toString(datetime()) + ": created user via API", toString(datetime()) + \
                                ": created autoapproved via API during workshop registration"],
                u.password = $password,
                u.userid = toString(id),
                u.role = 'user',
                u.intendedUse = $intendedUse,
                u.database = split($database,"|")
                return u.userid as userid
                """

        data = getQuery(query, driver = driver, firstName=firstName, lastName=lastName, email=email,
                                 password=password, username=username, intendedUse=intendedUse, database=database)

        body = f"""
                Hello,
                A new user has just requested registration.
                Name: {firstName} {lastName}
                email: {email}
                database: {database}
                description: {intendedUse}
                """

        sendEmail(mail, subject="New registered user", recipients=[
            "admin@catmapper.org"], body=body, sender=mail_default)

        return jsonify({"message": "Success"}), 200

    except Exception as e:
        # Check for specific error messages
        error_message = str(e)

        print(error_message)

        if "Account with this email already exists." in error_message:
            return jsonify({"error": str(e)}), 400    # Return 400 Bad Request

        elif "Username already exists" in error_message:
            return jsonify({"error": str(e)}), 400   # Return 400 Bad Request

        else:
            # Default error message
            return jsonify({"error": "please contact admin@catmapper.org. Error:" + error_message}), 500

@user_bp.route('/login', methods=['POST'])
def getLogin():
    try:
        data = request.get_data()
        data = json.loads(data)
        user = unlist(data.get('user'))
        password = unlist(data.get('password'))

        credentials = login(user, password)

        return credentials

    except Exception as e:
        result = str(e)
        return result, 500


@user_bp.route('/profile/<userid>', methods=['GET'])
def get_profile(userid):
    try:
        credentials_raw = request.args.get("credentials")
        credentials = json.loads(credentials_raw) if credentials_raw else None
        _verify_profile_credentials(userid, credentials)
        user_row = _load_user(userid)
        return jsonify(_format_profile(user_row)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/request-update', methods=['POST'])
def request_profile_update():
    try:
        _cleanup_requests()
        data = request.get_json(silent=True) or {}
        userid = unlist(data.get("userId"))
        updates = data.get("updates") or {}
        credentials = data.get("credentials")

        if not userid:
            raise Exception("Missing userId")
        _verify_profile_credentials(userid, credentials)

        required_fields = ["firstName", "lastName", "username", "email", "database"]
        missing = [field for field in required_fields if not updates.get(field)]
        if missing:
            raise Exception(f"Missing required fields: {', '.join(missing)}")

        existing = _load_user(userid)
        driver = getDriver("userdb")

        username_query = """
        MATCH (u:USER {username: $username})
        WHERE u.userid <> toString($userid)
        RETURN count(*) as count
        """
        username_exists = getQuery(
            username_query,
            driver=driver,
            params={"username": updates.get("username"), "userid": userid},
            type="list",
        )[0]
        if username_exists > 0:
            raise Exception("Username already exists. Please try another username.")

        email_query = """
        MATCH (u:USER {email: $email})
        WHERE u.userid <> toString($userid)
        RETURN count(*) as count
        """
        email_exists = getQuery(
            email_query,
            driver=driver,
            params={"email": updates.get("email"), "userid": userid},
            type="list",
        )[0]
        if email_exists > 0:
            raise Exception("Account with this email already exists.")

        request_id = f"profile_{uuid.uuid4().hex[:12]}"
        verification_code = f"{secrets.randbelow(900000) + 100000}"
        with REQUEST_LOCK:
            PROFILE_UPDATE_REQUESTS[request_id] = {
                "userid": str(userid),
                "updates": updates,
                "verification_code": verification_code,
                "expires_at": datetime.utcnow() + timedelta(minutes=REQUEST_TTL_MINUTES),
            }

        target_email = updates.get("email") or existing.get("email")
        _send_verification_email(target_email, verification_code, "Profile Update")
        logger.info(
            "Profile verification email sent: userid=%s request_id=%s email=%s",
            userid,
            request_id,
            _mask_email(target_email),
        )

        response = {
            "requestId": request_id,
            "maskedEmail": _mask_email(target_email),
        }
        if _include_debug_verification_code():
            response["debugVerificationCode"] = verification_code

        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/confirm-update', methods=['POST'])
def confirm_profile_update():
    try:
        _cleanup_requests()
        data = request.get_json(silent=True) or {}
        userid = unlist(data.get("userId"))
        request_id = unlist(data.get("requestId"))
        verification_code = str(unlist(data.get("verificationCode")) or "").strip()
        credentials = data.get("credentials")

        if not userid or not request_id or not verification_code:
            raise Exception("Missing required confirmation fields")
        _verify_profile_credentials(userid, credentials)

        with REQUEST_LOCK:
            pending = PROFILE_UPDATE_REQUESTS.get(request_id)
            if not pending or pending.get("userid") != str(userid):
                raise Exception("Profile update request not found. Please request a new verification email.")
            if pending.get("verification_code") != verification_code:
                raise Exception("Invalid verification code.")
            updates = pending.get("updates", {})
            PROFILE_UPDATE_REQUESTS.pop(request_id, None)

        driver = getDriver("userdb")
        query = """
        MATCH (u:USER {userid: toString($userid)})
        SET
          u.first = $first,
          u.last = $last,
          u.username = $username,
          u.email = $email,
          u.database = split($database, '|'),
          u.intendedUse = $intendedUse,
          u.updatedAt = $updatedAt
        RETURN
          u.userid as userid,
          u.first as first,
          u.last as last,
          u.username as username,
          u.email as email,
          u.database as database,
          u.intendedUse as intendedUse,
          u.createdAt as createdAt,
          u.updatedAt as updatedAt,
          u.passwordLastChangedAt as passwordLastChangedAt
        """
        saved = getQuery(
            query,
            driver=driver,
            params={
                "userid": userid,
                "first": updates.get("firstName", ""),
                "last": updates.get("lastName", ""),
                "username": updates.get("username", ""),
                "email": updates.get("email", ""),
                "database": _normalize_database(updates.get("database")),
                "intendedUse": updates.get("intendedUse", ""),
                "updatedAt": _now_iso(),
            },
        )
        if not saved:
            raise Exception("User not found")
        return jsonify(_format_profile(saved[0])), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/request-password-change', methods=['POST'])
def request_password_change():
    try:
        _cleanup_requests()
        data = request.get_json(silent=True) or {}
        userid = unlist(data.get("userId"))
        current_password = unlist(data.get("currentPassword"))
        new_password = unlist(data.get("newPassword"))
        credentials = data.get("credentials")

        if not userid:
            raise Exception("Missing userId")
        if not current_password or not new_password:
            raise Exception("Current and new password are required.")
        if not _password_meets_policy(new_password):
            raise Exception("Password must be at least 6 letters and contain no numbers or special characters.")

        _verify_profile_credentials(userid, credentials)
        existing = _load_user(userid)
        stored_hash = existing.get("password", "")
        if stored_hash and not verifyPassword(stored_hash, current_password):
            raise Exception("Current password is incorrect.")

        request_id = f"password_{uuid.uuid4().hex[:12]}"
        verification_code = f"{secrets.randbelow(900000) + 100000}"
        with REQUEST_LOCK:
            PASSWORD_CHANGE_REQUESTS[request_id] = {
                "userid": str(userid),
                "password_hash": password_hash(new_password),
                "verification_code": verification_code,
                "expires_at": datetime.utcnow() + timedelta(minutes=REQUEST_TTL_MINUTES),
            }

        target_email = existing.get("email")
        _send_verification_email(target_email, verification_code, "Password Change")
        logger.info(
            "Password verification email sent: userid=%s request_id=%s email=%s",
            userid,
            request_id,
            _mask_email(target_email),
        )

        response = {
            "requestId": request_id,
            "maskedEmail": _mask_email(target_email),
        }
        if _include_debug_verification_code():
            response["debugVerificationCode"] = verification_code

        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/confirm-password-change', methods=['POST'])
def confirm_password_change():
    try:
        _cleanup_requests()
        data = request.get_json(silent=True) or {}
        userid = unlist(data.get("userId"))
        request_id = unlist(data.get("requestId"))
        verification_code = str(unlist(data.get("verificationCode")) or "").strip()
        credentials = data.get("credentials")

        if not userid or not request_id or not verification_code:
            raise Exception("Missing required confirmation fields")
        _verify_profile_credentials(userid, credentials)

        with REQUEST_LOCK:
            pending = PASSWORD_CHANGE_REQUESTS.get(request_id)
            if not pending or pending.get("userid") != str(userid):
                raise Exception("Password change request not found. Please request a new verification email.")
            if pending.get("verification_code") != verification_code:
                raise Exception("Invalid verification code.")
            password_hash_value = pending.get("password_hash")
            PASSWORD_CHANGE_REQUESTS.pop(request_id, None)

        driver = getDriver("userdb")
        query = """
        MATCH (u:USER {userid: toString($userid)})
        SET
          u.password = $password,
          u.passwordLastChangedAt = $changedAt,
          u.updatedAt = $changedAt
        RETURN u.passwordLastChangedAt as passwordLastChangedAt
        """
        saved = getQuery(
            query,
            driver=driver,
            params={"userid": userid, "password": password_hash_value, "changedAt": _now_iso()},
        )
        if not saved:
            raise Exception("User not found")
        return jsonify({"passwordLastChangedAt": saved[0].get("passwordLastChangedAt")}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@user_bp.route('/updateNewUsers', methods=['POST'])
def updateNewUsers():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get('database'))
        credentials = unlist(data.get('credentials'))
        process = unlist(data.get('process'))
        userid = data.get('userid')

        verified = verifyUser(credentials.get(
            "userid"), credentials.get("key"), "admin")

        if verified != "verified":
            raise Exception("Error: User is not verified")

        approver = credentials.get("userid")

        result = enableUser(database, process=process,
                            userid=userid, approver=approver)

        if isinstance(result, list) and process == "approve":

            users = [user for user in result if user.get("email")]
            if len(users) > 0:
                for user in users:
                    body = f"""
        Hello {user.get("first")} {user.get("last")},

        Your registration has been approved. You can now access the CatMapper applications. Please see catmapper.org/help or email support@catmapper.org for any questions.

        Best,
        CatMapper Team
                    """
                    sendEmail(mail, subject="CatMapper Registration Approved", recipients=[user.get(
                        "email"), 'admin@catmapper.org'], body=body, sender="admin@catmapper.org")

        return result
    except Exception as e:
        result = str(e)
        return result, 500
