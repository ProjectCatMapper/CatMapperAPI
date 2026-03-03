from flask import Blueprint, request, jsonify
from CM import (
    getDriver,
    password_hash,
    sendEmail,
    verifyUser,
    login,
    enableUser,
    unlist,
    getQuery,
    verifyPassword,
    get_default_sender,
    get_alert_recipients,
    get_support_email,
)
import json
from datetime import datetime, timedelta, timezone
from threading import Lock
import secrets
import uuid
import os
import logging
from .extensions import mail
from .auth_utils import issue_auth_token, verify_request_auth, verify_bearer_auth

user_bp = Blueprint('user', __name__)

PROFILE_UPDATE_REQUESTS = {}
PASSWORD_CHANGE_REQUESTS = {}
API_KEY_CREATE_REQUESTS = {}
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


def _send_verification_email(email, verification_code, action_label, username=None):
    if not email:
        raise Exception("User email is missing; cannot send verification code.")

    sender = get_default_sender()
    subject = f"CatMapper {action_label} Verification Code"
    username_line = f"Username: {username}\n\n" if username else ""
    body = (
        "Hello,\n\n"
        f"We received a request for: {action_label}.\n"
        f"{username_line}"
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
        for store in (PROFILE_UPDATE_REQUESTS, PASSWORD_CHANGE_REQUESTS, API_KEY_CREATE_REQUESTS):
            expired = [key for key, value in store.items() if value["expires_at"] < now]
            for key in expired:
                store.pop(key, None)


def _normalize_database(database_value):
    if isinstance(database_value, list):
        return "|".join(str(item) for item in database_value if item)
    if database_value is None:
        return ""
    return str(database_value)


def _read_json_payload():
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data
    raw = request.get_data(as_text=True) or ""
    if raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}


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
        "hasApiKey": bool(row.get("apiKeyHash")),
        "apiKeyCreatedAt": row.get("apiKeyCreatedAt") or "",
    }


def _password_meets_policy(password):
    # Policy: minimum length 6. Numbers/special characters are allowed but not required.
    if not isinstance(password, str):
        return False
    return len(password) >= 6


def _serialize_entries(entries):
    return [json.dumps(entry, separators=(",", ":"), ensure_ascii=True) for entry in entries]


def _deserialize_entries(values):
    rows = []
    for value in values or []:
        try:
            row = json.loads(value) if isinstance(value, str) else value
            if isinstance(row, dict):
                rows.append(row)
        except Exception:
            continue
    return rows


def _get_user_entries(userid, field_name):
    driver = getDriver("userdb")
    query = f"""
    MATCH (u:USER {{userid: toString($userid)}})
    RETURN coalesce(u.{field_name}, []) as entries
    """
    rows = getQuery(query, driver=driver, params={"userid": userid})
    if not rows:
        raise Exception("User not found")
    return _deserialize_entries(rows[0].get("entries", []))


def _set_user_entries(userid, field_name, entries):
    driver = getDriver("userdb")
    query = f"""
    MATCH (u:USER {{userid: toString($userid)}})
    SET u.{field_name} = $entries
    RETURN u.userid as userid
    """
    result = getQuery(
        query,
        driver=driver,
        params={"userid": userid, "entries": _serialize_entries(entries)},
    )
    if not result:
        raise Exception("User not found")


def _get_cmid_type(cmid):
    if not isinstance(cmid, str):
        return "UNKNOWN"
    if cmid.startswith(("SD", "AD")):
        return "DATASET"
    if cmid.startswith(("SM", "AM")):
        return "CATEGORY"
    return "UNKNOWN"


def _lookup_cmid_name(database, cmid):
    try:
        driver = getDriver(database)
        query = """
        MATCH (n {CMID: $cmid})
        RETURN n.CMName as CMName
        LIMIT 1
        """
        rows = getQuery(query, driver=driver, params={"cmid": cmid})
        if rows and rows[0].get("CMName"):
            return rows[0]["CMName"]
    except Exception:
        return ""
    return ""


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
      u.password as password,
      coalesce(u.apiKeyHash, '') as apiKeyHash,
      u.apiKeyCreatedAt as apiKeyCreatedAt
    """
    data = getQuery(query, driver=driver, params={"userid": userid})
    if not data:
        raise Exception("User not found")
    return data[0]


def _load_user_by_identifier(identifier):
    lookup = str(identifier or "").strip()
    if not lookup:
        raise Exception("Missing user identifier")

    driver = getDriver("userdb")
    query = """
    MATCH (u:USER)
    WHERE toString(u.userid) = toString($lookup)
       OR toLower(u.username) = toLower($lookup)
       OR toLower(u.email) = toLower($lookup)
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
      u.password as password,
      coalesce(u.access, '') as access
    LIMIT 1
    """
    rows = getQuery(query, driver=driver, params={"lookup": lookup})
    if not rows:
        raise Exception("User not found")
    user_row = rows[0]
    if str(user_row.get("access", "")).lower() != "enabled":
        raise Exception("User is not verified")
    return user_row


def _verify_profile_credentials(userid, credentials):
    bearer_claims = verify_bearer_auth(required_userid=userid, req=request)
    if bearer_claims:
        return True
    if not credentials:
        raise Exception("Missing credentials")
    credential_userid = credentials.get("userid")
    credential_key = credentials.get("key")
    if not credential_userid or not credential_key:
        raise Exception("Missing credential fields")
    if str(credential_userid) != str(userid):
        raise Exception("Credentials do not match requested user")
    verified = verifyUser(str(credential_userid), credential_key)
    if verified == "verified":
        return True

    driver = getDriver("userdb")
    query = """
    MATCH (u:USER {userid: toString($userid)})
    RETURN
      coalesce(u.access, '') as access,
      coalesce(u.apiKeyHash, '') as apiKeyHash,
      coalesce(u.apiKeyHashes, []) as apiKeyHashes
    """
    rows = getQuery(query, driver=driver, params={"userid": str(userid)})
    if not rows:
        raise Exception("User is not verified")

    row = rows[0] or {}
    if str(row.get("access", "")).lower() != "enabled":
        raise Exception("User is not verified")

    key_hashes = []
    single_hash = row.get("apiKeyHash")
    if isinstance(single_hash, str) and single_hash.strip():
        key_hashes.append(single_hash.strip())
    hash_list = row.get("apiKeyHashes")
    if isinstance(hash_list, list):
        for item in hash_list:
            if isinstance(item, str) and item.strip():
                key_hashes.append(item.strip())

    for key_hash in key_hashes:
        if verifyPassword(key_hash, str(credential_key)):
            return True

    raise Exception("User is not verified")

@user_bp.route('/newuser', methods=['POST'])
def getnewuser():
    try:

        mail_default = get_default_sender()
        alert_recipients = get_alert_recipients()
        support_email = get_support_email() or "the configured support email"
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
                f"Account with this email already exists. Please contact {support_email} to reset password.")

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

        sendEmail(
            mail,
            subject="New registered user",
            recipients=alert_recipients,
            body=body,
            sender=mail_default,
        )

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
            support_email = get_support_email() or "the configured support email"
            return jsonify({"error": "please contact " + support_email + ". Error:" + error_message}), 500

@user_bp.route('/login', methods=['POST'])
def getLogin():
    try:
        data = request.get_data()
        data = json.loads(data)
        user = unlist(data.get('user'))
        password = unlist(data.get('password'))

        credentials = login(user, password)
        if isinstance(credentials, tuple):
            payload, status = credentials
            if isinstance(payload, dict):
                return jsonify(payload), int(status)
            return jsonify({"error": str(payload)}), int(status)
        if not isinstance(credentials, dict):
            return jsonify({"error": "verification failed"}), 500

        token = issue_auth_token(credentials.get("userid"), credentials.get("role"))
        response = {
            "userid": credentials.get("userid"),
            "username": credentials.get("username"),
            "role": credentials.get("role", "user"),
            "token": token,
        }
        return jsonify(response), 200

    except Exception as e:
        result = str(e)
        return jsonify({"error": result}), 500


@user_bp.route('/forgot-password/request', methods=['POST'])
def request_forgot_password():
    try:
        _cleanup_requests()
        data = _read_json_payload()
        user_identifier = unlist(data.get("user"))
        email_identifier = unlist(data.get("email"))
        lookup_identifier = email_identifier or user_identifier
        new_password = unlist(data.get("newPassword"))

        if not lookup_identifier:
            raise Exception("Missing username or email")
        if not new_password:
            raise Exception("New password is required.")
        if not _password_meets_policy(new_password):
            raise Exception("Password must be at least 6 characters.")

        try:
            existing = _load_user_by_identifier(lookup_identifier)
        except Exception:
            # Best practice: do not reveal whether an account exists.
            return jsonify({
                "message": "If an account exists for the provided username/email, a verification code has been sent."
            }), 200

        userid = str(existing.get("userid"))

        request_id = f"forgot_{uuid.uuid4().hex[:12]}"
        verification_code = f"{secrets.randbelow(900000) + 100000}"
        with REQUEST_LOCK:
            PASSWORD_CHANGE_REQUESTS[request_id] = {
                "userid": userid,
                "password_hash": password_hash(new_password),
                "verification_code": verification_code,
                "expires_at": datetime.utcnow() + timedelta(minutes=REQUEST_TTL_MINUTES),
            }

        target_email = existing.get("email")
        _send_verification_email(
            target_email,
            verification_code,
            "Password Reset",
            username=existing.get("username"),
        )
        logger.info(
            "Forgot-password verification email sent: userid=%s request_id=%s email=%s",
            userid,
            request_id,
            _mask_email(target_email),
        )

        response = {
            "requestId": request_id,
            "userId": userid,
            "maskedEmail": _mask_email(target_email),
            "message": "If an account exists for the provided username/email, a verification code has been sent.",
        }
        if _include_debug_verification_code():
            response["debugVerificationCode"] = verification_code

        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/forgot-password/confirm', methods=['POST'])
def confirm_forgot_password():
    try:
        _cleanup_requests()
        data = _read_json_payload()
        user_identifier = unlist(data.get("user"))
        email_identifier = unlist(data.get("email"))
        lookup_identifier = email_identifier or user_identifier
        request_id = unlist(data.get("requestId"))
        verification_code = str(unlist(data.get("verificationCode")) or "").strip()

        if not lookup_identifier or not request_id or not verification_code:
            raise Exception("Missing required confirmation fields")

        existing = _load_user_by_identifier(lookup_identifier)
        userid = str(existing.get("userid"))

        with REQUEST_LOCK:
            pending = PASSWORD_CHANGE_REQUESTS.get(request_id)
            if not pending or pending.get("userid") != userid:
                raise Exception("Password reset request not found. Please request a new verification email.")
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
        data = _read_json_payload()
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
        data = _read_json_payload()
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
        data = _read_json_payload()
        userid = unlist(data.get("userId"))
        current_password = unlist(data.get("currentPassword"))
        new_password = unlist(data.get("newPassword"))
        credentials = data.get("credentials")

        if not userid:
            raise Exception("Missing userId")
        if not current_password or not new_password:
            raise Exception("Current and new password are required.")
        if not _password_meets_policy(new_password):
            raise Exception("Password must be at least 6 characters.")

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
        data = _read_json_payload()
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


@user_bp.route('/profile/request-api-key', methods=['POST'])
def request_api_key_creation():
    try:
        _cleanup_requests()
        data = _read_json_payload()
        userid = unlist(data.get("userId"))
        credentials = data.get("credentials")

        if not userid:
            raise Exception("Missing userId")
        _verify_profile_credentials(userid, credentials)

        existing = _load_user(userid)
        target_email = existing.get("email")
        if not target_email:
            raise Exception("User email is missing; cannot create API key.")

        request_id = f"apikey_{uuid.uuid4().hex[:12]}"
        verification_code = f"{secrets.randbelow(900000) + 100000}"
        api_key = f"cmk_{secrets.token_urlsafe(32)}"
        api_key_hash = password_hash(api_key)
        if not isinstance(api_key_hash, str) or api_key_hash.startswith("password hash failed"):
            raise Exception("Unable to generate API key.")

        with REQUEST_LOCK:
            API_KEY_CREATE_REQUESTS[request_id] = {
                "userid": str(userid),
                "api_key": api_key,
                "api_key_hash": api_key_hash,
                "verification_code": verification_code,
                "expires_at": datetime.utcnow() + timedelta(minutes=REQUEST_TTL_MINUTES),
            }

        _send_verification_email(
            target_email,
            verification_code,
            "API Key Creation",
            username=existing.get("username"),
        )
        logger.info(
            "API key verification email sent: userid=%s request_id=%s email=%s",
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


@user_bp.route('/profile/confirm-api-key', methods=['POST'])
def confirm_api_key_creation():
    try:
        _cleanup_requests()
        data = _read_json_payload()
        userid = unlist(data.get("userId"))
        request_id = unlist(data.get("requestId"))
        verification_code = str(unlist(data.get("verificationCode")) or "").strip()
        credentials = data.get("credentials")

        if not userid or not request_id or not verification_code:
            raise Exception("Missing required confirmation fields")
        _verify_profile_credentials(userid, credentials)

        with REQUEST_LOCK:
            pending = API_KEY_CREATE_REQUESTS.get(request_id)
            if not pending or pending.get("userid") != str(userid):
                raise Exception("API key request not found. Please request a new verification email.")
            if pending.get("verification_code") != verification_code:
                raise Exception("Invalid verification code.")
            api_key = pending.get("api_key")
            api_key_hash = pending.get("api_key_hash")
            API_KEY_CREATE_REQUESTS.pop(request_id, None)

        updated_at = _now_iso()
        driver = getDriver("userdb")
        query = """
        MATCH (u:USER {userid: toString($userid)})
        SET
          u.apiKeyHash = $apiKeyHash,
          u.apiKeyCreatedAt = $updatedAt,
          u.updatedAt = $updatedAt
        RETURN u.apiKeyCreatedAt as apiKeyCreatedAt
        """
        saved = getQuery(
            query,
            driver=driver,
            params={
                "userid": userid,
                "apiKeyHash": api_key_hash,
                "updatedAt": updated_at,
            },
        )
        if not saved:
            raise Exception("User not found")

        return jsonify({
            "apiKey": api_key,
            "apiKeyCreatedAt": saved[0].get("apiKeyCreatedAt") or updated_at,
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/activity/<userid>', methods=['GET'])
def get_profile_activity(userid):
    try:
        credentials_raw = request.args.get("credentials")
        credentials = json.loads(credentials_raw) if credentials_raw else None
        database = request.args.get("database")
        if not database:
            raise Exception("Missing database")
        _verify_profile_credentials(userid, credentials)

        driver = getDriver(database)
        query = """
        MATCH (l:LOG)
        WHERE toString(l.user) = toString($userid)
        RETURN
          coalesce(toString(l.action), '') as action,
          coalesce(toString(l.description), '') as description
        """
        rows = getQuery(query, driver=driver, params={"userid": userid})

        counters = {
            "createdNodes": 0,
            "createdRelationships": 0,
            "updatedNodes": 0,
            "updatedRelationships": 0,
            "totalActions": 0,
        }
        for row in rows:
            action = (row.get("action") or "").lower()
            desc = (row.get("description") or "").lower()
            text = f"{action} {desc}"
            counters["totalActions"] += 1
            if "created node" in text:
                counters["createdNodes"] += 1
            elif "created relationship" in text:
                counters["createdRelationships"] += 1
            elif "changed" in text and "relationship" in text:
                counters["updatedRelationships"] += 1
            elif "changed" in text:
                counters["updatedNodes"] += 1

        return jsonify(counters), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/bookmarks/<userid>', methods=['GET'])
def get_profile_bookmarks(userid):
    try:
        credentials_raw = request.args.get("credentials")
        credentials = json.loads(credentials_raw) if credentials_raw else None
        _verify_profile_credentials(userid, credentials)
        rows = _get_user_entries(userid, "bookmarks")
        return jsonify({"bookmarks": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/bookmarks/add', methods=['POST'])
def add_profile_bookmark():
    try:
        data = _read_json_payload()
        userid = unlist(data.get("userId"))
        credentials = data.get("credentials")
        database = unlist(data.get("database"))
        cmid = str(unlist(data.get("cmid")) or "").strip()
        cmname = str(unlist(data.get("cmname")) or "").strip()

        if not userid or not database or not cmid:
            raise Exception("Missing required fields")
        _verify_profile_credentials(userid, credentials)

        if not cmname:
            cmname = _lookup_cmid_name(database, cmid)

        bookmarks = _get_user_entries(userid, "bookmarks")
        bookmarks = [
            row for row in bookmarks
            if not (str(row.get("cmid")) == cmid and str(row.get("database")) == database)
        ]
        bookmarks.insert(0, {
            "cmid": cmid,
            "cmname": cmname,
            "database": database,
            "cmidType": _get_cmid_type(cmid),
            "savedAt": _now_iso(),
        })
        _set_user_entries(userid, "bookmarks", bookmarks[:500])
        return jsonify({"status": "ok", "bookmarks": bookmarks[:500]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/bookmarks/remove', methods=['POST'])
def remove_profile_bookmark():
    try:
        data = _read_json_payload()
        userid = unlist(data.get("userId"))
        credentials = data.get("credentials")
        items = data.get("items") or []
        _verify_profile_credentials(userid, credentials)

        removals = {
            (str(item.get("cmid")), str(item.get("database")))
            for item in items
            if item.get("cmid") and item.get("database")
        }
        bookmarks = _get_user_entries(userid, "bookmarks")
        remaining = [
            row for row in bookmarks
            if (str(row.get("cmid")), str(row.get("database"))) not in removals
        ]
        _set_user_entries(userid, "bookmarks", remaining)
        return jsonify({"status": "ok", "bookmarks": remaining}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/history/<userid>', methods=['GET'])
def get_profile_history(userid):
    try:
        credentials_raw = request.args.get("credentials")
        credentials = json.loads(credentials_raw) if credentials_raw else None
        _verify_profile_credentials(userid, credentials)
        rows = _get_user_entries(userid, "history")
        return jsonify({"history": rows[:50]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@user_bp.route('/profile/history/add', methods=['POST'])
def add_profile_history():
    try:
        data = _read_json_payload()
        userid = unlist(data.get("userId"))
        credentials = data.get("credentials")
        database = unlist(data.get("database"))
        cmid = str(unlist(data.get("cmid")) or "").strip()
        cmname = str(unlist(data.get("cmname")) or "").strip()

        if not userid or not database or not cmid:
            raise Exception("Missing required fields")
        _verify_profile_credentials(userid, credentials)

        if not cmname:
            cmname = _lookup_cmid_name(database, cmid)

        history = _get_user_entries(userid, "history")
        history = [
            row for row in history
            if not (str(row.get("cmid")) == cmid and str(row.get("database")) == database)
        ]
        history.insert(0, {
            "cmid": cmid,
            "cmname": cmname,
            "database": database,
            "cmidType": _get_cmid_type(cmid),
            "accessedAt": _now_iso(),
        })
        history = history[:50]
        _set_user_entries(userid, "history", history)
        return jsonify({"status": "ok", "history": history}), 200
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

        claims = verify_request_auth(credentials=credentials, required_role="admin", req=request)
        approver = claims.get("userid")

        result = enableUser(database, process=process,
                            userid=userid, approver=approver)

        if isinstance(result, list) and process == "approve":

            users = [user for user in result if user.get("email")]
            alert_recipients = get_alert_recipients()
            default_sender = get_default_sender()
            support_email = get_support_email() or "the configured support email"
            if len(users) > 0:
                for user in users:
                    body = f"""
        Hello {user.get("first")} {user.get("last")},

        Your registration has been approved. You can now access the CatMapper applications. Please see catmapper.org/help or email {support_email} for any questions.

        Best,
        CatMapper Team
                    """
                    recipients = [user.get("email")] + alert_recipients
                    # Preserve order while removing duplicates/empties.
                    recipients = list(dict.fromkeys([r for r in recipients if r]))
                    sendEmail(
                        mail,
                        subject="CatMapper Registration Approved",
                        recipients=recipients,
                        body=body,
                        sender=default_sender,
                    )

        return result
    except Exception as e:
        result = str(e)
        return result, 500
