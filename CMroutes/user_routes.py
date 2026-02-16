from flask import Blueprint, request, jsonify
from CM import getDriver, password_hash, sendEmail, verifyUser, login, enableUser, unlist, getQuery
from flask_mail import Mail
import json
from datetime import datetime, timezone

user_bp = Blueprint('user', __name__)

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

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
                mail = Mail()

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


def _is_present(value):
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def _parse_json(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _merge_values(target, source):
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        if _is_present(value):
            target[key] = value


def _get_value(data, *keys):
    normalized = {str(k).lower(): v for k, v in data.items()}
    for key in keys:
        value = data.get(key)
        if _is_present(value):
            return value
        value = normalized.get(str(key).lower())
        if _is_present(value):
            return value
    return None


def _read_json_payload():
    payload = {}

    # Lowest precedence first.
    _merge_values(payload, request.args.to_dict(flat=True))
    _merge_values(payload, request.form.to_dict(flat=True))

    if request.is_json:
        json_data = request.get_json(silent=True)
        if isinstance(json_data, dict):
            _merge_values(payload, json_data)
        elif isinstance(json_data, list) and json_data and isinstance(json_data[0], dict):
            _merge_values(payload, json_data[0])

    raw = request.get_data()
    raw_dict = _parse_json(raw)
    if isinstance(raw_dict, dict):
        _merge_values(payload, raw_dict)

    # Common wrappers used by some clients.
    for wrapper_key in ["payload", "data", "body", "request"]:
        wrapped = payload.get(wrapper_key)
        if isinstance(wrapped, dict):
            _merge_values(payload, wrapped)
        else:
            wrapped_dict = _parse_json(wrapped)
            if isinstance(wrapped_dict, dict):
                _merge_values(payload, wrapped_dict)

    return payload


def _extract_entry(data, property_name):
    direct_entry = _get_value(data, "entry", "item", "path", "url", "location", "target", "bookmark", "history")
    if isinstance(direct_entry, dict):
        return json.dumps(direct_entry, separators=(",", ":"), sort_keys=True), None
    if isinstance(direct_entry, list):
        return json.dumps(direct_entry, separators=(",", ":"), sort_keys=True), None
    if _is_present(direct_entry):
        return str(direct_entry).strip(), None

    cmid = _get_value(data, "cmid", "CMID")
    cmname = _get_value(data, "cmname", "CMName")
    database = _get_value(data, "database")
    if _is_present(cmid):
        cmid = str(cmid).strip()
        cmid_type = "DATASET" if len(cmid) > 1 and cmid[1] == "D" else "CATEGORY" if len(cmid) > 1 and cmid[1] == "M" else None
        ts_key = "savedAt" if property_name == "bookmarks" else "accessedAt"
        entry_obj = {
            "database": str(database).lower().strip() if _is_present(database) else None,
            "cmid": cmid,
            "cmname": str(cmname).strip() if _is_present(cmname) else None,
            "cmidType": cmid_type,
            ts_key: datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        }
        # Drop null keys for cleaner entries.
        entry_obj = {k: v for k, v in entry_obj.items() if v is not None}
        return json.dumps(entry_obj, separators=(",", ":"), sort_keys=True), cmid

    return None, None


def _extract_credentials(data):
    merged = dict(data)
    credentials = data.get("credentials")
    if isinstance(credentials, dict):
        for key, value in credentials.items():
            if key not in merged and _is_present(value):
                merged[key] = value

    userid = _get_value(merged, "userid", "userId", "user", "id")
    key = _get_value(merged, "key", "token", "authorization")

    header_userid = request.headers.get("X-Userid")
    if not _is_present(userid) and _is_present(header_userid):
        userid = header_userid

    if not _is_present(key):
        key = request.headers.get("X-Key") or request.headers.get("Authorization")

    if isinstance(key, str) and key.startswith("Bearer "):
        key = key.replace("Bearer ", "", 1).strip()

    if not _is_present(userid):
        return None, None
    return str(userid), str(key) if _is_present(key) else None


def _append_profile_item(property_name):
    if request.method == "OPTIONS":
        return ("", 204)

    data = _read_json_payload()
    userid, key = _extract_credentials(data)
    if not userid:
        return jsonify({"error": "Missing userId/userid"}), 400

    entry, cmid = _extract_entry(data, property_name)
    if not entry:
        return jsonify({"error": "Missing profile entry (entry/item/path/url/cmid)"}), 400

    driver = getDriver("userdb")
    if key:
        verified = verifyUser(userid, key)
        is_verified = False
        if isinstance(verified, dict):
            is_verified = verified.get("verified") == "verified"
        elif isinstance(verified, str):
            is_verified = verified == "verified"
        elif isinstance(verified, list):
            is_verified = "verified" in verified

        if not is_verified:
            return jsonify({"error": "Invalid credentials"}), 401
        match_clause = "MATCH (u:USER {userid: toString($userid), password: $key, access: 'enabled'})"
    else:
        # Legacy client support: allow authenticated app flows that only send userId.
        match_clause = "MATCH (u:USER {userid: toString($userid), access: 'enabled'})"

    query = f"""
    {match_clause}
    WITH u, coalesce(u.{property_name}, []) AS currentItems
    WITH u, [x IN currentItems
             WHERE x IS NOT NULL AND (
               CASE
                 WHEN $cmid IS NOT NULL THEN NOT toString(x) CONTAINS '\\"cmid\\":\\"' + $cmid + '\\"'
                 ELSE toString(x) <> $entry
               END
             )] AS deduped
    SET u.{property_name} = [$entry] + deduped
    RETURN u.userid AS userid, u.{property_name} AS items
    """
    result = getQuery(query, driver=driver, params={"userid": userid, "key": key, "entry": entry, "cmid": cmid})
    if not result:
        return jsonify({"error": "User not found or unauthorized"}), 401

    return jsonify({
        "message": "Success",
        "userid": result[0].get("userid"),
        property_name: result[0].get("items", []),
    }), 200


@user_bp.route('/profile/history/add', methods=['POST', 'OPTIONS'])
def add_profile_history():
    try:
        return _append_profile_item("history")
    except Exception as e:
        return jsonify({"error": f"Failed to add history: {str(e)}"}), 500


@user_bp.route('/profile/bookmarks/add', methods=['POST', 'OPTIONS'])
def add_profile_bookmark():
    try:
        return _append_profile_item("bookmarks")
    except Exception as e:
        return jsonify({"error": f"Failed to add bookmark: {str(e)}"}), 500
