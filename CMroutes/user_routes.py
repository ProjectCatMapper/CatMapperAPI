from flask import Blueprint, request, jsonify
from CM import getDriver, password_hash, sendEmail, verifyUser, login, enableUser, unlist, getQuery
from flask_mail import Mail
import json

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


def _read_json_payload():
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data
    raw = request.get_data()
    if not raw:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _extract_entry(data):
    for key in ["entry", "item", "path", "url", "location", "target", "bookmark", "history"]:
        value = data.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()

    payload = data.get("payload")
    if isinstance(payload, dict):
        for key in ["entry", "item", "path", "url", "location", "target", "bookmark", "history"]:
            value = payload.get(key)
            if value is not None and str(value).strip() != "":
                return str(value).strip()

    for key in ["entry", "item", "path", "url", "location", "target"]:
        value = request.args.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()

    return None


def _extract_credentials(data):
    credentials = data.get("credentials")
    if not isinstance(credentials, dict):
        credentials = {}

    userid = (
        credentials.get("userid")
        or data.get("userid")
        or data.get("userId")
        or data.get("user")
        or request.args.get("userid")
        or request.args.get("userId")
        or request.headers.get("X-Userid")
    )
    key = (
        credentials.get("key")
        or data.get("key")
        or data.get("token")
        or request.args.get("key")
        or request.args.get("token")
        or request.headers.get("X-Key")
        or request.headers.get("Authorization")
    )

    if isinstance(key, str) and key.startswith("Bearer "):
        key = key.replace("Bearer ", "", 1).strip()

    if userid is None or key is None:
        return None, None
    return str(userid), str(key)


def _append_profile_item(property_name):
    if request.method == "OPTIONS":
        return ("", 204)

    data = _read_json_payload()
    userid, key = _extract_credentials(data)
    if not userid or not key:
        return jsonify({"error": "Missing credentials.userid and credentials.key"}), 400

    entry = _extract_entry(data)
    if not entry:
        return jsonify({"error": "Missing profile entry (entry/item/path/url)"}), 400

    verified = verifyUser(userid, key)
    if not (isinstance(verified, dict) and verified.get("verified") == "verified"):
        return jsonify({"error": "Invalid credentials"}), 401

    driver = getDriver("userdb")
    query = f"""
    MATCH (u:USER {{userid: toString($userid), password: $key, access: 'enabled'}})
    WITH u, coalesce(u.{property_name}, []) AS currentItems
    WITH u, [x IN currentItems WHERE x IS NOT NULL AND toString(x) <> $entry] AS deduped
    SET u.{property_name} = [$entry] + deduped
    RETURN u.userid AS userid, u.{property_name} AS items
    """
    result = getQuery(query, driver=driver, params={"userid": userid, "key": key, "entry": entry})
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
