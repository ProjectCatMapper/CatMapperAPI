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

