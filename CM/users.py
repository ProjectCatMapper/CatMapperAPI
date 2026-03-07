''' users.py '''

from unittest import result
import pysodium
from .utils import *


def password_hash(password):
    try:
        # Set the time and memory costs for scrypt
        opslimit = pysodium.crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_INTERACTIVE
        memlimit = pysodium.crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_INTERACTIVE

        # Hash the password
        hashed_bytes = pysodium.crypto_pwhash_scryptsalsa208sha256_str(
            password.encode('utf-8'), opslimit=opslimit, memlimit=memlimit
        )

        # # Convert the bytes to hexadecimal and then decode to string
        hashed_string = hashed_bytes.decode("utf-8")
        hashed_string = hashed_string.replace("\x00", "")

        return hashed_string

    except Exception as e:
        return f"password hash failed: {str(e)}"

# Function to verify the password


def verifyPassword(stored_hash, password):
    try:
        if not stored_hash.endswith("\x00"):
            stored_hash = stored_hash + "\x00"
        result = pysodium.crypto_pwhash_scryptsalsa208sha256_str_verify(
            stored_hash.encode('utf-8'), password.encode('utf-8')
        )
        print("Verification succeeded")
        return True
    except Exception as e:
        print(f"Verification failed: {e}")
        return False


def login(user, password):
    try:

        driver = getDriver("userdb")

        query = """
        match (u:USER) 
        where toLower(u.username) = toLower($username) and u.access = "enabled"
        return u.username as username, u.userid as userid, u.password as key, u.access as access, u.role as role
"""
        result = getQuery(query, driver, params={
                          'username': user})

        if len(result) == 0:
            return {"error": "User not found"}, 401

        pwd = result[0].get("key")
        valid = verifyPassword(pwd, password)

        if valid:
            result = result[0]
        else:
            return {"error": "invalid password"}, 401

        return result

    except Exception as e:
        return {"error": f"verification failed: {str(e)}"}, 500


def verifyUser(user, pwd, role=None):
    try:
        driver = getDriver("userdb")
        print(user)
        if role == "admin":
            query = "match (u:USER {userid: toString($user),password: $pwd, access: 'enabled', role: 'admin'}) return 'verified' as verified"
        else:
            query = "match (u:USER {userid: toString($user),password: $pwd, access: 'enabled'}) return 'verified' as verified"
        result = getQuery(query, driver, params={
                          'user': user, 'pwd': pwd}, type='list')
        print(result)
        return result[0]
    except Exception as e:
        return f"Error verifying user: {e}", 500


def enableUser(database, process, userid, approver):

    driver = getDriver('userdb')

    if process == "approve":
        if userid is None:
            raise Exception("Error: userid must be specified")

        userid = flattenList(userid)

        query = f"""
unwind $userids as id
with toString(id) as id
match (u {{userid: id}})
set u.access = 'enabled', u.log = u.log + [toString(datetime()) + ": access approved by {approver}"]
return u.userid as userid, u.first as first, u.last as last, u.email as email, u.database as database, u.intendedUse as intendedUse, u.access as access
"""
        result = getQuery(query, driver, params={"userids": userid})

    else:
        query = """
match (u {{access: 'pending'}})
WHERE any(db in coalesce(u.database, []) WHERE toLower(toString(db)) = toLower($database))
return u.userid as userid, u.first as first, u.last as last, u.email as email, u.database as database, u.intendedUse as intendedUse, u.access as access
"""
        result = getQuery(query, driver, params={"database": database})

    return result

def changePassword(credentials, newPassword):

    try:
        driver = getDriver('userdb')

        userid = credentials.get("userid")
    
        hashedPassword = password_hash(newPassword)

        query = """
        MATCH (u:USER {userid: toString($userid)})
        SET u.password = $hashedPassword
        RETURN u.userid as userid, u.first as first, u.last as last, u.email as email, u.database as database, u.intendedUse as intendedUse, u.access as access
        """
        
        result = getQuery(query, driver, params={'userid': userid, 'hashedPassword': hashedPassword})
        
        return result
    except Exception as e:
        return f"Error changing password: {e}", 500
