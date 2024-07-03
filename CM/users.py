''' users.py '''

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
        hashed_string = hashed_string.replace("\x00","")

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

def login(database,user,password):
    try:

        if database.lower() == "archamap":
            database = "ArchaMap"
        elif database.lower() == "sociomap":
            database = "SocioMap"
        else:
            raise Exception("database must be SocioMap or ArchaMap")

        driver = getDriver("userdb")

        query = """
        match (u:USER) 
        where toLower(u.username) = toLower($username) and $database in u.database 
        return u.username as username, u.userid as userid, u.password as password, u.access as access, u.role as role
"""
        result = getQuery(query,driver, params = {'username': user, 'database': database})

        if len(result) == 0:
            raise Exception("User not found")

        pwd = result[0].get("password")
        valid = verifyPassword(pwd,password)

        if valid:
            result = result[0]
            del result["password"]        
        else:
            raise Exception("invalid password")

        return result

    except Exception as e:
        return f"verification failed: {str(e)}"