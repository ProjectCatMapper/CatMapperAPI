''' users.py '''

# functions related to users

import pysodium

def password_hash(password):
    try:
        # Set the time and memory costs for scrypt
        opslimit = pysodium.crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_INTERACTIVE
        memlimit = pysodium.crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_INTERACTIVE

        # Hash the password
        hashed_bytes = pysodium.crypto_pwhash_scryptsalsa208sha256_str(
            password.encode('utf-8'), opslimit=opslimit, memlimit=memlimit
        )

        # Convert the bytes to hexadecimal and then decode to string
        hashed_string = hashed_bytes.decode()
        hashed_string = hashed_string.replace("\x00","")

        # Ensure the string is exactly 101 characters long
        # if len(hashed_string) != 101:
        #     raise ValueError("Hashed string length is not 101 characters")

        return hashed_string
    except Exception as e:
        raise RuntimeError("pwhash failed") from e