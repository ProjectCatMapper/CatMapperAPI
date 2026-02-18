import os
from flask import request
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from CM import getDriver, getQuery, verifyUser, verifyPassword


AUTH_TOKEN_TTL_SECONDS = int(os.getenv("CATMAPPER_AUTH_TOKEN_TTL_SECONDS", "2592000"))
AUTH_TOKEN_SALT = "catmapper-auth-v1"


def _auth_secret():
    # Keep deployments deterministic even if Flask SECRET_KEY is not configured.
    return (
        os.getenv("CATMAPPER_AUTH_SECRET")
        or os.getenv("SECRET_KEY")
        or "catmapper-dev-auth-secret-change-me"
    )


def _serializer():
    return URLSafeTimedSerializer(_auth_secret(), salt=AUTH_TOKEN_SALT)


def issue_auth_token(userid, role):
    payload = {"userid": str(userid), "role": str(role or "user")}
    return _serializer().dumps(payload)


def parse_bearer_token(req=None):
    req = req or request
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        return None
    try:
        payload = _serializer().loads(token, max_age=AUTH_TOKEN_TTL_SECONDS)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _is_active_user(userid, required_role=None):
    driver = getDriver("userdb")
    query = """
    MATCH (u:USER {userid: toString($userid)})
    RETURN coalesce(u.access, '') as access, coalesce(u.role, '') as role
    """
    rows = getQuery(query, driver=driver, params={"userid": str(userid)})
    if not rows:
        return False
    row = rows[0] or {}
    access = str(row.get("access", "")).lower()
    role = str(row.get("role", "")).lower()
    if access != "enabled":
        return False
    if required_role and role != str(required_role).lower():
        return False
    return True


def _verify_api_key_credentials(userid, credential_key, required_role=None):
    driver = getDriver("userdb")
    query = """
    MATCH (u:USER {userid: toString($userid)})
    RETURN
      coalesce(u.access, '') as access,
      coalesce(u.role, '') as role,
      coalesce(u.apiKeyHash, '') as apiKeyHash,
      coalesce(u.apiKeyHashes, []) as apiKeyHashes
    """
    rows = getQuery(query, driver=driver, params={"userid": str(userid)})
    if not rows:
        return None

    row = rows[0] or {}
    access = str(row.get("access", "")).lower()
    role = str(row.get("role", "")).lower()
    if access != "enabled":
        return None
    if required_role and role != str(required_role).lower():
        return None

    hashes = []
    single_hash = row.get("apiKeyHash")
    if isinstance(single_hash, str) and single_hash.strip():
        hashes.append(single_hash.strip())
    hash_list = row.get("apiKeyHashes")
    if isinstance(hash_list, list):
        for item in hash_list:
            if isinstance(item, str) and item.strip():
                hashes.append(item.strip())

    for stored_hash in hashes:
        if verifyPassword(stored_hash, str(credential_key)):
            return {"userid": str(userid), "role": role or "user"}

    return None


def verify_request_auth(required_userid=None, credentials=None, required_role=None, req=None):
    # Preferred path: signed bearer token in Authorization header.
    claims = parse_bearer_token(req=req)
    if claims:
        token_userid = str(claims.get("userid", ""))
        token_role = str(claims.get("role", "")).lower()
        if required_userid is not None and token_userid != str(required_userid):
            raise Exception("Credentials do not match requested user")
        if required_role and token_role != str(required_role).lower():
            raise Exception("User is not authorized")
        if not _is_active_user(token_userid, required_role=required_role):
            raise Exception("User is not verified")
        return {"userid": token_userid, "role": token_role}

    # Backward-compatible fallback: credential object in payload/query.
    if not credentials:
        raise Exception("Missing credentials")

    credential_userid = credentials.get("userid")
    credential_key = credentials.get("key")
    if not credential_userid or not credential_key:
        raise Exception("Missing credential fields")
    if required_userid is not None and str(credential_userid) != str(required_userid):
        raise Exception("Credentials do not match requested user")
    verified = verifyUser(str(credential_userid), credential_key, required_role)
    if verified == "verified":
        return {
            "userid": str(credential_userid),
            "role": str(required_role or credentials.get("role", "user")).lower(),
        }

    api_key_claims = _verify_api_key_credentials(
        userid=str(credential_userid),
        credential_key=credential_key,
        required_role=required_role,
    )
    if api_key_claims:
        return api_key_claims

    raise Exception("User is not verified")


def verify_bearer_auth(required_userid=None, required_role=None, req=None):
    claims = parse_bearer_token(req=req)
    if not claims:
        return None
    token_userid = str(claims.get("userid", ""))
    token_role = str(claims.get("role", "")).lower()
    if required_userid is not None and token_userid != str(required_userid):
        raise Exception("Credentials do not match requested user")
    if required_role and token_role != str(required_role).lower():
        raise Exception("User is not authorized")
    if not _is_active_user(token_userid, required_role=required_role):
        raise Exception("User is not verified")
    return {"userid": token_userid, "role": token_role}
