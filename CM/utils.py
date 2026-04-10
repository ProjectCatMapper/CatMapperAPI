''' utils.py '''

# general utility functions

import itertools
import re
from neo4j import GraphDatabase
import pandas as pd
from collections.abc import Iterable
import json
from configparser import ConfigParser
import threading
from datetime import datetime, timedelta

_driver_cache = {}
_driver_lock = threading.Lock()
_last_verified = {}
_QUERY_CANCEL_CHECKER = threading.local()
_CYPHER_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CYPHER_ELEMENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9:_-]+$")
VALID_VARIABLE_CATEGORY_TYPE_VALUES = {
    "ORDINAL",
    "CONTINUOUS",
    "CATEGORICAL",
    "TEXT",
}

config = ConfigParser()
config.read('config.ini')


class QueryCancelledError(Exception):
    """Raised when an operation should stop due to user cancellation."""


def validate_variable_category_type_value(value, *, allow_blank=False):
    """
    Validate and normalize categoryType values for VARIABLE USES ties.

    Returns the canonical uppercase value when valid.
    """
    if value is None or pd.isna(value):
        if allow_blank:
            return value
        raise ValueError(
            "categoryType for VARIABLE must be a single term: "
            "ORDINAL, CONTINUOUS, CATEGORICAL, or TEXT."
        )

    normalized = str(value).strip()
    if not normalized:
        if allow_blank:
            return normalized
        raise ValueError(
            "categoryType for VARIABLE must be a single term: "
            "ORDINAL, CONTINUOUS, CATEGORICAL, or TEXT."
        )

    normalized = normalized.upper()
    if normalized not in VALID_VARIABLE_CATEGORY_TYPE_VALUES:
        allowed = ", ".join(sorted(VALID_VARIABLE_CATEGORY_TYPE_VALUES))
        raise ValueError(
            f"Invalid categoryType '{value}'. Expected one of: {allowed}."
        )
    return normalized


def set_query_cancel_checker(checker):
    _QUERY_CANCEL_CHECKER.callback = checker


def clear_query_cancel_checker():
    if hasattr(_QUERY_CANCEL_CHECKER, "callback"):
        delattr(_QUERY_CANCEL_CHECKER, "callback")


def check_query_cancellation():
    checker = getattr(_QUERY_CANCEL_CHECKER, "callback", None)
    if checker:
        checker()
    
def getDriver(database):
    """
    Get or create a cached Neo4j driver with health checking.
    Automatically handles defunct connections.
    """
    database = database.lower()
    
    with _driver_lock:
        now = datetime.now()
        
        # 1. Check if driver exists in cache
        if database in _driver_cache:
            last_check = _last_verified.get(database, datetime.min)
            
            # Only verify if it's been more than 2 minutes
            if now - last_check < timedelta(minutes=2):
                return _driver_cache[database]
            
            # Time to verify connection
            driver = _driver_cache[database]
            try:
                driver.verify_connectivity()
                _last_verified[database] = now
                return driver
            except Exception as e:
                print(f"Driver for {database} is defunct, removing from cache: {e}")
                
                # Cleanup old driver
                try:
                    driver.close()
                except:
                    pass # Ignore errors during close
                
                # Remove from cache and fall through to creation
                _driver_cache.pop(database, None)
                _last_verified.pop(database, None)
        
        # 2. Create new driver 
        # (Reaches here if driver was not in cache OR if it was just removed above)
        try:
            driver = _create_driver(database)
            _driver_cache[database] = driver
            _last_verified[database] = now
            return driver
        except Exception as e:
            print(f"Failed to create driver for {database}: {e}")
            raise


def _create_driver(database):
    """Create a new Neo4j driver with optimal settings."""

    
    # Validate database
    valid_databases = ['sociomap', 'archamap', 'gisdb', 'userdb']
    if database not in valid_databases:
        raise ValueError(f"Invalid database: {database}")
    
     # Determine config section
    config_opt = 'DB'
    try:
        if not testConnection():
            config_opt = 'OFFLINE'
            # warnings.warn("Using OFFLINE config due to failed connection test", RuntimeWarning)
    except:
        config_opt = 'DB'  # Fallback to DB config
    if database not in config[config_opt]:
        raise ValueError(f"Database '{database}' not found in config")
    
    user = config[config_opt]['user']
    pwd = config[config_opt]['pwd']
    uri = config[config_opt][database]
    
    # Create driver with optimal connection settings
    driver = GraphDatabase.driver(
        uri,
        auth=(user, pwd),
        max_connection_lifetime=3600,
        max_connection_pool_size=50,
        connection_acquisition_timeout=60,
        keep_alive=True,
        connection_timeout=30,
        max_transaction_retry_time=30,        # Correct v5 name
        resolver=None,
        encrypted=False                       # Removed 'trust'
    )
    
    # warnings.warn(f"Created new driver for {database} at {uri} with user {user} and pwd length {len(pwd)}")
    
    # Verify connection works
    try:
        driver.verify_connectivity()
    except Exception as e:
        driver.close()
        raise ConnectionError(f"Driver created but cannot connect to {database}: {e}")
    
    return driver


def closeAllDrivers():
    """Close all cached drivers. Call on application shutdown."""
    with _driver_lock:
        for database, driver in list(_driver_cache.items()):
            try:
                driver.close()
                print(f"Closed driver for {database}")
            except Exception as e:
                print(f"Error closing {database}: {e}")
        _driver_cache.clear()
        _last_verified.clear()


def getCacheStats():
    """Get driver cache statistics for monitoring."""
    with _driver_lock:
        return {
            'cached_databases': list(_driver_cache.keys()),
            'count': len(_driver_cache),
            'last_verified': {
                db: timestamp.isoformat() 
                for db, timestamp in _last_verified.items()
            }
        }

# CM/utils.py

def getQuery(query, driver, params=None, type="dict", max_retries=3, **kwargs):
    """
    Execute a Neo4j query with automatic retry on connection failures.
    
    Args:
        query: Cypher query string
        driver: Neo4j driver instance
        params: Query parameters (dict)
        type: Return type - "records", "dict", "list"
        max_retries: Maximum retry attempts
        **kwargs
    Returns:
        Consumed query results
    """
    params = params.copy() if params else {}
    params.update(kwargs)
    
    for attempt in range(max_retries):
        try:
            check_query_cancellation()
            with driver.session() as session:
                result = session.run(query, params)
                
                # Consume results BEFORE session closes
                
                if type == "dict":
                    # For queries returning key-value pairs
                    result = [dict(record) for record in result]
                    check_query_cancellation()
                    return result
                
                elif type == "df" or type == "dataframe":
                    result = [dict(record) for record in result]
                    df = pd.DataFrame(result)
                    check_query_cancellation()
                    return df
                elif type == "list":
                    # For queries returning single column
                    result = list(itertools.chain.from_iterable(
                    record.values() for record in result))
                    check_query_cancellation()
                    return result
                
                elif type == "records":
                    # For queries returning multiple columns (default)
                    data = []
                    for record in result:
                        data.append(dict(record))
                    check_query_cancellation()
                    return data
                
                else:
                    raise ValueError(f"Invalid type parameter: '{type}'. Must be 'records', 'dict', or 'list'")
                    
        except Exception as e:
            if isinstance(e, QueryCancelledError):
                raise
            error_msg = str(e).lower()
            
            # Check for connection/session errors
            is_connection_error = any(keyword in error_msg for keyword in [
                'defunct', 'connection', 'session', 'expired', 'closed',
                'failed to read', 'unable to retrieve routing'
            ])
            
            if is_connection_error and attempt < max_retries - 1:
                print(f"Connection error on attempt {attempt + 1}/{max_retries}, retrying: {e}")
                
                # Try to verify driver connectivity
                try:
                    driver.verify_connectivity()
                except Exception as verify_error:
                    print(f"Driver verification failed: {verify_error}")
                    # Driver will be recreated on next getDriver() call
                
                continue  # Retry
            
            elif is_connection_error:
                # All retries exhausted
                raise RuntimeError(f"Query failed after {max_retries} attempts due to connection issues: {e}")
            
            else:
                # Not a connection error, raise immediately
                raise RuntimeError(f"Query execution error: {e}")
    
    # Should not reach here, but just in case
    raise RuntimeError(f"Query failed after {max_retries} attempts")


def sanitize_cypher_identifier(value, field_name="identifier"):
    """
    Allow only simple Cypher identifiers (labels, relationship types, properties).
    Prevents Cypher injection when interpolation is unavoidable for identifiers.
    """
    if value is None:
        raise ValueError(f"{field_name} is required")
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError(f"{field_name} cannot be empty")
    if not _CYPHER_IDENTIFIER_PATTERN.fullmatch(cleaned):
        raise ValueError(f"Invalid {field_name}: {value}")
    return cleaned


def sanitize_cypher_element_id(value, field_name="elementId"):
    """
    Validate Neo4j elementId tokens before passing to queries.
    """
    if value is None:
        raise ValueError(f"{field_name} is required")
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError(f"{field_name} cannot be empty")
    if not _CYPHER_ELEMENT_ID_PATTERN.fullmatch(cleaned):
        raise ValueError(f"Invalid {field_name}: {value}")
    return cleaned


def get_valid_domain_labels(driver):
    """
    Return known node-label domains valid for API domain filtering.
    Includes metadata labels plus built-in structural labels used in the app.
    """
    base_labels = {
        "CATEGORY",
        "DATASET",
        "DISTRICT",
        "ADM0",
        "METADATA",
        "LABEL",
        "PROPERTY",
        "VARIABLE",
        "STACK",
        "DELETED",
        "VECTOR",
    }
    rows = getQuery(
        "MATCH (l:LABEL) RETURN DISTINCT l.CMName AS label",
        driver=driver,
        type="list",
    )
    dynamic_labels = {label for label in rows if isinstance(label, str) and label.strip()}
    return base_labels | dynamic_labels


def validate_domain_label(domain, driver=None, aliases=None, extra_allowed=None):
    """
    Normalize and validate a domain label for safe use in Cypher label slots.
    """
    aliases = aliases or {}
    extra_allowed = set(extra_allowed or [])
    normalized = aliases.get(domain, domain)
    normalized = sanitize_cypher_identifier(normalized, "domain")

    if driver is None:
        return normalized

    allowed = get_valid_domain_labels(driver) | extra_allowed
    if normalized not in allowed:
        raise ValueError(f"Unknown domain label: {normalized}")
    return normalized

def unlist(l):
    if isinstance(l, list):
        l = l[0]
    return l

# it returns true if CMID exists else returns empty list
def isValidCMID(cmid, driver):

    query = "unwind $cmid as cmid match (c:CATEGORY|DATASET {CMID: cmid}) return c.CMID as cmid, true as exists"

    result = getQuery(query, driver, params={"cmid": cmid}, type="dict")
    
    return result

def cleanCMID(cmid):
    # Define the regex pattern for valid prefixes
    valid_prefix_pattern = re.compile(r'^(AD|SD|AM|SM)')

    # Function to check if a string has a valid prefix
    def has_valid_prefix(s):
        return bool(valid_prefix_pattern.match(s))

    if isinstance(cmid, list):
        # If cmid is a list, filter out items that don't have a valid prefix
        cleaned_cmid = [item for item in cmid if has_valid_prefix(item)]
        # If the cleaned list is empty, return None
        return cleaned_cmid if cleaned_cmid else None
    elif isinstance(cmid, str):
        # If cmid is a string, check if it has a valid prefix
        return cmid if has_valid_prefix(cmid) else None
    else:
        # If cmid is neither a list nor a string, return None
        return None


def getAvailableID(new_id="CMID", label="CATEGORY", n=1, database="SocioMap"):
    if database.lower() == "sociomap":
        database = "SocioMap"
    elif database.lower() == "archamap":
        database = "ArchaMap"
    elif database.lower() == "gisdb":
        database = "gisdb"
    elif database.lower() == "userdb":
        database = "userdb"
    else:
        raise ValueError(
            f"Database must be 'SocioMap', 'ArchaMap', 'gisdb', or 'userdb', but database is {database}"
        )

    if not isinstance(n, int) or n < 1:
        raise ValueError("`n` must be a positive integer.")

    # Ensure label is either "DATASET", "USER", or "CATEGORY"
    if label not in ["DATASET", "USER"]:
        label = "CATEGORY"

    driver = getDriver(database)

    prefix_by_target = {
        ("SocioMap", "DATASET"): "SD",
        ("SocioMap", "CATEGORY"): "SM",
        ("ArchaMap", "DATASET"): "AD",
        ("ArchaMap", "CATEGORY"): "AM",
    }

    if database == "gisdb":
        prefix = "gis"
        label_filter = ""
    elif database == "userdb":
        # Keep legacy behavior for userdb by using all nodes for index scanning.
        # CMIDs are not expected to be generated here in normal workflows.
        prefix = "M" if label != "DATASET" else "D"
        label_filter = ""
    else:
        prefix = prefix_by_target[(database, "DATASET" if label == "DATASET" else "CATEGORY")]
        if label == "CATEGORY":
            # CATEGORY IDs must not collide with existing CATEGORY or DELETED CMIDs.
            label_filter = "WHERE (n:CATEGORY OR n:DELETED)"
        elif label == "DATASET":
            # DATASET IDs also back MERGING/STACK nodes and must stay reserved
            # after deletion, because deleteNode() preserves the old CMID on a
            # standalone DELETED node.
            label_filter = "WHERE (n:DATASET OR n:DELETED)"
        else:
            label_filter = ""

    query = f"""
    MATCH (n)
    {label_filter}
    WITH n
    WHERE n.{new_id} IS NOT NULL
      AND toString(n.{new_id}) =~ $pattern
    RETURN toInteger(substring(toString(n.{new_id}), size($prefix))) AS used_id
    """

    used_ids_raw = getQuery(
        query,
        driver,
        type="list",
        params={
            "pattern": f"^{prefix}[0-9]+$",
            "prefix": prefix,
        },
    )
    used_ids = {
        int(value)
        for value in (used_ids_raw or [])
        if isinstance(value, (int, float)) and int(value) > 0
    }

    # Fill any missing sequential IDs first, then continue after the highest.
    next_numeric_ids = []
    candidate = 1
    while len(next_numeric_ids) < n:
        if candidate not in used_ids:
            next_numeric_ids.append(candidate)
            used_ids.add(candidate)
        candidate += 1

    if database == "gisdb":
        return [f"{prefix}{x}" for x in next_numeric_ids]

    return [f"{prefix}{x}" for x in next_numeric_ids]


def flattenList(input_data):
    if isinstance(input_data, str):
        return [input_data]
    elif isinstance(input_data, dict):
        input_data = input_data.keys()

    if isinstance(input_data, Iterable):
        flat_list = []
        for item in input_data:
            flat_list.extend(flattenList(item))
        return flat_list
    else:
        return [input_data]


def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False


def flatten_json(json_obj, parent_key='', sep='_'):
    flat_dict = {}
    for key, value in json_obj.items():
        new_key = key if parent_key else key
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        else:
            flat_dict[new_key] = value
    return flat_dict


def custom_sort(elem):
    if elem == 'CONTAINS':
        return 0
    elif elem == 'DISTRICT_OF':
        return 1
    elif elem == 'USES':
        return 2
    else:
        return 3

def testConnection(configOpt="DB",database="SocioMap"):
    """
    Test the connection to the specified database.
    """
    driver = None
    try:
        user = config['DB']['user']
        pwd = config['DB']['pwd']
        database = str.lower(database)
        uri = config['DB'][database]  # Default to SocioMap URI
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
        with driver.session() as session:
            result = session.run("RETURN 1")
            success = result.single()[0] == 1
        return success
    except Exception:
        return False
    finally:
        if driver is not None:
            try:
                driver.close()
            except Exception:
                pass
    

# Function to serialize a Neo4j Node object into a serializable dictionary
def serialize_node(node):
    return {
        "id": node.element_id,
        "labels": list(node.labels),
        "properties": dict(node)
    }
# Function to serialize Neo4j Relationship object into a serializable dictionary


def serialize_relationship(relationship):
    return {
        "type": relationship.type,
        "start_node_id": relationship.start_node.element_id,
        "end_node_id": relationship.end_node.element_id,
        "properties": dict(relationship.items())
    }
