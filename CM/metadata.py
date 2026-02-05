from functools import lru_cache
from .utils import getDriver, getQuery

@lru_cache(maxsize=128)
def get_metadata_groups(database):
    """
    Retrieve metadata groups with caching.
    
    Args:
        database: Database identifier for driver
        
    Returns:
        tuple: Tuple of dicts (immutable for caching)
    """
    driver = getDriver(database)
    query = '''
    MATCH (m:METADATA)
    WHERE m.displayOrder IS NOT NULL
      AND NOT m.CMName IN ['ALL NODES']
    WITH m.groupLabel AS group, m.CMName AS node, m.displayOrder AS nodeOrder
    MATCH (g:METADATA {CMName: group})
    WHERE g.displayOrder IS NOT NULL
    WITH g.groupLabel AS group, g.displayOrder AS groupOrder, node, nodeOrder
    ORDER BY group, nodeOrder, node 
    WITH group, groupOrder, collect(node) AS nodes
    RETURN group, nodes
    ORDER BY groupOrder
    '''
    
    result = getQuery(query, driver)
    
    # Return tuple for immutability (required for caching)
    return tuple(
        {"group": record["group"], "nodes": record["nodes"]}
        for record in result
    )
    
from functools import lru_cache

@lru_cache(maxsize=128)
def get_public_subdomains(database):
    """
    Retrieve public domain labels and their subdomains ordered by display order.
    
    Args:
        database: Database identifier for driver (used as cache key)
        
    Returns:
        dict: Dictionary mapping domains to their subdomains
              Example: {"Domain1": ["Domain1", "Subdomain1", "Subdomain2"], ...}
    """
    driver = getDriver(database)
    
    query = """
    MATCH (n:LABEL) 
    WHERE n.public = 'TRUE' AND NOT n.CMName = 'CATEGORY' 
    WITH DISTINCT n.groupLabel AS domain, n.CMName AS label, n.displayOrder AS displayOrder 
    ORDER BY domain, displayOrder 
    WITH domain, collect(label) AS subdomains 
    MATCH (d:LABEL {CMName: domain}) 
    WITH domain, subdomains, d.displayOrder AS domainOrder 
    RETURN domain, [domain] + [x IN subdomains WHERE x <> domain] AS subdomains 
    ORDER BY domainOrder, domain
    """
    
    return getQuery(query, driver, type="dict")

from functools import lru_cache

@lru_cache(maxsize=128)
def get_public_domains(database):
    """
    Retrieve top-level public domains ordered by display order.
    
    Args:
        database: Database identifier for driver (used as cache key)
        
    Returns:
        list: List of domain names
              Example: ["Domain1", "Domain2", "Domain3"]
    """
    driver = getDriver(database)
    
    query = """
    MATCH (n:LABEL) 
    WHERE n.public = 'TRUE' 
      AND NOT n.CMName = 'CATEGORY' 
      AND n.groupLabel = n.CMName 
    RETURN n.groupLabel AS domain 
    ORDER BY n.displayOrder, domain
    """
    
    return getQuery(query, driver, type="list")

from functools import lru_cache

@lru_cache(maxsize=10)
def get_domain_descriptions(database):
    """
    Retrieve descriptions for top-level public domains.
    
    Args:
        database: Database identifier for driver
        
    Returns:
        dict: Dictionary mapping domain labels to their descriptions
              Example: {"Biology": "Study of living organisms", ...}
              Returns error message string if connection fails
    """
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."
    
    query = """
    MATCH (n:LABEL) 
    WHERE n.CMName = n.groupLabel 
      AND n.public = 'TRUE' 
      AND NOT n.CMName = 'CATEGORY' 
    RETURN DISTINCT n.CMName AS label, n.description AS description 
    ORDER BY label
    """
    
    return getQuery(query, driver, type="dict")

def getLabelsMetadata(driver):
    query = """
    match (n:LABEL)
    return n.CMName as label, n.groupLabel as groupLabel, 
    n.relationship as relationship, n.public as public, 
    n.default as default, n.description as description, 
    n.displayName as displayName, n.remove as remove, n.color as color  
    """
    data = getQuery(query=query, driver=driver)
    return data
    
def getPropertiesMetadata(driver):
    query = """
    match (n:PROPERTY) 
    return n.CMName as property, n.type as type, 
    n.relationship as relationship, n.description as description, 
    n.display as display, n.group as group,
    n.metaType as metaType, n.search as search,
    n.translation as translation
    """
    data = getQuery(query=query, driver=driver)
    return data

def getNodeProperties(database, domain, CMID):
    """
    Get the properties of a node in the specified database.
    Args:
        database (str): The name of the database.
        domain (str): The domain of the node (e.g., "CATEGORY", "DATASET").
        CMID (list): A list of CMIDs to query.
    """
    driver = getDriver(database)
    if domain == "CATEGORY":
        query = """
        unwind $CMID as cmid
        match (c:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET)
        with distinct apoc.coll.toSet(apoc.coll.flatten(collect(keys(c) + keys(r)))) as properties
        with [i in properties where not i in ["log","logID","CMName","CMID","names","label"]] as properties
        unwind properties as property
        return property
        """
    elif domain == "DATASET":
        query = """
        unwind $CMID as cmid
        match (d:DATASET {CMID: cmid})
        with distinct apoc.coll.toSet(collect(keys(d))) as properties
        with [i in properties where not i in ["log","logID","CMName","CMID","names"]] as properties
        unwind properties as property
        return property
        """
    else: 
        raise ValueError("Invalid domain specified")

    properties = getQuery(query, driver, CMID = CMID, type = "list")
    
    return properties

@lru_cache(maxsize=128)
def _get_label_mapping(driver):
    query = "MATCH (l:LABEL) RETURN l.CMName AS label, l.groupLabel AS groupLabel"
    labels_data = getQuery(query=query, driver=driver)
    return pd.DataFrame(labels_data)