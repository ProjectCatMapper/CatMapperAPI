from .utils import getDriver, getQuery
from .metadata import _get_label_mapping
import json
import pandas as pd


def getDatasetData(database, cmid, domain, children):

    driver = getDriver(database)

    # Route handlers often pass cmid as a query string, but Cypher uses
    # UNWIND and therefore requires a list.
    cmid = _normalize_cmid(cmid)
    
    # Normalize domain
    domain = _normalize_domain(domain, driver)
    
    # Expand cmid if children requested
    if str(children).lower() == "true":
        cmid = _get_dataset_children(cmid, driver)
    
    # Build and execute main query
    query, query_params = _build_dataset_query(cmid, domain)
    data = getQuery(query=query, driver=driver, params=query_params)
    
    # Process results efficiently
    result = _process_dataset_results(data)
    return result


def _normalize_cmid(cmid):
    if isinstance(cmid, list):
        return [c for c in cmid if c]
    if isinstance(cmid, str):
        stripped = cmid.strip()
        if not stripped:
            return []
        # Support callers passing JSON/list-style query values.
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return [c for c in parsed if c]
            except Exception:
                pass
        return [stripped]
    if cmid is None:
        return []
    return [cmid]

def _normalize_domain(domain, driver):
    """Normalize and expand domain parameters."""
    if isinstance(domain, str):
        stripped = domain.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    domain = parsed
                else:
                    domain = [stripped]
            except Exception:
                domain = [stripped]
        else:
            domain = [stripped]
    
    if domain is None or "ANY DOMAIN" in domain:
        return ["CATEGORY"]
    
    if domain != ["CATEGORY"]:
        # Cache this query result if possible
        labels = _get_label_mapping(driver)
        
        # Expand groupLabel domains to include actual labels
        group_labels = set(labels['groupLabel'].values)
        matching_groups = [d for d in domain if d in group_labels]
        
        if matching_groups:
            matched_labels = labels[labels['groupLabel'].isin(matching_groups)]['label'].tolist()
            domain = matched_labels + domain
    
    return domain


def _get_dataset_children(cmid, driver):
    """Get all child datasets up to 5 levels deep."""
    query = """
    UNWIND $cmid AS cmid
    MATCH (:DATASET {CMID: cmid})-[:CONTAINS*1..5]->(d:DATASET)
    RETURN DISTINCT d.CMID AS CMID
    """
    result = getQuery(query=query, params={"cmid": cmid}, driver=driver, type="list")
    return [cmid] + (result if result else [])


def _build_dataset_query(cmid, domain):
    """Build the appropriate query based on domain."""
    base_query = """
    UNWIND $cmid AS cmid
    MATCH (a:DATASET {CMID: cmid})-[r:USES]->(b:CATEGORY)
    """
    
    if "CATEGORY" in domain:
        query = base_query + """
        UNWIND keys(r) AS property
        RETURN DISTINCT 
            a.CMName AS datasetName, 
            a.CMID AS datasetID,
            b.CMID AS CMID, 
            b.CMName AS CMName, 
            elementId(r) AS relID, 
            property, 
            r[property] AS value, 
            custom.getName(r[property]) AS property_name
        """
    else:
        query = base_query + """
        WHERE NOT isEmpty([i IN r.label WHERE i IN apoc.coll.flatten([$domain], true)])
        UNWIND keys(r) AS property
        RETURN DISTINCT 
            a.CMName AS datasetName, 
            a.CMID AS datasetID,
            b.CMID AS CMID, 
            b.CMName AS CMName, 
            elementId(r) AS relID, 
            property, 
            r[property] AS value, 
            custom.getName(r[property]) AS property_name
        """
    
    return query, {"cmid": cmid, "domain": domain}


def _process_dataset_results(data):
    """Process query results into final JSON format."""
    if not data:
        return json.dumps([])
    
    df = pd.DataFrame(data)
    df.dropna(axis=1, how='all', inplace=True)
    
    required_columns = ["datasetID", "CMID", "property", "property_name", "relID"]
    
    # Early return if missing required columns
    if not all(col in df.columns for col in required_columns):
        return df.to_json(orient="records")
    
    # Process property names
    df_names = df[required_columns].copy()
    df_names = df_names[df_names['property_name'].notna() & (df_names['property_name'] != '')]
    df_names['property'] = df_names['property'] + '_name'
    
    # Pivot tables
    index_cols = [col for col in df.columns if col not in ['property', 'value', 'property_name']]
    df = df.drop('property_name', axis=1)
    
    df = df.pivot_table(
        index=index_cols, 
        columns='property', 
        values='value', 
        aggfunc='first'
    ).reset_index()
    
    if not df_names.empty:
        df_names = df_names.pivot_table(
            index=["datasetID", "CMID", "relID"],
            columns='property',
            values='property_name',
            aggfunc='first'
        ).reset_index()
        
        df = df.merge(df_names, on=['datasetID', 'CMID', 'relID'], how='left')
    
    # Handle list columns efficiently
    list_cols = [col for col in df.columns 
                 if df[col].dtype == 'object' 
                 and not df[col].empty 
                 and isinstance(df[col].iloc[0], list)]
    
    for col in list_cols:
        df[col] = df[col].apply(lambda x: '; '.join(map(str, x)) if isinstance(x, list) else x)
    
    # Final cleanup
    df = df.drop('relID', axis=1, errors='ignore')
    df = df.astype(str).replace(['nan', 'None'], '')
    
    return df.to_json(orient='records')