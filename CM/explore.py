from CM import *
from bs4 import BeautifulSoup
from collections import defaultdict

from collections import defaultdict
from bs4 import BeautifulSoup

def getCategoryInfo(database, cmid):
    """
    Get basic category/dataset info for a given CMID.
    
    Args:
        database: Database identifier
        cmid: Content Management ID
    """
    driver = getDriver(database)
    
     # Get info
    
    label = getQuery(
        """
            UNWIND $cmid AS cmid
            MATCH (n:CATEGORY|DATASET|DELETED {CMID: cmid})
            RETURN labels(n) AS labels
            """,
            driver=driver, cmid=cmid, type = "list")
    
    if not label or not label[0]:
        return {"error": "Node not found"}
    
    if "DELETED" in label[0]:
        label = "DELETED"
    elif "DATASET" in label[0]:
        label = "DATASET"
    else:
        label = "CATEGORY"
    
    queries = _get_queries_for_label(label, database = database)
    
    info = getQuery(queries['info'], driver = driver, cmid=cmid, type = "dict")    
 
    # Process Domains field for DATASET
    for row in info:
        if 'Domains' in row and isinstance(row['Domains'], list) and 'DATASET' in row['Domains']:
            row['Domains'] = row['Domains'][-1]
            
    # Get parents
    if queries['parents']:
        parents = getQuery(queries['parents'], driver = driver, cmid=cmid)
    else:
        parents = []  
            
    # Post-process info
    if info:
        info[0] = _post_process_info(info[0], parents, label)
        
    return info[0] if info else {}

def getCategoryPage(database, cmid):
    """
    Get comprehensive category/dataset page data.
    
    Args:
        database: Database identifier
        cmid: Content Management ID
        
    Returns:
        dict: Contains samples, categories, childcategories, relnames
    """
    driver = getDriver(database)
    
    # Define bad relations to filter out
    bad_relations = ["HAS_LOG", "IS", "HAS_VECTOR"]
    
    # Get node metadata
    q_metadata = """
    UNWIND $cmid as cmid
    MATCH (n:CATEGORY|DATASET|DELETED {CMID: cmid}) 
    OPTIONAL MATCH path=((n)-[r]-())
    RETURN DISTINCT 
        labels(n) AS labels, 
        apoc.coll.toSet(apoc.coll.flatten(collect([rel in relationships(path) | type(rel)]))) AS relation_names
    """
    
    nodeMetaData = getQuery(q_metadata, driver=driver, params={'cmid': cmid}, type="records")
    if not nodeMetaData:
        return {
            "samples": [],
            "categories": [],
            "childcategories": [],
            "relnames": []
        }
    
    # Extract and filter relation names
    relnames = nodeMetaData[0].get('relation_names', [])
    relnames = [rel for rel in relnames if rel and rel not in bad_relations]
    
    # Determine label type
    labels = nodeMetaData[0].get('labels', [])
    if "DATASET" in labels:
        label = "DATASET"
    elif "DELETED" in labels:
        label = "DELETED"
    else:
        label = "CATEGORY"
    
    # Define queries based on label type
    queries = _get_queries_for_label(label, database = database)
    
    # Get categories
    if queries['categories']:
        categories = getQuery(queries['categories'], driver = driver, cmid=cmid)
    else:
        categories = []
    
    # Get samples
    if queries['samples']:
        samples = getQuery(queries['samples'], driver = driver, cmid=cmid, database = database)
        samples = _aggregate_samples(samples)
    else:
        samples = []
    
    # Get child categories
    if queries['child_categories']:
        childCategories = getQuery(queries['child_categories'], driver = driver, cmid=cmid)
    else:
        childCategories = []
    
    relnames = sorted(relnames, key=custom_sort)
    
    return {
        "samples": samples,
        "categories": categories,
        "childcategories": childCategories,
        "relnames": relnames
    }


def _get_queries_for_label(label, database):
    """
    Get appropriate queries based on node label type.
    
    Args:
        label: Node label ("CATEGORY", "DATASET", or "DELETED")
        
    Returns:
        dict: Dictionary of query strings
    """
    database = database.lower()
    if label == "CATEGORY":
        return {
            'info': '''
                UNWIND $cmid AS cmid 
                MATCH (a:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET)
                WITH a, r, d,
                    CASE WHEN r.country IS NOT NULL AND NOT r.country = [] THEN custom.getName(r.country) ELSE null END AS country,
                    CASE WHEN r.district IS NOT NULL AND NOT r.district = [] THEN custom.getName(r.district) ELSE null END AS district,
                    CASE WHEN r.language IS NOT NULL AND NOT r.language = [] THEN custom.getGlot(r.language) ELSE null END AS language,
                    CASE WHEN r.religion IS NOT NULL AND NOT r.religion = [] THEN custom.getName(r.religion) ELSE null END AS religion
                RETURN 
                    a.CMName AS CMName,
                    apoc.text.join([i IN [
                        custom.anytoList(collect(split(country, ', ')), true),
                        custom.anytoList(collect(split(district, ', ')), true)
                    ] WHERE NOT i = ''], ', ') AS Location,
                    a.CMID AS CMID,
                    apoc.text.join([i IN labels(a) WHERE NOT i = 'CATEGORY'], ', ') AS Domains,
                    custom.anytoList(collect(split(language, ', ')), true) AS Languages,
                    custom.anytoList(collect(split(religion, ', ')), true) AS Religions
            ''',
            
            'samples': '''
                UNWIND $cmid AS cmid
                MATCH (a:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET)
                WITH a, d, r, 
                    coalesce(d.project, d.CMName) AS Source,
                    d.CMID AS datasetID,
                    d.DatasetVersion AS Version
                WITH a, d, r, Source, datasetID, Version,
                    COLLECT(DISTINCT r.categoryType) AS allCTypes
                WITH a, d, r, Source, datasetID, Version, allCTypes,
                    SIZE([x IN allCTypes WHERE x IS NOT NULL AND x <> '']) AS cTypeCount
                WITH r, d, Source, datasetID, Version, cTypeCount,
                    r.Name AS Name,
                    CASE WHEN r.country IS NOT NULL THEN custom.getName(r.country) ELSE null END AS country,
                    CASE WHEN r.district IS NOT NULL THEN custom.getName(r.district) ELSE null END AS district,
                    r.url AS Link,
                    r.recordStart AS recordStart,
                    r.recordEnd AS recordEnd,
                    r.yearStart AS yearStart,
                    r.yearEnd AS yearEnd,
                    toInteger(r.populationEstimate) AS Population,
                    toInteger(r.sampleSize) AS `Sample size`,
                    r.type AS type,
                    CASE
                        WHEN r.populationEstimate IS NULL OR r.populationEstimate = 0 THEN null
                        WHEN cTypeCount >= 1 THEN r.categoryType
                        ELSE null
                    END AS cType
                RETURN 
                    apoc.text.join(Name, '; ') AS Name,
                    apoc.text.join([i IN [country, district] 
                        WHERE i IS NOT NULL AND i <> ''], ', ') AS Location,
                    type AS Type,
                    recordStart AS `rStart`,
                    recordEnd AS `rEnd`,
                    yearStart AS `yStart`,
                    yearEnd AS `yEnd`,
                    Population AS `Population est.`,
                    `Sample size` AS `Sample size`,
                    Source AS `Source`,
                    'https://catmapper.org/' + tolower($database) + '/' + datasetID AS `link2`,
                    Version,
                    cType,
                    r.Key AS Key,
                    Link
                ORDER BY Source, Name
            ''',
            
            'categories': """
                UNWIND $cmid AS cmid
                MATCH (a:ADM0 {CMID: cmid})-[r:AREA_OF]-(c)
                UNWIND labels(c) AS Domain 
                RETURN Domain, size(collect(DISTINCT c)) AS Count, sum(size(r.referenceKey)) AS TotalUses
                ORDER BY Domain
            """,
            
            'child_categories': None,
            
            'parents': """
                UNWIND $cmid AS cmid
                MATCH (n:CATEGORY {CMID: cmid})
                OPTIONAL MATCH (parent)-[:CONTAINS]->(n)
                WITH n, collect(DISTINCT parent.CMID) AS directParents
                OPTIONAL MATCH (n)-[:CONTAINS]->(child)
                WITH n, directParents, collect(DISTINCT child.CMID) AS directChildren
                OPTIONAL MATCH (n)-[:CONTAINS*1..]->(descendant)
                RETURN 
                    directParents,
                    directChildren,
                    collect(DISTINCT descendant.CMID) AS allDescendants
            """
        }
    
    elif label == "DATASET":
        return {
            'info': '''
                UNWIND $cmid AS cmid
                MATCH (a:DATASET)
                WHERE a.CMID = cmid
                WITH a, CASE WHEN a.District IS NOT NULL THEN custom.getName(a.District) ELSE null END AS Location
                RETURN 
                    a.CMName AS CMName,
                    custom.anytoList(collect(Location), true) AS Location,
                    a.CMID AS CMID,
                    labels(a) AS Domains,
                    a.parent AS Parent,
                    a.DatasetCitation AS Citation,
                    "<a href ='" + a.DatasetLocation + "' target='_blank' >" + a.DatasetLocation + "</a>" AS `Dataset Location`,
                    a.yearPublished AS `Year Published`,
                    CASE 
                        WHEN a.recordStart IS NULL AND a.recordEnd IS NULL THEN null
                        WHEN a.recordStart = a.recordEnd THEN a.recordStart
                        ELSE coalesce(a.recordStart, '') + '-' + coalesce(a.recordEnd, '')
                    END AS `Time Span`,
                    custom.getName(a.foci) AS Foci,
                    a.Note AS Note
            ''',
            
            'samples': None,
            
            'categories': """
                UNWIND $cmid AS cmid
                MATCH (d:DATASET {CMID: cmid})-[r:USES]->(c)
                WITH c, r,
                    CASE
                        WHEN r.label IS NULL THEN []
                        WHEN r.label IS :: LIST<ANY> THEN [x IN r.label | toString(x)]
                        WHEN r.label IS :: STRING THEN [r.label]
                        ELSE [toString(r.label)]
                    END AS domains
                UNWIND domains AS Domain
                WITH Domain, 
                    COUNT(DISTINCT c) AS distinctNodeCount,
                    COLLECT(r) AS usesRels
                WITH Domain, distinctNodeCount, usesRels, size(usesRels) AS totalUses
                RETURN Domain, distinctNodeCount AS Count, totalUses AS TotalUses
                ORDER BY Domain
            """,
            
            'child_categories': """
                UNWIND $cmid AS cmid
                MATCH (d:DATASET {CMID: cmid})
                OPTIONAL MATCH (d)-[:CONTAINS*..5]->(a)-[b:USES]->(cc)
                WITH b, cc,
                    CASE
                        WHEN b.label IS NULL THEN []
                        WHEN b.label IS :: LIST<ANY> THEN [x IN b.label | toString(x)]
                        WHEN b.label IS :: STRING THEN [b.label]
                        ELSE [toString(b.label)]
                    END AS domains
                UNWIND domains AS Domain
                RETURN
                    Domain,
                    COUNT(DISTINCT cc) AS ChildCount,
                    COUNT(b) AS TotalChildUses
                ORDER BY Domain
            """,
            
            'parents': """
                UNWIND $cmid AS cmid
                MATCH (n:DATASET {CMID: cmid})
                OPTIONAL MATCH (parent)-[:CONTAINS]->(n)
                WITH n, collect(DISTINCT parent.CMID) AS directParents
                OPTIONAL MATCH (n)-[:CONTAINS]->(child)
                WITH n, directParents, collect(DISTINCT child.CMID) AS directChildren
                OPTIONAL MATCH (n)-[:CONTAINS*1..]->(descendant)
                RETURN 
                    directParents,
                    directChildren,
                    collect(DISTINCT descendant.CMID) AS allDescendants
            """
        }
    
    else:  # DELETED
        return {
            'info': '''
                UNWIND $cmid AS cmid
                MATCH (a:DELETED)
                WHERE a.CMID = cmid
                OPTIONAL MATCH (a)-[:IS]->(b)
                RETURN 
                    a.CMName AS CMName,
                    a.CMID AS CMID,
                    labels(a) AS Domains,
                    CASE WHEN b IS NOT NULL THEN b.CMID ELSE NULL END AS Merged_into_CMID
            ''',
            'samples': None,
            'categories': None,
            'child_categories': None,
            'parents': None
        }


def _aggregate_samples(samples):
    """
    Aggregate sample data by grouping key.
    
    Args:
        samples: List of sample dictionaries
        
    Returns:
        list: Aggregated samples
    """
    grouped = defaultdict(lambda: {
        'Name': set(),
        'Key': [],
        'Population est.': 0,
        'Sample size': 0
    })
    
    for row in samples:
        # Create grouping key
        key = (
            row.get('Source')[0] if isinstance(row.get('Source'), list) else row.get('Source'),
            row.get('rStart'),
            row.get('rEnd'),
            row.get('Location'),
            row.get('Type')[0] if isinstance(row.get('Type'), list) else row.get('Type'),
            row.get('yStart'),
            row.get('yEnd'),
            row.get('link2'),
            row.get('Version'),
            row.get('cType'),
            row.get('Link'),
        )
        
        group = grouped[key]
        
        # Aggregate name
        name = row.get('Name')
        if name:
            group['Name'].add(name)

        # Aggregate Key values. When rows are combined, join distinct keys using
        # " || " so source keys remain visible without changing row grouping.
        raw_key = row.get('Key')
        if raw_key:
            if isinstance(raw_key, list):
                key_values = [str(v).strip() for v in raw_key if str(v).strip()]
            else:
                key_values = [str(raw_key).strip()]
            for key_value in key_values:
                if key_value and key_value not in group['Key']:
                    group['Key'].append(key_value)
        
        # Aggregate population
        try:
            pop = float(row.get('Population est.', 0))
            group['Population est.'] += pop
        except (ValueError, TypeError):
            pass
        
        # Aggregate sample size
        try:
            sample = float(row.get('Sample size', 0))
            group['Sample size'] += sample
        except (ValueError, TypeError):
            pass
    
    # Construct final output
    aggregated_samples = []
    
    for key, values in grouped.items():
        (Source, rStart, rEnd, Location, Type, yStart, yEnd, link2, Version, cType, Link) = key
        
        # Deduplicate and join names
        names_set = set(name.strip() for name in values['Name'])
        names_str = ", ".join(sorted(names_set))
        keys_str = " || ".join(values['Key'])
        
        # Round and handle zero values
        pop_est = round(values['Population est.'])
        sample_size = round(values['Sample size'])
        
        aggregated_samples.append({
            'Source': Source,
            'rStart': rStart,
            'rEnd': rEnd,
            'Location': Location,
            'Type': Type,
            'yStart': yStart,
            'yEnd': yEnd,
            'link2': link2,
            'Version': Version,
            'cType': cType,
            'Link': Link,
            'Name': names_str,
            'Key': keys_str,
            'Population est.': pop_est if pop_est > 0 else "",
            'Sample size': sample_size if sample_size > 0 else "",
        })
    
    return aggregated_samples


def _post_process_info(info, parents, label):
    """
    Post-process info dictionary.
    
    Args:
        info: Info dictionary
        parents: Parents data
        label: Node label type
        
    Returns:
        dict: Processed info
    """
    # Clean Dataset Location (extract href from HTML)
    if "Dataset Location" in info and info["Dataset Location"]:
        soup = BeautifulSoup(info["Dataset Location"], 'html.parser')
        link_tag = soup.find('a')
        if link_tag:
            info["Dataset Location"] = link_tag.get('href')
    
    # Clean Languages field
    if "Languages" in info and info['Languages']:
        langs = info['Languages']
        if langs.startswith(','):
            info['Languages'] = langs[2:].strip()
        if langs.endswith(','):
            info['Languages'] = langs[:-2].strip()
    
    # Clean Location field
    if "Location" in info and info['Location']:
        loc = info['Location']
        if len(loc) >= 2 and loc[-2:].endswith(','):
            info['Location'] = loc[:-2].strip()
    
    # Add parent/children counts
    if parents and label != "DELETED":
        parent_data = parents[0]
        info['direct_Parents'] = len(parent_data.get('directParents', []))
        info['direct_Children'] = len(parent_data.get('directChildren', []))
        info['all_Descendants'] = len(parent_data.get('allDescendants', []))
    
    return info


def custom_sort(item):
    """
    Custom sort function for relation names.
    Define your custom sorting logic here.
    """
    # Placeholder - implement your actual custom_sort logic
    priority = {
        'CONTAINS': 0,
        'USES': 1,
        'HAS_GEOMETRY': 2,
    }
    return priority.get(item, 999), item


MAP_LAYER_DIRECT = "direct"
MAP_LAYER_RELATED = "related"
MAP_LAYER_DESCENDANTS = "descendants"
MAP_LAYER_USES_CATEGORIES = "uses"
MAP_INHERITANCE_RELATIONSHIPS = [
    "AREA_OF",
    "LANGUOID_OF",
    "RELIGION_OF",
    "PERIOD_OF",
    "CULTURE_OF",
    "POLITY_OF",
    "VARIABLE_OF",
]
DEFAULT_MAP_DESCENDANT_DEPTH = 5
MAX_MAP_DESCENDANT_DEPTH = 30
DEFAULT_MAP_NODE_LIMIT = 5000
MAX_MAP_NODE_LIMIT = 5000
DEFAULT_MAP_POINT_LIMIT = 5000
MAX_MAP_POINT_LIMIT = 20000
DEFAULT_MAP_POLYGON_LIMIT = 2500
MAX_MAP_POLYGON_LIMIT = 10000
DEFAULT_MAP_FEATURE_LIMIT = DEFAULT_MAP_POINT_LIMIT + DEFAULT_MAP_POLYGON_LIMIT
MAX_MAP_FEATURE_LIMIT = MAX_MAP_POINT_LIMIT + MAX_MAP_POLYGON_LIMIT


def _split_param_values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(_split_param_values(item))
        return values
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _coerce_int(value, default, minimum, maximum):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _normalize_map_layers(layers):
    requested = [item.lower() for item in _split_param_values(layers)]
    if not requested:
        return [MAP_LAYER_DIRECT]
    aliases = {
        "used_categories": MAP_LAYER_USES_CATEGORIES,
        "uses_categories": MAP_LAYER_USES_CATEGORIES,
        "usescategories": MAP_LAYER_USES_CATEGORIES,
    }
    allowed = {
        MAP_LAYER_DIRECT,
        MAP_LAYER_RELATED,
        MAP_LAYER_DESCENDANTS,
        MAP_LAYER_USES_CATEGORIES,
    }
    normalized = [aliases.get(item, item) for item in requested]
    normalized = [item for item in normalized if item in allowed]
    return normalized or [MAP_LAYER_DIRECT]


def _normalize_inheritance_relations(relations):
    requested = [item.upper() for item in _split_param_values(relations)]
    if not requested:
        return list(MAP_INHERITANCE_RELATIONSHIPS)
    allowed = set(MAP_INHERITANCE_RELATIONSHIPS)
    return [item for item in requested if item in allowed]


def _empty_geometry_payload():
    return {
        "polygons": [],
        "points": [],
        "datasetpoints": [],
        "polysources": [],
        "badsources": [],
    }


def _polygon_feature_count(polygons):
    if not polygons:
        return 0
    if isinstance(polygons, dict):
        if isinstance(polygons.get("features"), list):
            return len(polygons["features"])
        if polygons.get("type"):
            return 1
        return 0
    if isinstance(polygons, list):
        return len(polygons)
    return 0


def _limit_points(points, feature_limit):
    if not isinstance(points, list):
        return [], 0
    if len(points) <= feature_limit:
        return points, 0
    return points[:feature_limit], len(points) - feature_limit


def _limit_polygons(polygons, feature_limit):
    count = _polygon_feature_count(polygons)
    if count <= feature_limit:
        return polygons, 0

    truncated = count - feature_limit
    if isinstance(polygons, dict) and isinstance(polygons.get("features"), list):
        limited = polygons.copy()
        limited["features"] = polygons["features"][:feature_limit]
        return limited, truncated
    if isinstance(polygons, list):
        return polygons[:feature_limit], truncated
    if feature_limit < 1:
        return [], truncated
    return polygons, truncated


def _limit_map_features(points, polygons, point_limit, polygon_limit, feature_limit=None):
    if feature_limit is not None:
        limited_points, truncated_points = _limit_points(points, feature_limit)
        remaining_polygon_limit = max(0, feature_limit - len(limited_points))
        limited_polygons, truncated_polygons = _limit_polygons(polygons, remaining_polygon_limit)
        return limited_points, limited_polygons, truncated_points, truncated_polygons

    limited_points, truncated_points = _limit_points(points, point_limit)
    limited_polygons, truncated_polygons = _limit_polygons(polygons, polygon_limit)
    return limited_points, limited_polygons, truncated_points, truncated_polygons


def _get_geometry_counts_for_cmids(driver, cmids):
    cmids = [cmid for cmid in dict.fromkeys(cmids or []) if cmid]
    if not cmids:
        return {}

    counts = {
        cmid: {
            "pointCount": 0,
            "polygonCount": 0,
        }
        for cmid in cmids
    }

    point_query = """
    UNWIND $cmids AS cmid
    MATCH (c:CATEGORY {CMID: cmid})
    OPTIONAL MATCH (c)<-[pointRel:USES]-(:DATASET)
    WHERE pointRel.geoCoords IS NOT NULL
    RETURN
        c.CMID AS CMID,
        count(DISTINCT pointRel) AS pointCount
    """
    for row in getQuery(point_query, driver, params={"cmids": cmids}):
        cmid = row.get("CMID")
        if cmid in counts:
            counts[cmid]["pointCount"] = int(row.get("pointCount") or 0)

    polygon_ref_query = """
    MATCH (c:CATEGORY)<-[polyRel:USES]-(:DATASET)
    WHERE c.CMID IN $cmids AND polyRel.geoPolygon IS NOT NULL
    RETURN c.CMID AS CMID, polyRel.geoPolygon AS geomID
    """
    polygon_refs = getQuery(polygon_ref_query, driver, params={"cmids": cmids})
    geom_to_cmids = defaultdict(set)
    for row in polygon_refs:
        cmid = row.get("CMID")
        geom_ids = _normalize_geom_ids(row.get("geomID"))
        for geom_id in geom_ids:
            geom_to_cmids[geom_id].add(cmid)

    if geom_to_cmids:
        try:
            driver_gis = getDriver('gisdb')
            geometry_count_query = """
            UNWIND $geomIDs AS geomID
            MATCH (g:GEOMETRY)
            WHERE g.geomID = geomID
            RETURN DISTINCT g.geomID AS geomID
            """
            found_geometries = getQuery(
                geometry_count_query,
                driver_gis,
                params={"geomIDs": list(geom_to_cmids.keys())},
            )
            for row in found_geometries:
                for cmid in geom_to_cmids.get(row.get("geomID"), []):
                    if cmid in counts:
                        counts[cmid]["polygonCount"] += 1
        except Exception:
            # The options endpoint should not fail the Explore page when gisdb is down.
            pass

    return counts


def _normalize_geom_ids(value):
    if value is None:
        return []
    if isinstance(value, list):
        values = []
        for item in value:
            values.extend(_normalize_geom_ids(item))
        return values
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                return _normalize_geom_ids(json.loads(text))
            except (json.JSONDecodeError, TypeError):
                return [text]
        return [text]
    return [value]


def _get_related_map_nodes(driver, cmid, relationships, node_limit):
    if not relationships:
        return []

    query = """
    MATCH (n:CATEGORY {CMID: $cmid})-[r]-(related:CATEGORY)
    WHERE type(r) IN $relationships AND related.CMID <> $cmid
    RETURN DISTINCT
        related.CMID AS CMID,
        coalesce(related.CMName, related.Name, related.CMID) AS CMName,
        labels(related) AS labels,
        type(r) AS relationship,
        [n.CMID, related.CMID] AS path
    ORDER BY relationship, CMName, CMID
    LIMIT $node_limit
    """
    return getQuery(
        query,
        driver,
        params={"cmid": cmid, "relationships": relationships, "node_limit": node_limit},
    )


def _get_related_map_node_counts(driver, cmid, relationships):
    if not relationships:
        return {}

    query = """
    MATCH (n:CATEGORY {CMID: $cmid})-[r]-(related:CATEGORY)
    WHERE type(r) IN $relationships AND related.CMID <> $cmid
    RETURN type(r) AS relationship, count(DISTINCT related) AS totalNodeCount
    """
    rows = getQuery(
        query,
        driver,
        params={"cmid": cmid, "relationships": relationships},
    )
    return {
        row.get("relationship"): int(row.get("totalNodeCount") or 0)
        for row in rows
        if row.get("relationship")
    }


def _get_dataset_used_category_nodes(driver, cmid, node_limit):
    query = """
    MATCH (d:DATASET {CMID: $cmid})-[r:USES]->(category:CATEGORY)
    WHERE category.CMID <> $cmid
    RETURN DISTINCT
        category.CMID AS CMID,
        coalesce(category.CMName, category.Name, category.CMID) AS CMName,
        labels(category) AS labels,
        "USES" AS relationship,
        [d.CMID, category.CMID] AS path
    ORDER BY CMName, CMID
    LIMIT $node_limit
    """
    return getQuery(query, driver, params={"cmid": cmid, "node_limit": node_limit})


def _get_dataset_used_category_count(driver, cmid):
    query = """
    MATCH (d:DATASET {CMID: $cmid})-[:USES]->(category:CATEGORY)
    WHERE category.CMID <> $cmid
    RETURN count(DISTINCT category) AS totalNodeCount
    """
    rows = getQuery(query, driver, params={"cmid": cmid})
    if not rows:
        return 0
    return int(rows[0].get("totalNodeCount") or 0)


def _get_descendant_map_nodes(driver, cmid, max_depth, node_limit):
    max_depth = _coerce_int(max_depth, DEFAULT_MAP_DESCENDANT_DEPTH, 1, MAX_MAP_DESCENDANT_DEPTH)
    query = f"""
    MATCH (n:CATEGORY {{CMID: $cmid}})
    MATCH path=(n)-[:CONTAINS*1..{max_depth}]->(descendant:CATEGORY)
    WHERE descendant.CMID <> $cmid
    WITH
        descendant,
        min(length(path)) AS depth,
        head(collect([node IN nodes(path) | node.CMID])) AS pathCmids
    RETURN DISTINCT
        descendant.CMID AS CMID,
        coalesce(descendant.CMName, descendant.Name, descendant.CMID) AS CMName,
        labels(descendant) AS labels,
        depth,
        pathCmids AS path,
        "CONTAINS" AS relationship
    ORDER BY depth, CMName, CMID
    LIMIT $node_limit
    """
    return getQuery(query, driver, params={"cmid": cmid, "node_limit": node_limit})


def _get_descendant_map_node_summary(driver, cmid, max_depth):
    max_depth = _coerce_int(max_depth, DEFAULT_MAP_DESCENDANT_DEPTH, 1, MAX_MAP_DESCENDANT_DEPTH)
    query = f"""
    MATCH (n:CATEGORY {{CMID: $cmid}})
    MATCH path=(n)-[:CONTAINS*1..{max_depth}]->(descendant:CATEGORY)
    WHERE descendant.CMID <> $cmid
    WITH descendant, min(length(path)) AS depth
    WITH depth, count(descendant) AS nodeCount
    ORDER BY depth
    WITH collect({{depth: depth, nodeCount: nodeCount}}) AS depthCounts, sum(nodeCount) AS totalNodeCount
    RETURN totalNodeCount, depthCounts
    """
    rows = getQuery(query, driver, params={"cmid": cmid})
    if not rows:
        return {"totalNodeCount": 0, "depthCounts": []}
    row = rows[0]
    return {
        "totalNodeCount": int(row.get("totalNodeCount") or 0),
        "depthCounts": row.get("depthCounts") or [],
    }


def _get_points_for_cmids(driver, cmids):
    cmids = [cmid for cmid in dict.fromkeys(cmids or []) if cmid]
    if not cmids:
        return []

    query = """
    MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
    WHERE c.CMID IN $cmids AND r.geoCoords IS NOT NULL
    RETURN DISTINCT
        r.geoCoords AS geometry,
        coalesce(d.shortName, d.CMName, d.CMID) AS source,
        r.Key AS Key,
        c.CMID AS sourceNodeCMID,
        coalesce(c.CMName, c.Name, c.CMID) AS sourceNodeName,
        labels(c) AS sourceNodeLabels
    """
    return [dict(record) for record in getQuery(query, driver, params={"cmids": cmids})]


def _get_polygons_for_cmids(driver, cmids, simple=True):
    cmids = [cmid for cmid in dict.fromkeys(cmids or []) if cmid]
    if not cmids:
        return []

    query = """
    MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
    WHERE c.CMID IN $cmids AND r.geoPolygon IS NOT NULL
    RETURN DISTINCT
        r.geoPolygon AS geomID,
        coalesce(d.shortName, d.CMName, d.CMID) AS source,
        c.CMID AS sourceNodeCMID,
        coalesce(c.CMName, c.Name, c.CMID) AS sourceNodeName,
        labels(c) AS sourceNodeLabels
    """
    rows = getQuery(query, driver, params={"cmids": cmids})
    if not rows:
        return []

    lookup_rows = []
    for row in rows:
        for geom_id in _normalize_geom_ids(row.get("geomID")):
            lookup_rows.append({**dict(row), "geomID": geom_id})
    if not lookup_rows:
        return []

    driverGIS = getDriver('gisdb')
    if simple:
        geometry_query = """
        UNWIND $rows AS row
        MATCH (g:GEOMETRY)
        WHERE g.geomID = row.geomID
        RETURN
            row.source AS source,
            row.sourceNodeCMID AS sourceNodeCMID,
            row.sourceNodeName AS sourceNodeName,
            row.sourceNodeLabels AS sourceNodeLabels,
            coalesce(g.simplified, g.geometry) AS geometry,
            g.simplified IS NOT NULL AS simple
        """
    else:
        geometry_query = """
        UNWIND $rows AS row
        MATCH (g:GEOMETRY)
        WHERE g.geomID = row.geomID
        RETURN
            row.source AS source,
            row.sourceNodeCMID AS sourceNodeCMID,
            row.sourceNodeName AS sourceNodeName,
            row.sourceNodeLabels AS sourceNodeLabels,
            g.geometry AS geometry
        """
    return getQuery(geometry_query, driverGIS, params={"rows": lookup_rows})


def _build_layer_option(layer_id, label, mode, nodes, counts_by_cmid, **extra):
    node_count = len(nodes)
    total_node_count = extra.pop("totalNodeCount", node_count)
    node_limit = extra.get("nodeLimit")
    point_count = sum(counts_by_cmid.get(node.get("CMID"), {}).get("pointCount", 0) for node in nodes)
    polygon_count = sum(counts_by_cmid.get(node.get("CMID"), {}).get("polygonCount", 0) for node in nodes)
    option = {
        "id": layer_id,
        "label": label,
        "mode": mode,
        "available": point_count > 0 or polygon_count > 0,
        "nodeCount": node_count,
        "displayedNodeCount": node_count,
        "totalNodeCount": total_node_count,
        "truncatedNodeCount": max(0, total_node_count - node_count),
        "nodeLimited": total_node_count > node_count,
        "nodeLimit": node_limit,
        "pointCount": point_count,
        "polygonCount": polygon_count,
    }
    option.update(extra)
    return option


def getMapLayerOptions(database, cmid, max_depth=DEFAULT_MAP_DESCENDANT_DEPTH, node_limit=DEFAULT_MAP_NODE_LIMIT):
    """
    Return cheap map-layer availability summaries for a node.
    Full inherited geometry is intentionally loaded through exploreGeometry only
    after a user selects a layer.
    """
    driver = getDriver(database)
    max_depth = _coerce_int(max_depth, DEFAULT_MAP_DESCENDANT_DEPTH, 1, MAX_MAP_DESCENDANT_DEPTH)
    node_limit = _coerce_int(node_limit, DEFAULT_MAP_NODE_LIMIT, 1, MAX_MAP_NODE_LIMIT)

    direct_counts = _get_geometry_counts_for_cmids(driver, [cmid]).get(
        cmid, {"pointCount": 0, "polygonCount": 0}
    )
    layers = [
        {
            "id": MAP_LAYER_DIRECT,
            "label": "Direct locations",
            "mode": MAP_LAYER_DIRECT,
            "available": direct_counts["pointCount"] > 0 or direct_counts["polygonCount"] > 0,
            "nodeCount": 1,
            "pointCount": direct_counts["pointCount"],
            "polygonCount": direct_counts["polygonCount"],
        }
    ]

    used_category_nodes = _get_dataset_used_category_nodes(driver, cmid, node_limit)
    if used_category_nodes:
        used_category_counts = _get_geometry_counts_for_cmids(
            driver, [node.get("CMID") for node in used_category_nodes]
        )
        layers.append(
            _build_layer_option(
                f"{MAP_LAYER_USES_CATEGORIES}:CATEGORY",
                "USES category locations",
                MAP_LAYER_USES_CATEGORIES,
                used_category_nodes,
                used_category_counts,
                relationship="USES",
                totalNodeCount=_get_dataset_used_category_count(driver, cmid),
                nodeLimit=node_limit,
            )
        )

    related_nodes = _get_related_map_nodes(
        driver, cmid, MAP_INHERITANCE_RELATIONSHIPS, node_limit
    )
    related_total_counts = _get_related_map_node_counts(
        driver, cmid, MAP_INHERITANCE_RELATIONSHIPS
    )
    related_counts = _get_geometry_counts_for_cmids(
        driver, [node.get("CMID") for node in related_nodes]
    )
    nodes_by_relationship = defaultdict(list)
    for node in related_nodes:
        nodes_by_relationship[node.get("relationship")].append(node)
    for relationship in MAP_INHERITANCE_RELATIONSHIPS:
        relationship_nodes = nodes_by_relationship.get(relationship, [])
        if not relationship_nodes:
            continue
        layers.append(
            _build_layer_option(
                f"{MAP_LAYER_RELATED}:{relationship}",
                f"Related {relationship} locations",
                MAP_LAYER_RELATED,
                relationship_nodes,
                related_counts,
                relationship=relationship,
                totalNodeCount=related_total_counts.get(relationship, len(relationship_nodes)),
                nodeLimit=node_limit,
            )
        )

    descendant_nodes = _get_descendant_map_nodes(driver, cmid, max_depth, node_limit)
    descendant_summary = _get_descendant_map_node_summary(driver, cmid, max_depth)
    descendant_counts = _get_geometry_counts_for_cmids(
        driver, [node.get("CMID") for node in descendant_nodes]
    )
    if descendant_nodes:
        layers.append(
            _build_layer_option(
                f"{MAP_LAYER_DESCENDANTS}:CONTAINS",
                "Descendant locations",
                MAP_LAYER_DESCENDANTS,
                descendant_nodes,
                descendant_counts,
                relationship="CONTAINS",
                maxDepth=max_depth,
                totalNodeCount=descendant_summary.get("totalNodeCount", len(descendant_nodes)),
                depthCounts=descendant_summary.get("depthCounts", []),
                nodeLimit=node_limit,
            )
        )

    return {
        "database": database,
        "cmid": cmid,
        "layers": layers,
        "limits": {
            "maxDepth": MAX_MAP_DESCENDANT_DEPTH,
            "maxNodes": MAX_MAP_NODE_LIMIT,
            "defaultDepth": DEFAULT_MAP_DESCENDANT_DEPTH,
            "defaultNodeLimit": DEFAULT_MAP_NODE_LIMIT,
            "defaultPointLimit": DEFAULT_MAP_POINT_LIMIT,
            "defaultPolygonLimit": DEFAULT_MAP_POLYGON_LIMIT,
            "defaultFeatureLimit": DEFAULT_MAP_FEATURE_LIMIT,
            "maxPointLimit": MAX_MAP_POINT_LIMIT,
            "maxPolygonLimit": MAX_MAP_POLYGON_LIMIT,
        },
    }


def _metadata_for_inherited_node(node, mode, relationship):
    inherited_from = node.get("CMID")
    inherited_name = node.get("CMName") or inherited_from
    return {
        "layerType": "inherited",
        "inherited": True,
        "inheritanceMode": mode,
        "inheritanceRelationship": node.get("relationship") or relationship,
        "inheritanceDepth": node.get("depth", 1),
        "inheritancePath": node.get("path") or [inherited_from],
        "inheritedFromCMID": inherited_from,
        "inheritedFromName": inherited_name,
    }


def _annotate_rows_for_inherited_layer(rows, nodes_by_cmid, mode, relationship):
    annotated = []
    for row in rows:
        row_dict = dict(row)
        node = nodes_by_cmid.get(row_dict.get("sourceNodeCMID")) or {}
        row_dict.update(_metadata_for_inherited_node(node, mode, relationship))
        annotated.append(row_dict)
    return annotated


def _build_geometry_layer(
    layer_id,
    label,
    mode,
    points,
    polygons,
    nodes=None,
    relationship=None,
    truncated=0,
    total_node_count=None,
    node_limit=None,
    point_limit=None,
    polygon_limit=None,
    feature_limit=None,
    depth_counts=None,
):
    displayed_node_count = len(nodes or [])
    total_node_count = displayed_node_count if total_node_count is None else total_node_count
    return {
        "id": layer_id,
        "label": label,
        "mode": mode,
        "relationship": relationship,
        "nodeCount": displayed_node_count,
        "displayedNodeCount": displayed_node_count,
        "totalNodeCount": total_node_count,
        "truncatedNodeCount": max(0, total_node_count - displayed_node_count),
        "nodeLimited": total_node_count > displayed_node_count,
        "nodeLimit": node_limit,
        "pointLimit": point_limit,
        "polygonLimit": polygon_limit,
        "featureLimit": feature_limit,
        "depthCounts": depth_counts or [],
        "pointCount": len(points or []),
        "polygonCount": _polygon_feature_count(polygons),
        "truncatedFeatureCount": truncated,
        "points": points or [],
        "polygons": polygons or [],
    }


def _build_inherited_geometry_layer(
    driver,
    layer_id,
    label,
    mode,
    nodes,
    relationship,
    point_limit,
    polygon_limit,
    feature_limit=None,
    total_node_count=None,
    node_limit=None,
    depth_counts=None,
):
    if not nodes:
        return _build_geometry_layer(
            layer_id,
            label,
            mode,
            [],
            [],
            [],
            relationship,
            total_node_count=total_node_count,
            node_limit=node_limit,
            point_limit=point_limit,
            polygon_limit=polygon_limit,
            feature_limit=feature_limit,
            depth_counts=depth_counts,
        )

    nodes_by_cmid = {node.get("CMID"): node for node in nodes if node.get("CMID")}
    cmids = list(nodes_by_cmid.keys())
    raw_points = _annotate_rows_for_inherited_layer(
        _get_points_for_cmids(driver, cmids), nodes_by_cmid, mode, relationship
    )
    raw_polygons = _annotate_rows_for_inherited_layer(
        _get_polygons_for_cmids(driver, cmids), nodes_by_cmid, mode, relationship
    )
    points, bad_sources = _validate_points(raw_points, preserve_metadata=True)
    polygons, _polysources = _process_polygons(raw_polygons, preserve_metadata=True)

    points, polygons, truncated_points, truncated_polygons = _limit_map_features(
        points,
        polygons,
        point_limit,
        polygon_limit,
        feature_limit,
    )
    layer = _build_geometry_layer(
        layer_id,
        label,
        mode,
        points,
        polygons,
        nodes,
        relationship,
        truncated_points + truncated_polygons,
        total_node_count=total_node_count,
        node_limit=node_limit,
        point_limit=point_limit,
        polygon_limit=polygon_limit,
        feature_limit=feature_limit,
        depth_counts=depth_counts,
    )
    layer["badsources"] = bad_sources
    return layer


def _explore_direct_geometry(cmid, driver):
    polygons = getPolygon(cmid, driver)
    points = getPoints(cmid, driver)
    dataset_points = getDatasetPoints(cmid, driver)

    transformed_points = _transform_dataset_points(dataset_points)
    polygons, polysources = _process_polygons(polygons)
    points, bad_sources = _validate_points(points)

    return {
        "polygons": polygons,
        "points": points,
        "datasetpoints": transformed_points,
        "polysources": polysources,
        "badsources": bad_sources
    }

def exploreGeometry(
    database,
    cmid,
    layers=None,
    relations=None,
    max_depth=DEFAULT_MAP_DESCENDANT_DEPTH,
    node_limit=DEFAULT_MAP_NODE_LIMIT,
    point_limit=None,
    polygon_limit=None,
    feature_limit=None,
):
    """
    Explore and process geometry data for a given CMID.
    
    Args:
        database: Database identifier
        cmid: Content Management ID
        layers: Optional comma-separated/list of direct, related, descendants
            or uses
        relations: Optional relationship allow-list for related inheritance
        
    Returns:
        dict: Dictionary containing polygons, points, dataset points, sources, and errors
    """
    driver = getDriver(database)
    requested_layers = _normalize_map_layers(layers)
    requested_relations = _normalize_inheritance_relations(relations)
    max_depth = _coerce_int(max_depth, DEFAULT_MAP_DESCENDANT_DEPTH, 1, MAX_MAP_DESCENDANT_DEPTH)
    node_limit = _coerce_int(node_limit, DEFAULT_MAP_NODE_LIMIT, 1, MAX_MAP_NODE_LIMIT)
    legacy_feature_limit = None
    if feature_limit is not None and point_limit is None and polygon_limit is None:
        legacy_feature_limit = _coerce_int(
            feature_limit,
            DEFAULT_MAP_FEATURE_LIMIT,
            1,
            MAX_MAP_FEATURE_LIMIT,
        )
    point_limit = _coerce_int(point_limit, DEFAULT_MAP_POINT_LIMIT, 1, MAX_MAP_POINT_LIMIT)
    polygon_limit = _coerce_int(polygon_limit, DEFAULT_MAP_POLYGON_LIMIT, 0, MAX_MAP_POLYGON_LIMIT)

    if MAP_LAYER_DIRECT in requested_layers:
        result = _explore_direct_geometry(cmid, driver)
    else:
        result = _empty_geometry_payload()

    map_layers = []
    if MAP_LAYER_DIRECT in requested_layers:
        direct_points, direct_polygons, truncated_points, truncated_polygons = _limit_map_features(
            result["points"],
            result["polygons"],
            point_limit,
            polygon_limit,
            legacy_feature_limit,
        )
        dataset_point_limit = legacy_feature_limit if legacy_feature_limit is not None else point_limit
        dataset_points, truncated_dataset_points = _limit_points(result["datasetpoints"], dataset_point_limit)
        result["points"] = direct_points
        result["polygons"] = direct_polygons
        result["datasetpoints"] = dataset_points
        direct_layer_points = direct_points if direct_points else dataset_points
        map_layers.append(
            _build_geometry_layer(
                MAP_LAYER_DIRECT,
                "Direct locations",
                MAP_LAYER_DIRECT,
                direct_layer_points,
                direct_polygons,
                [{"CMID": cmid}],
                None,
                truncated_points + truncated_dataset_points + truncated_polygons,
                point_limit=point_limit,
                polygon_limit=polygon_limit,
                feature_limit=legacy_feature_limit,
            )
        )

    if MAP_LAYER_USES_CATEGORIES in requested_layers:
        used_category_nodes = _get_dataset_used_category_nodes(driver, cmid, node_limit)
        if used_category_nodes:
            map_layers.append(
                _build_inherited_geometry_layer(
                    driver,
                    f"{MAP_LAYER_USES_CATEGORIES}:CATEGORY",
                    "USES category locations",
                    MAP_LAYER_USES_CATEGORIES,
                    used_category_nodes,
                    "USES",
                    point_limit,
                    polygon_limit,
                    feature_limit=legacy_feature_limit,
                    total_node_count=_get_dataset_used_category_count(driver, cmid),
                    node_limit=node_limit,
                )
            )

    if MAP_LAYER_RELATED in requested_layers and requested_relations:
        related_nodes = _get_related_map_nodes(driver, cmid, requested_relations, node_limit)
        related_total_counts = _get_related_map_node_counts(driver, cmid, requested_relations)
        nodes_by_relationship = defaultdict(list)
        for node in related_nodes:
            nodes_by_relationship[node.get("relationship")].append(node)
        for relationship in requested_relations:
            relationship_nodes = nodes_by_relationship.get(relationship, [])
            if not relationship_nodes:
                continue
            map_layers.append(
                _build_inherited_geometry_layer(
                    driver,
                    f"{MAP_LAYER_RELATED}:{relationship}",
                    f"Related {relationship} locations",
                    MAP_LAYER_RELATED,
                    relationship_nodes,
                    relationship,
                    point_limit,
                    polygon_limit,
                    feature_limit=legacy_feature_limit,
                    total_node_count=related_total_counts.get(relationship, len(relationship_nodes)),
                    node_limit=node_limit,
                )
            )

    if MAP_LAYER_DESCENDANTS in requested_layers:
        descendant_nodes = _get_descendant_map_nodes(driver, cmid, max_depth, node_limit)
        descendant_summary = _get_descendant_map_node_summary(driver, cmid, max_depth)
        if descendant_nodes:
            map_layers.append(
                _build_inherited_geometry_layer(
                    driver,
                    f"{MAP_LAYER_DESCENDANTS}:CONTAINS",
                    "Descendant locations",
                    MAP_LAYER_DESCENDANTS,
                    descendant_nodes,
                    "CONTAINS",
                    point_limit,
                    polygon_limit,
                    feature_limit=legacy_feature_limit,
                    total_node_count=descendant_summary.get("totalNodeCount", len(descendant_nodes)),
                    node_limit=node_limit,
                    depth_counts=descendant_summary.get("depthCounts", []),
                )
            )

    result["maplayers"] = map_layers
    result["limits"] = {
        "maxDepth": max_depth,
        "nodeLimit": node_limit,
        "pointLimit": point_limit,
        "polygonLimit": polygon_limit,
        "featureLimit": legacy_feature_limit,
    }
    return result


def _transform_dataset_points(dataset_points):
    """Transform dataset points to include coordinate arrays."""
    transformed_points = []
    
    for point in dataset_points:
        try:
            geom = json.loads(point["geometry"])
            if not geom:
                continue
            
            coords = geom.get("coordinates")
            geom_type = geom.get("type")
            
            if geom_type == "Point" and isinstance(coords, list) and len(coords) == 2:
                new_point = point.copy()
                new_point["cood"] = [coords[0], coords[1]]
                transformed_points.append(new_point)
            elif geom_type == "MultiPoint" and isinstance(coords, list):
                for lng, lat in coords:
                    if isinstance(lng, (int, float)) and isinstance(lat, (int, float)):
                        new_point = point.copy()
                        new_point["cood"] = [lng, lat]
                        transformed_points.append(new_point)
            else:
                point["cood"] = None
                transformed_points.append(point)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            point["cood"] = None
            transformed_points.append(point)
    
    return transformed_points


def _feature_metadata_from_row(row):
    metadata_keys = [
        "layerType",
        "inherited",
        "inheritanceMode",
        "inheritanceRelationship",
        "inheritanceDepth",
        "inheritancePath",
        "inheritedFromCMID",
        "inheritedFromName",
        "sourceNodeCMID",
        "sourceNodeName",
        "sourceNodeLabels",
    ]
    return {key: row.get(key) for key in metadata_keys if key in row}


def _apply_polygon_metadata(feature, polygon, preserve_metadata=False):
    if not isinstance(feature, dict):
        return feature

    if feature.get("type") == "FeatureCollection":
        feature["source"] = polygon.get("source")
        feature["features"] = [
            _apply_polygon_metadata(child, polygon, preserve_metadata)
            for child in feature.get("features", [])
        ]
        return feature

    feature["source"] = polygon.get("source")
    if not preserve_metadata:
        properties = feature.get("properties")
        if isinstance(properties, dict):
            properties["source"] = polygon.get("source")
        else:
            feature["properties"] = {"source": polygon.get("source")}
        return feature

    metadata = _feature_metadata_from_row(polygon)
    feature.update(metadata)
    properties = feature.get("properties")
    if not isinstance(properties, dict):
        properties = {}
    properties["source"] = polygon.get("source")
    properties.update(metadata)
    feature["properties"] = properties
    return feature


def _process_polygons(polygons, preserve_metadata=False):
    """Process polygon geometries into GeoJSON format."""
    polysources = []
    
    if not polygons or len(polygons) == 0:
        return polygons, polysources
    
    if len(polygons) > 1:
        # Multiple polygons - create FeatureCollection
        poly = {"type": 'FeatureCollection', "features": []}
        for i, polygon in enumerate(polygons):
            feature = json.loads(polygon['geometry'])
            feature = _apply_polygon_metadata(feature, polygon, preserve_metadata)
            poly["features"].append(feature)
            polysources.append(polygon['source'])
        return poly, polysources
    else:
        # Single polygon
        poly = json.loads(polygons[0]['geometry'])
        poly = _apply_polygon_metadata(poly, polygons[0], preserve_metadata)
        polysources.append(polygons[0]['source'])
        return [poly], polysources


def _point_payload(entry, coord, preserve_metadata=False):
    payload = {
        "cood": coord,
        "source": entry["source"]
    }
    if preserve_metadata:
        payload.update(_feature_metadata_from_row(entry))
        if "Key" in entry:
            payload["Key"] = entry.get("Key")
    return payload


def _validate_points(points, preserve_metadata=False):
    """Validate and process point geometries."""
    valid_data = []
    bad_sources = []
    
    def is_valid_lat_long(lat, long):
        return -90 <= lat <= 90 and -180 <= long <= 180
    
    for entry in points:
        try:
            geometry = entry['geometry']
            
            # Handle list-wrapped geometry
            if isinstance(geometry, list):
                if len(geometry) == 1:
                    geometry = geometry[0]
                else:
                    raise ValueError("Multiple geometries found where one was expected")
            
            # Parse JSON string
            if isinstance(geometry, str):
                if geometry.count("{") != geometry.count("}"):
                    raise ValueError("Missing brackets in geometry JSON")
                geometry = json.loads(geometry)
            
            # Validate structure
            if 'coordinates' not in geometry:
                raise ValueError("Coordinates missing in geometry JSON")
            
            # Validate Point
            if geometry['type'] == 'Point':
                long, lat = geometry['coordinates']
                if not is_valid_lat_long(lat, long):
                    raise ValueError(f"Out of range latitude/longitude: {lat}, {long}")
            
            # Validate MultiPoint
            elif geometry['type'] == 'MultiPoint':
                for coord in geometry['coordinates']:
                    long, lat = coord
                    if not is_valid_lat_long(lat, long):
                        raise ValueError(f"Out of range latitude/longitude in MultiPoint: {lat}, {long}")
            else:
                raise ValueError(f"Unsupported geometry type: {geometry['type']}")
            
            entry['geometry'] = geometry
            valid_data.append(entry)
            
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            bad_source = {
                'source': entry.get('source', 'Unknown'),
                'key': entry.get('key', 'Unknown'),
                'error': str(e)
            }
            if preserve_metadata:
                bad_source.update(_feature_metadata_from_row(entry))
            bad_sources.append(bad_source)
    
    # Flatten MultiPoint geometries
    if valid_data:
        point_list = []
        for entry in valid_data:
            if entry['geometry'] == "null":
                continue
            
            if entry['geometry']["type"] == "Point":
                point_list.append(
                    _point_payload(entry, entry['geometry']["coordinates"], preserve_metadata)
                )
            elif entry['geometry']["type"] == "MultiPoint":
                for coord in entry['geometry']['coordinates']:
                    point_list.append(_point_payload(entry, coord, preserve_metadata))
        
        if point_list:
            return point_list, bad_sources
    
    return points, bad_sources
