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
                WITH a, r, d
                CALL apoc.when(
                    r.country IS NOT NULL AND NOT r.country = [],
                    'RETURN custom.getName($id) AS name',
                    'RETURN null AS name',
                    {id: r.country}
                ) YIELD value AS country
                CALL apoc.when(
                    r.district IS NOT NULL AND NOT r.district = [],
                    'RETURN custom.getName($id) AS name',
                    'RETURN null AS name',
                    {id: r.district}
                ) YIELD value AS district
                CALL apoc.when(
                    r.language IS NOT NULL AND NOT r.language = [],
                    'RETURN custom.getGlot($id) AS name',
                    'RETURN null AS name',
                    {id: r.language}
                ) YIELD value AS language
                CALL apoc.when(
                    r.religion IS NOT NULL AND NOT r.religion = [],
                    'RETURN custom.getName($id) AS name',
                    'RETURN null AS name',
                    {id: r.religion}
                ) YIELD value AS religion
                WITH a, r, d, country, district, language, religion
                RETURN 
                    a.CMName AS CMName,
                    apoc.text.join([i IN [
                        custom.anytoList(collect(split(country.name, ', ')), true),
                        custom.anytoList(collect(split(district.name, ', ')), true)
                    ] WHERE NOT i = ''], ', ') AS Location,
                    a.CMID AS CMID,
                    apoc.text.join([i IN labels(a) WHERE NOT i = 'CATEGORY'], ', ') AS Domains,
                    custom.anytoList(collect(split(language.name, ', ')), true) AS Languages,
                    custom.anytoList(collect(split(religion.name, ', ')), true) AS Religions
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
                    r.country AS countryID,
                    r.district AS districtID,
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
                CALL apoc.when(
                    countryID IS NOT NULL,
                    'RETURN custom.getName($id) AS country',
                    'RETURN null AS country',
                    {id: countryID}
                ) YIELD value AS country
                CALL apoc.when(
                    districtID IS NOT NULL,
                    'RETURN custom.getName($id) AS district',
                    'RETURN null AS district',
                    {id: districtID}
                ) YIELD value AS district
                RETURN 
                    apoc.text.join(Name, '; ') AS Name,
                    apoc.text.join([i IN [country.country, district.district] 
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
                    Link
                ORDER BY Source, Name
            ''',
            
            'categories': """
                UNWIND $cmid AS cmid
                MATCH (a:ADM0 {CMID: cmid})-[r:DISTRICT_OF]-(c)
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
                WITH a 
                CALL apoc.when(
                    a.District IS NOT NULL,
                    'RETURN custom.getName($id) AS name',
                    'RETURN null AS name',
                    {id: a.District}
                ) YIELD value AS Location
                RETURN 
                    a.CMName AS CMName,
                    custom.anytoList(collect(Location.name), true) AS Location,
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
                UNWIND r.label AS Domain
                WITH Domain, c, r
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
                UNWIND b.label AS Domain
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
    
def exploreGeometry(database, cmid):
    """
    Explore and process geometry data for a given CMID.
    
    Args:
        database: Database identifier
        cmid: Content Management ID
        
    Returns:
        dict: Dictionary containing polygons, points, dataset points, sources, and errors
    """
    driver = getDriver(database)
    
    # Get raw data from Neo4j
    polygons = getPolygon(cmid, driver)
    points = getPoints(cmid, driver)
    dataset_points = getDatasetPoints(cmid, driver)
    
    # Transform dataset points
    transformed_points = _transform_dataset_points(dataset_points)
    
    # Process polygons
    polygons, polysources = _process_polygons(polygons)
    
    # Validate and process points
    points, bad_sources = _validate_points(points)
    
    return {
        "polygons": polygons,
        "points": points,
        "datasetpoints": transformed_points,
        "polysources": polysources,
        "badsources": bad_sources
    }


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


def _process_polygons(polygons):
    """Process polygon geometries into GeoJSON format."""
    polysources = []
    
    if not polygons or len(polygons) == 0:
        return polygons, polysources
    
    if len(polygons) > 1:
        # Multiple polygons - create FeatureCollection
        poly = {"type": 'FeatureCollection', "features": []}
        for i, polygon in enumerate(polygons):
            feature = json.loads(polygon['geometry'])
            feature["source"] = polygon['source']
            poly["features"].append(feature)
            polysources.append(polygon['source'])
        return poly, polysources
    else:
        # Single polygon
        poly = json.loads(polygons[0]['geometry'])
        poly["source"] = polygons[0]['source']
        polysources.append(polygons[0]['source'])
        return [poly], polysources


def _validate_points(points):
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
            bad_sources.append({
                'source': entry.get('source', 'Unknown'),
                'key': entry.get('key', 'Unknown'),
                'error': str(e)
            })
    
    # Flatten MultiPoint geometries
    if valid_data:
        point_list = []
        for entry in valid_data:
            if entry['geometry'] == "null":
                continue
            
            if entry['geometry']["type"] == "Point":
                point_list.append({
                    "cood": entry['geometry']["coordinates"],
                    "source": entry["source"]
                })
            elif entry['geometry']["type"] == "MultiPoint":
                source = entry['source']
                for coord in entry['geometry']['coordinates']:
                    point_list.append({
                        'cood': coord,
                        "source": source
                    })
        
        if point_list:
            return point_list, bad_sources
    
    return points, bad_sources
