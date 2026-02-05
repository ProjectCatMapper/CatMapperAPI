import json
import pandas as pd
from .utils import *


def convert_to_multipoint(geojson_string):
    """
    Convert a semicolon-separated string of GeoJSON Point objects into a single MultiPoint geometry.

    Args:
        geojson_string (str): Semicolon-separated GeoJSON Points.

    Returns:
        dict: A valid GeoJSON MultiPoint object.
    """
    # Split the string by semicolon
    geojson_parts = geojson_string.split(";")

    coordinates = []  # List to hold coordinates

    # Parse each part and extract coordinates
    for part in geojson_parts:
        part = part.strip()  # Remove leading/trailing whitespace
        if part:  # Skip empty parts
            point = json.loads(part)
            if point.get("type") != "Point":
                raise ValueError("Input contains a non-Point geometry.")
            coordinates.append(point["coordinates"])

    # Construct the MultiPoint GeoJSON
    multipoint_geojson = {
        "type": "MultiPoint",
        "coordinates": coordinates
    }

    return json.dumps(multipoint_geojson)


def correct_geojson(CMID, database):
    """
    Correct the geoCoords property of a node in the database.

    Args:
        CMID (str): The CMID of the node to correct.
        database (str): The name of the database.

    Returns:
        dict: A dictionary containing the corrected GeoJSON.
    """
    try:
        driver = getDriver(database)

        # Query to fetch the node's geoCoords property
        query = "unwind $CMID as cmid MATCH (c:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET) where r.geoCoords contains ';' RETURN c.CMID as CMID, d.CMID as datasetID, r.Key as Key, r.geoCoords AS geoCoords"

        # Execute the query
        result = getQuery(query, driver, params={'CMID': CMID})
        result = pd.DataFrame(result)
        result = result[result['geoCoords'].str.contains(";", na=False)]

        if result.empty:
            return None
        if result[['geoCoords']].isnull().values.any():
            return None

        result['geoCoords'] = result['geoCoords'].apply(
            lambda x: convert_to_multipoint(x) if ';' in x else json.dumps(json.loads(x)))

        query = "unwind $rows as row MATCH (c:CATEGORY {CMID: row.CMID})<-[r:USES {Key: row.Key}]-(d:DATASET {CMID: row.datasetID}) set r.geoCoords = row.geoCoords return count(*) as count"

        result = result.to_dict(orient='records')

        count = getQuery(query, driver, params={'rows': result})
        print(count)

        return result

    except Exception as e:
        return f"Unable to correct GeoJSON properties: {str(e)}"


def getPolygon(CMID, driver, simple=True):
    try:
        query = """
    match (:CATEGORY {CMID: $CMID})<-[r:USES]-(d:DATASET) where not r.geoPolygon is null 
    return distinct r.geoPolygon as geomID, d.shortName as source
    """
        result = getQuery(query, driver, params={"CMID": CMID})

        driverGIS = getDriver('gisdb')
        if simple == True:
                query = """
    unwind $rows as row 
    unwind row.geomID as geomID
    unwind row.source as source
    with geomID, source
    match (g:GEOMETRY)
    where g.geomID = geomID
    return source, coalesce(g.simplified,g.geometry) as geometry, g.simplified is not null as simple
    """
        else:
                query = """
    unwind $rows as row 
    unwind row.geomID as geomID
    unwind row.source as source
    with geomID, source
    match (g:GEOMETRY) 
    where g.geomID = geomID
    return source, g.geometry as geometry
    """
            # query = "unwind $rows as row return row"
        polygons = getQuery(query, driverGIS, params={"rows": result})
        return polygons
    except Exception as e:
        return {"firstResult": result, "query": query, "error": str(e)}


def getPoints(CMID, driver):
    query = "match (:CATEGORY {CMID: $CMID})<-[r:USES]-(d:DATASET) where not r.geoCoords is null return distinct r.geoCoords as geometry, d.shortName as source, r.Key as Key"
    result = getQuery(query, driver, params={"CMID": CMID})
    points = [dict(record) for record in result]
    return points

def getDatasetPoints(CMID, driver):

    query = "match (c:CATEGORY)<-[r:USES]-(:DATASET {CMID: $CMID}) where not r.geoCoords is null return distinct r.geoCoords as geometry, c.CMName as source"
    result = getQuery(query, driver, params={"CMID": CMID})
    points = [dict(record) for record in result]
    return points


def getRelations(CMID, driver):
    query = "match ({CMID: $CMID})-[r]-() return distinct type(r) as relation"
    result = getQuery(query, driver, params={"CMID": CMID})
    return result
