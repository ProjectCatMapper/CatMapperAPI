from flask import request, send_file, send_from_directory, jsonify, render_template, make_response, send_from_directory
import os
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
from collections import defaultdict
from CM import *
from CMroutes import *

app = create_app()


@app.route("/")
def root():
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('api.html'), 200, headers)


@app.route('/apidocs/')
def swagger_ui():
    return send_from_directory('static/swagger-ui', 'index.html')


@app.route('/swagger.yaml')
def swagger_yaml():
    return send_file('swagger.yml', mimetype='application/yaml')


@app.route("/category", methods=['GET'])
def catm():

    cmid = request.args.get('cmid')
    database = request.args.get('database')

    driver = getDriver(database)

    relnames = []
    relations = ["USES", "CONTAINS", "DISTRICT_OF",
                 "LANGUOID_OF", "RELIGION_OF"]
    q = "match (a) where a.CMID = '"+cmid + \
        "' return elementId(a) as id,labels(a) as label"
    # q = '''unwind $cmid as cmid unwind $relation as relation match (a)-[r]-(b) where a.CMID = cmid and type(r) = relation with b unwind labels(b) as l with l where not l = 'CATEGORY' return distinct l as label'''
    session = driver.session()
    labels = session.run(q)
    labels = labels.data()
    if labels:
        labels = str(labels[0]['label'][-1])
    else:
        labels = ""
    q = "MATCH (n:"+labels+" {CMID:'"+cmid + \
        "'})-[r]-(n1) RETURN DISTINCT TYPE(r) as label"
    rel_name = session.run(q).data()
    for i in rel_name:
        if i['label'] in relations:
            relnames.append(i['label'])
    driver.close()

    driver = getDriver(database)

    if str.lower(database) == "sociomap":
        label = re.search("^SM", cmid)
    elif str.lower(database) == "archamap":
        label = re.search("^AM", cmid)
    else:
        pass

    if label is not None:
        label = "CATEGORY"
    else:
        label = "DATASET"

    if label == "CATEGORY":
        qInfo = '''
    unwind $cmid as cmid match (a)<-[r:USES]-(d:DATASET)
    where a.CMID = cmid with a,r,d
    call apoc.when(r.country is not null and not r.country = [],'return custom.getName($id) as name','return null as name',{id:r.country}) yield value as country
    call apoc.when(r.district is not null and not r.district = [],'return custom.getName($id) as name','return null as name',{id:r.district}) yield value as district
    call apoc.when(r.language is not null and not r.language = [],'return custom.getGlot($id) as name','return null as name',{id:r.language}) yield value as language
    call apoc.when(r.religion is not null and not r.religion = [],'return custom.getName($id) as name','return null as name',{id:r.religion}) yield value as religion
    with a,r,d, country, district, language, religion
    return a.CMName as CMName, apoc.text.join([i in [custom.anytoList(collect(split(country.name,', ')),true),custom.anytoList(collect(split(district.name,', ')),true)] where not i = ''],', ') as Location,
    a.CMID as CMID, apoc.text.join([i in labels(a) where not i = 'CATEGORY'],', ') as Domains,
    custom.anytoList(collect(split(language.name,', ')),true) as Languages, custom.anytoList(collect(split(religion.name,', ')),true) as Religions
    '''
    # case when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is not null then custom.getMinYear(r.yearStart) + '-' + custom.getMaxYear(r.yearEnd)
    # when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is null then custom.getMinYear(r.yearStart) + '-present'
    # when custom.getMinYear(r.yearStart) is null and custom.getMaxYear(r.yearEnd) is not null then custom.getMaxYear(r.yearEnd)
    # else null
    # end as timeSpan
    # custom.anytoList(collect(split(timeSpan,', ')),true) as `Date range`
        qSamples = '''
   UNWIND $cmid AS cmid
MATCH (a)<-[r:USES]-(d:DATASET)
WHERE a.CMID = cmid

WITH a, d, r, d.project AS Source, d.CMID AS datasetID, d.DatasetVersion AS Version

WITH a, d, r, Source, datasetID, Version,
     COLLECT(DISTINCT r.categoryType) AS allCTypes

WITH a, d, r, Source, datasetID, Version, allCTypes,
     SIZE([x IN allCTypes WHERE x IS NOT NULL AND x <> '']) AS cTypeCount

WITH r, d, Source, datasetID, Version, cTypeCount,
     r.Name AS Name, r.country AS countryID, r.district AS districtID,
     r.url AS Link, r.recordStart AS recordStart, r.recordEnd AS recordEnd, r.yearStart as yearStart, r.yearEnd as yearEnd,
     toInteger(r.populationEstimate) AS Population, toInteger(r.sampleSize) AS `Sample size`,
     r.type AS type,
     CASE
       WHEN r.populationEstimate IS NULL OR r.populationEstimate = 0 THEN null
       WHEN cTypeCount >= 1 THEN r.categoryType
       ELSE null
     END AS cType

CALL apoc.when(countryID IS NOT NULL,
    'RETURN custom.getName($id) AS country',
    'RETURN null AS country',
    {id: countryID}) YIELD value AS country

CALL apoc.when(districtID IS NOT NULL,
    'RETURN custom.getName($id) AS district',
    'RETURN null AS district',
    {id: districtID}) YIELD value AS district

RETURN 
    apoc.text.join(Name, ', ') AS Name,
    apoc.text.join([i IN [country.country, district.district] WHERE i IS NOT NULL AND i <> ''], ', ') AS Location,
    type AS Type,
    recordStart AS `rStart`,
    recordEnd AS `rEnd`,    
    yearStart AS `yStart`,
    yearEnd AS `yEnd`,
    Population AS `Population est.`,
    `Sample size` AS `Sample size`,
    Source AS `Source`,
    'https://catmapper.org/' + $database + '/' + datasetID AS `link2`,
    Version,
    cType,
    Link
ORDER BY Source, Name
    '''
    # apoc.text.join([
    #     coalesce(toString(recordStart), ''),
    #     coalesce(toString(recordEnd), '')
    # ], '-') AS `Time span`,

        qCategories = """
unwind $cmid as cmid
match (a:ADM0 {CMID: cmid})-[:DISTRICT_OF]-(c:CATEGORY)
unwind labels(c) as Domain with Domain, count(*) as Count
return distinct Domain, Count order by Domain
"""

    else:
        qInfo = '''
    unwind $cmid as cmid
    match (a:DATASET)
    where a.CMID = cmid
    with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
    'return null as name',{id:a.District}) yield value as Location
    return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID,
    labels(a) as Domains, a.parent as Parent,
      a.DatasetCitation as Citation, "<a href ='" + a.DatasetLocation + "' target='_blank' >" + a.DatasetLocation +"</a>" as `Dataset Location`,
        a.ApplicableYears as `Applicable Years`,
        custom.getName(a.foci) as Foci,
        a.Note as Note
    '''
        qSamples = None

        qCategories = """
UNWIND $cmid AS cmid
MATCH (d:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY)

// Unwind labels per relationship
UNWIND r.label AS Domain

WITH Domain, c, r

// Count distinct nodes per Domain and collect all uses relationships per Domain
WITH Domain, 
     COUNT(DISTINCT c) AS distinctNodeCount,   // distinct CATEGORY nodes per domain
     COLLECT(r) AS usesRels                    // all :USES rels for this domain

WITH Domain, distinctNodeCount, usesRels, size(usesRels) AS totalUses

RETURN Domain, distinctNodeCount AS Count, totalUses as TotalUses
ORDER BY Domain

"""
    with driver.session() as session:
        info = session.run(qInfo, cmid=cmid)
        info = [dict(record) for record in info]
        if qCategories is None:
            categories = []
        else:
            categories = session.run(qCategories, cmid=cmid)
            categories = [dict(record) for record in categories]
        if qSamples is not None:
            samples = session.run(qSamples, cmid=cmid, database=database)
            samples = [dict(record) for record in samples]
        else:
            samples = []
        driver.close()

    if "Dataset Location" in info[0]:
        print(info[0]["Dataset Location"])
        if info[0]["Dataset Location"]:
            soup = BeautifulSoup(info[0]["Dataset Location"], 'html.parser')
            link_tag = soup.find('a')
            if link_tag:
                info[0]["Dataset Location"] = link_tag.get('href')

    polygons = getPolygon(cmid, driver)
    points = getPoints(cmid, driver)
    dataset_points = getDatasetPoints(cmid,driver)

    transformed_points = []

    for point in dataset_points:
        try:
            geom = json.loads(point["geometry"])
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

    with open('poly.json', 'w', encoding='utf-8') as f:
        json.dump(polygons, f, ensure_ascii=False, indent=4)

    polysources = []

    if len(polygons) != 0:
        # polygons != "" or polygons != [] or
        if len(polygons) > 1:
            poly = {"type": 'FeatureCollection', "features": []}
            for i in range(0, len(polygons)):
                poly["features"].append(json.loads(polygons[i]['geometry']))
                poly["features"][i]["source"] = (polygons[i]['source'])
                polysources.append(polygons[i]['source'])
            polygons = poly
            # polygons = json.loads(polygons)
        else:
            temp = polygons
            polygons = [json.loads(polygons[0]['geometry'])]
            polygons[0]["source"] = (temp[0]['source'])
            polysources.append(temp[0]['source'])
            temp = None

    with open('new.json', 'w', encoding='utf-8') as f:
        json.dump(polygons, f, ensure_ascii=False, indent=4)

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(points, f, ensure_ascii=False, indent=4)
    
    with open('data1.json', 'w', encoding='utf-8') as f:
        json.dump(transformed_points, f, ensure_ascii=False, indent=4)

    valid_data = []

    bad_sources = []

    def is_valid_lat_long(lat, long):
        return -90 <= lat <= 90 and -180 <= long <= 180

    for entry in points:
        try:
            geometry = entry['geometry']

            if isinstance(geometry, list):
                if len(geometry) == 1:
                    geometry = geometry[0]
                else:
                    raise ValueError(
                        "Multiple geometries found where one was expected")

            if isinstance(geometry, str):
                if geometry.count("{") != geometry.count("}"):
                    raise ValueError("Missing brackets in geometry JSON")

                geometry = json.loads(geometry)

            if 'coordinates' not in geometry:
                raise ValueError("Coordinates missing in geometry JSON")

            if geometry['type'] == 'Point':
                long, lat = geometry['coordinates']
                if not is_valid_lat_long(lat, long):
                    raise ValueError(
                        f"Out of range latitude/longitude: {lat}, {long}")
            elif geometry['type'] == 'MultiPoint':
                for coord in geometry['coordinates']:
                    long, lat = coord
                    if not is_valid_lat_long(lat, long):
                        raise ValueError(
                            f"Out of range latitude/longitude in MultiPoint: {lat}, {long}")
            else:
                raise ValueError(
                    f"Unsupported geometry type: {geometry['type']}")

            entry['geometry'] = geometry
            valid_data.append(entry)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            bad_sources.append({'source': entry.get(
                'source', 'Unknown'), 'key': entry.get('key', 'Unknown'), 'error': str(e)})

    if len(valid_data) > 0:
        point = []
        for i in range(0, len(valid_data)):
            if valid_data[i]['geometry'] == "null":
                continue
            if valid_data[i]['geometry']["type"] != "MultiPoint":
                point.append(
                    {"cood": valid_data[i]['geometry']["coordinates"][::-1], "source": valid_data[i]["source"]})
            else:
                temp = valid_data[i]
                source = temp['source']
                for j in range(0, len(temp['geometry']['coordinates'])):
                    point.append(
                        {'cood': temp['geometry']['coordinates'][j][::-1], "source": source})
        if point:
            points = point

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(points, f, ensure_ascii=False, indent=4)

    relnames = sorted(relnames, key=custom_sort)

    if "Languages" in info[0]:
        if info[0]['Languages'][:1] == ",":
            info[0]['Languages'] = info[0]['Languages'][2:].strip()
        if info[0]['Languages'][-2:-1] == ",":
            info[0]['Languages'] = info[0]['Languages'][:-2].strip()

    if "Location" in info[0]:
        if info[0]['Location'][-2:-1] == ",":
            info[0]['Location'] = info[0]['Location'][:-2].strip()

    # if "Date range" in info[0]:
    #     if info[0]["Date range"] == "-":
    #         del info[0]["Date range"]
    
    # for obj in samples:
    #     ystart = obj.get("ystart")
    #     yend = obj.get("yend")
    #     if ystart is None and yend is None:
    #         obj["time_span_2"] = None
    #     else:
    #         obj["time_span_2"] = f"{ystart}-{yend}"
    #     obj.pop("ystart", None)
    #     obj.pop("yend", None)


    return jsonify({
        "info": info[0],
        "samples": samples,
        "categories": categories,
        "polygons": polygons,
        "points": points,
        "datasetpoints" : transformed_points,
        "relnames": relnames,
        "polysource": polysources,
        "badsources": bad_sources
    })


@app.route("/network", methods=['GET'])
def net():
    p0 = request.args.get('value')
    p1 = request.args.get('cmid')
    p2 = request.args.get('relation')
    database = request.args.get('database')
    if not database:
        database = "sociomap"
    driver_neo4j = getDriver(database)
    session = driver_neo4j.session()
    q = "MATCH (n:"+p0+" {CMID:'"+p1+"'})-[r:" + \
        p2+"]-(OtherNodes) RETURN n,r,OtherNodes"
    r = session.run(q)
    resultnet = r.data()
    return resultnet


@app.route("/explore", methods=['GET'])
def getExplore():

    try:
        cmid = request.args.get('cmid')
        database = request.args.get('database')

        if str.lower(database) == "sociomap":
            label = re.search("^SM", cmid)
        elif str.lower(database) == "archamap":
            label = re.search("^AM", cmid)
        else:
            pass

        driver = getDriver(database)

        if label is not None:
            label = "CATEGORY"
        else:
            label = "DATASET"

        if label == "CATEGORY":
            qInfo = '''
    unwind $cmid as cmid match (a)<-[r:USES]-(d:DATASET)
    where a.CMID = cmid with a,r,d
    call apoc.when(r.country is not null and not r.country = [],'return custom.getName($id) as name','return null as name',{id:r.country}) yield value as country
    call apoc.when(r.district is not null and not r.district = [],'return custom.getName($id) as name','return null as name',{id:r.district}) yield value as district
    call apoc.when(r.language is not null and not r.language = [],'return custom.getGlot($id) as name','return null as name',{id:r.language}) yield value as language
    call apoc.when(r.religion is not null and not r.religion = [],'return custom.getName($id) as name','return null as name',{id:r.religion}) yield value as religion
    with a,r,d, country, district, language, religion,
    case when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is not null then custom.getMinYear(r.yearStart) + '-' + custom.getMaxYear(r.yearEnd)
    when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is null then custom.getMinYear(r.yearStart) + '-present'
    when custom.getMinYear(r.yearStart) is null and custom.getMaxYear(r.yearEnd) is not null then custom.getMaxYear(r.yearEnd)
    else null
    end as timeSpan
    return a.CMName as CMName, apoc.text.join([i in [custom.anytoList(collect(split(country.name,', ')),true),
    custom.anytoList(collect(split(district.name,', ')),true)] where not i = ''],', ') as Location,
    a.CMID as CMID, apoc.text.join([i in labels(a) where not i = 'CATEGORY'],', ') as Domains,
    custom.anytoList(collect(split(language.name,', ')),true) as Languages, custom.anytoList(collect(split(religion.name,', ')),true) as Religions,
    custom.anytoList(collect(split(timeSpan,', ')),true) as `Date range`
    '''
            qSamples = '''
    unwind $cmid as cmid
    match (a)<-[r:USES]-(d:DATASET)
    where a.CMID = cmid
    with custom.anytoList(collect(r.Name),true) as Name, r.country as countryID,
    r.district as districtID, d.project as Source, d.CMID as datasetID, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd,
    toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
    call apoc.when(countryID is not null,'return custom.getName($id) as country','return null',{id:countryID}) yield value
    with Name, value as country, districtID, Source, datasetID, Version, Link, recordStart, recordEnd, Population, `Sample size`, type
    call apoc.when(districtID is not null,'return custom.getName($id) as district','return null',{id:districtID}) yield value
    with Name, country, value as district, Source, datasetID, Version, Link, recordStart, recordEnd, Population, `Sample size`, type
    return Name, apoc.text.join([i in [custom.anytoList(collect(country.country),true),custom.anytoList(collect(district.district),true)] where not i = ''],', ') as Location, type as Type,
    apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),
    toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),
    toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Population est.`,
    apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`, '<a href="/app/' + $database + '/?main=view&explore=' + datasetID + '" target="_blank" >' + Source + '</a>' as Source,
    Version, Link order by `Time span`, Source, Name
    '''
            qCategories = """
unwind $cmid as cmid
match (a:ADM0 {CMID: cmid})-[:DISTRICT_OF]->(c:CATEGORY)
unwind labels(c) as Domain
with distinct c, apoc.coll.toSet(apoc.coll.flatten(collect(Domain),true)) as Domains
unwind Domains as Domain
with Domain, count(*) as Count
return distinct Domain, Count order by Domain
"""

        else:
            qInfo = '''
    unwind $cmid as cmid
    match (a:DATASET)
    where a.CMID = cmid
    with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
    'return null as name',{id:a.District}) yield value as Location
    return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID,
    labels(a) as Domains, a.parent as Parent, a.DatasetCitation as Citation,
      "<a href ='" + a.DatasetLocation + "' target='_blank' >" + a.DatasetLocation +"</a>" as `Dataset Location`,
        a.ApplicableYears as `Applicable Years`,
        custom.getName(a.foci) as Foci,
        a.Note as Note
    '''
            qSamples = None
            qCategories = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY)
unwind r.label as Domain
with distinct c, apoc.coll.toSet(apoc.coll.flatten(collect(Domain),true)) as Domains
unwind Domains as Domain
with Domain, count(*) as Count
return distinct Domain, Count order by Domain
"""

        with driver.session() as session:
            info = session.run(qInfo, cmid=cmid)
            info = [dict(record) for record in info]
            if qCategories is None:
                categories = []
            else:
                categories = session.run(qCategories, cmid=cmid)
                categories = [dict(record) for record in categories]
            if qSamples is not None:
                samples = session.run(qSamples, cmid=cmid, database=database)
                samples = [dict(record) for record in samples]
            else:
                samples = []
            driver.close()

        polygons = getPolygon(cmid, driver)
        points = getPoints(cmid, driver)

        if info is None:
            raise Exception("No results for info")
        if samples is None:
            raise Exception("No results for samples")

        return jsonify({
            "info": info,
            "samples": samples,
            "polygons": polygons,
            "points": points,
            "categories": categories
        })

    except Exception as e:
        return "Error returning results: " + str(e), 500

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


@app.route("/uploadInputNodes", methods=['GET', 'POST'])
def upload_API():
    try:
        data = request.get_data()
        data = json.loads(data)
        df = data.get("df")
        database = unlist(data.get("database"))
        formData = unlist(data.get("formData"))
        label = formData["domain"]
        if label == "ANY DOMAIN":
            label = "CATEGORY"
        if label == "AREA":
            label = "DISTRICT"
        datasetID = formData["datasetID"]
        CMName = formData["cmNameColumn"]
        Name = formData["categoryNamesColumn"]
        altNames = formData["alternateCategoryNamesColumn"]
        CMID = formData["cmidColumn"]
        Key = formData["keyColumn"]

        linkProperties = data.get("linkContext")
        if not linkProperties:
            linkProperties = None

        if data.get("addoptions")["district"] == False:
            addDistrict = False
        else:
            addDistrict = True

        if data.get("addoptions")["recordyear"] == False:
            addRecordYear = False
        else:
            addRecordYear = True

        user = data.get("user")

        if data.get("so") == "advanced":

            uploadOption = data.get("ao")

            dfpd = pd.DataFrame(df)
            required = ["CMName", "Name", "CMID",
                        "label", "altNames", "Key", "datasetID"]
            key_cols = {}
            for key in required:
                if key in dfpd.columns.to_list():
                    key_cols[key] = key
                else:
                    key_cols[key] = None

            nodeProperties = None
            if 'label' in dfpd.columns:
                if dfpd['label'][0] == "DATASET":
                    nodeProperties = linkProperties
                    linkProperties = None

            response = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption=uploadOption,
                formatKey=False,
                nodeProperties=nodeProperties,
                linkProperties=linkProperties,
                user=user,
                addDistrict=addDistrict,
                addRecordYear=addRecordYear,
                geocode=False,
                batchSize=1000)
        else:

            if not label:
                raise Exception("Must specify a domain")
            df = pd.DataFrame(df)
            df['label'] = label
            df['datasetID'] = datasetID
            if not Name in df.columns:
                df['Name'] = df[CMName]
                Name = "Name"
            if not CMID in df.columns:
                df['CMID'] = ""
                CMID = "CMID"
            df.rename(columns={CMName: "CMName", CMID: "CMID", Name: "Name",
                      Key: "Key", altNames: "altNames"}, inplace=True)
            df = df.to_dict(orient='records')
            # return {"Name":Name, "CMID":CMID,"altNames":altNames,"Key":Key,"user":user,"overwriteProperties":overwriteProperties,"updateProperties":updateProperties,"addDistrict":addDistrict,"addRecordYear":addRecordYear}
            response = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption="add_uses",
                formatKey=True,
                nodeProperties=None,
                linkProperties=None,
                user=user,
                addDistrict=False,
                addRecordYear=False,
                geocode=False,
                batchSize=1000)

        if isinstance(response, pd.DataFrame):
            n = len(response)
            response_dict = response.to_dict(orient='records')
            return {"message": f"Upload completed for {n} row(s)", "file": response_dict}
        # else:
        #     return "Error!! Check your file."

    except Exception as e:
        log_file = f'log/{user}uploadProgress.txt'
        full_log = []
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                full_log = file.readlines()
        else:
            full_log.append("Log file not found.")

        response_data = {
            "error": f"Upload error - {str(e)}",
            "full_log": full_log
        }

        return json.dumps(response_data), 500


@app.route('/networks', methods=['GET'])
def getNetwork():
    try:
        cmid = request.args.get('cmid')
        cmid = re.split(",", cmid)
        domain = request.args.get('domain')
        if domain is not None:
            domain = re.split(",", domain)
        else:
            domain = ["CATEGORY", "DATASET"]

        endcmid = request.args.get('endcmid')
        relation = request.args.get('relation')
        if relation is None:
            relation = "USES"
        database = request.args.get('database')

        driver = getDriver(database)

        if endcmid is not None:
            cypher_query = """
unwind $cmid as cmid unwind $endcmid as endcmid unwind $relation as relation
MATCH (a)
WHERE a.CMID = cmid
optional match (a)-[r]-(e)
where type(r) = relation and e.CMID = endcmid and
not isEmpty([label IN labels(e)
WHERE label IN apoc.coll.flatten([$domain],true)])
with collect(distinct a) as a, r, e
return a, collect(distinct r) as r, collect(distinct e) as e
"""
        else:
            cypher_query = """
unwind $cmid as cmid unwind $relation as relation MATCH (a)
WHERE a.CMID = cmid
optional match (a)-[r]-(e)
where type(r) = relation and
not isEmpty([label IN labels(e)
WHERE label IN apoc.coll.flatten([$domain],true)])
with collect(distinct a) as a, r, e limit 10
return a, collect(distinct r) as r, collect(distinct e) as e
"""

        with driver.session() as session:
            # Execute the Cypher queries
            result = session.run(
                cypher_query, cmid=cmid, relation=relation, domain=domain, endcmid=endcmid)
            result = unlist([dict(record) for record in result])
            node = []
            rel = []
            end = []
            a = result['a']
            for record in a:
                node.append({"node": serialize_node(record)})
            r = result['r']
            for record in r:
                rel.append({"relation": serialize_relationship(record)})
            e = result['e']
            for record in e:
                end.append({"end": serialize_node(record)})

        driver.close()
        node = [flatten_json(entry) for entry in node]
        rel = [flatten_json(entry) for entry in rel]
        end = [flatten_json(entry) for entry in end]

        return {"node": node, "relations": rel, "relNodes": end, "query": cypher_query, "params": [{"cmid": cmid, "database": database, "domain": domain, "relation": relation, "endcmid": endcmid}]}
    except Exception as e:
        return str(e), 500


# what about calling this createLinkfile internally? # do we want to?
@app.route('/proposeMergeSubmit', methods=['POST'])
def submit_merge():
    data = request.get_data()
    data = json.loads(data)
    dataset_choices = data.get("datasetChoices")
    dataset_choices = [choice.strip() for choice in dataset_choices.split(",")]
    ncontains = data.get("mergelevel")
    category_label = unlist(data.get("categoryLabel", ""))
    intersection = unlist(data.get("intersection", False))
    database = unlist(data.get('database'))
    criteria = str.lower(unlist(data.get('equivalence')))
    if category_label == "ANY DOMAIN":
        category_label = "CATEGORY"
    elif category_label == "AREA":
        category_label = "DISTRICT"

    result = proposeMerge(dataset_choices=dataset_choices, category_label=category_label,
                          criteria=criteria, database=database, intersection=intersection, ncontains=ncontains)

    return result


@app.route('/downloadMergeCode', methods=['POST'])
def get_merge_code():
    data = request.get_data()
    data = json.loads(data)


@app.route('/joinDatasets', methods=['POST'])
def submitjoinDatasets():
    data = request.get_data()
    data = json.loads(data)
    # print(data)
    database = unlist(data.get("database", ""))
    joinLeft = data.get("joinLeft")
    joinRight = data.get("joinRight")

    result = joinDatasets(database, joinLeft, joinRight)

    return jsonify(result)


@app.route('/validateDatasets', methods=['POST'])
def submitvalidateDatasets():
    data = request.get_data()
    data = json.loads(data)
    database = unlist(data.get("database", ""))
    names = data.get("names").split(",")

    driver = getDriver(database)

    with driver.session() as session:
        for i in names:
            q = """
            MATCH (n:DATASET)
            WHERE n.CMID = $prop
            RETURN COUNT(n) > 0 AS nodeExists
            """
            result = session.run(q, prop=i.strip())
            node_exists = result.single()["nodeExists"]
            if not node_exists:
                return jsonify({"success": False, "message": "Check your Dataset IDs."})
    driver.close()
    return jsonify({"success": True, "message": "All IDs exist."})


# Download template
app.add_url_rule('/merge/template/<database>/<datasetID>', 'get_merge_template',
                 get_merge_template, methods=['GET'])

# Merging syntax -- only accepts R syntax for now
app.add_url_rule('/merge/syntax/<database>', 'get_merge_syntax_route',
                 get_merge_syntax_route, methods=['POST'])


@app.route('/networksjs', methods=['GET'])
def getNetworkjs():
    try:
        cmid = request.args.get('cmid')
        cmid = re.split(",", cmid)
        domain = request.args.get('domain')
        if domain is not None:
            domain = re.split(",", domain)
        else:
            domain = ["CATEGORY", "DATASET"]

        endcmid = request.args.get('endcmid')
        relation = request.args.get('relation')
        if relation is None:
            relation = "USES"
        database = request.args.get('database')

        driver = getDriver(database)

        if endcmid is not None:
            cypher_query = """
unwind $cmid as cmid unwind $endcmid as endcmid unwind $relation as relation
MATCH (a)
WHERE a.CMID = cmid
optional match (a)-[r]-(e)
where type(r) = relation and e.CMID = endcmid and
not isEmpty([label IN labels(e)
WHERE label IN apoc.coll.flatten([$domain],true)])
with collect(distinct a) as a, r, e
return a, collect(distinct r) as r, collect(distinct e) as e
"""
        else:
            cypher_query = """
unwind $cmid as cmid unwind $relation as relation MATCH (a)
WHERE a.CMID = cmid
optional match (a)-[r]-(e)
where type(r) = relation and
not isEmpty([label IN labels(e)
WHERE label IN apoc.coll.flatten([$domain],true)])
with collect(distinct a) as a, r, e
return a, collect(distinct r) as r, collect(distinct e) as e
"""

        with driver.session() as session:
            # Execute the Cypher queries
            result = session.run(
                cypher_query, cmid=cmid, relation=relation, domain=domain, endcmid=endcmid)
            result = unlist([dict(record) for record in result])
            node = []
            rel = []
            end = []
            a = result['a']
            for record in a:
                node.append({"node": serialize_node(record)})
            r = result['r']
            for record in r:
                rel.append({"relation": serialize_relationship(record)})
            e = result['e']
            for record in e:
                end.append({"end": serialize_node(record)})

        driver.close()
        node = [flatten_json(entry) for entry in node]
        rel = [flatten_json(entry) for entry in rel]
        end = [flatten_json(entry) for entry in end]

        return {"node": node, "relations": rel, "relNodes": end, "query": cypher_query, "params": [{"cmid": cmid, "database": database, "domain": domain, "relation": relation, "endcmid": endcmid}]}
    except Exception as e:
        return str(e), 500


@app.route('/search', methods=['GET'])
def getSearch():
    """Search endpoint for explore page
    This endpoint is used for database searches of a single or empty term.
    ---
    parameters:
        - name: database
          in: query
          type: string
          enum: ['SocioMap','ArchaMap']
          required: true
          description: Name of the CatMapper database to search
        - name: term
          in: query
          type: string
          required: false
          description: Search term
        - name: property
          in: query
          type: string
          required: false
          enum: ['Name','CMID','Key']
          description: Property to search by
        - name: domain
          in: query
          type: string
          required: false
          enum: ['DISTRICT','ETHNICITY','STONE']
          default: CATEGORY
          description: Domain containing the category
        - name: yearStart
          in: query
          type: integer
          required: false
          description: Earliest year the category existed or data was collected from (will return a result if category year range intersects with year range)
        - name: yearEnd
          in: query
          type: integer
          required: false
          description: Latest year the category existed or data was collected from
        - name: country
          in: query
          type: string
          required: false
          description: CMID of ADM0 node with DISTRICT_OF tie
        - name: context
          in: query
          type: string
          required: false
          description: CMID of parent node in network
        - name: limit
          in: query
          type: string
          required: false
          default: 10000
          description: Number of results to limit search to
        - name: query
          in: query
          type: string
          enum: ['true','false']
          required: false
          description: Whether to return results or cypher query
    response:
        200:
            description: JSON of search results unless query is true, then a JSON with the cypher query is returned.
            schema:
                type: object
                properties:
                    CMID:
                        type: string
                        example: SM1
                    CMName:
                        type: string
                        example: Afghanistan
                    country:
                        type: array
                        items:
                            type: string
                        example: ["United States of America"]
                    domain:
                        type: array
                        items:
                            type: string
                        example: ["DISTRICT","FEATURE"]
                    matching:
                        type: string
                        example: Afghanistan
                    matchingDistance:
                        type: integer
                        example: 1
        500:
            description: JSON of error
            schema:
            type: string
    """
    try:
        database = request.args.get('database')
        term = request.args.get('term')
        property = request.args.get('property')
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = request.args.get('domain')
        yearStart = request.args.get('yearStart')
        yearEnd = request.args.get('yearEnd')
        context = request.args.get('context')
        country = request.args.get('country')
        limit = request.args.get('limit')
        query = request.args.get('query')

        result = search(
            database,
            term,
            property,
            domain,
            yearStart,
            yearEnd,
            context,
            country,
            limit,
            query
        )
        print(result)
        return result

    except Exception as e:
        return str(e), 500


@app.route('/translate2', methods=['POST'])
def getTranslate2():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get("database"))
        property = unlist(data.get("property"))
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = unlist(data.get("domain"))
        key = unlist(data.get("key"))
        term = unlist(data.get("term"))
        country = unlist(data.get('country'))
        context = unlist(data.get('context'))
        dataset = unlist(data.get('dataset'))
        yearStart = unlist(data.get('yearStart'))
        yearEnd = unlist(data.get('yearEnd'))
        query = unlist(data.get("query"))
        table = data.get("table")
        uniqueRows = data.get("uniqueRows")

        data = translate(
            database=database,
            property=property,
            domain=domain,
            key=key,
            term=term,
            country=country,
            context=context,
            dataset=dataset,
            yearStart=yearStart,
            yearEnd=yearEnd,
            query=query,
            table=table,
            uniqueRows=uniqueRows)

        data_dict = data.to_dict(orient='records')

        return data_dict

    except Exception as e:
        return str(e), 500


@app.route('/geometry', methods=['GET'])
def getGeometry():
    database = request.args.get('database')
    cmid = request.args.get('cmid')
    simple = request.args.get('simple')
    if simple is None:
        simple = True

    driver = getDriver(database)

    polygons = getPolygon(cmid, driver, simple=True)
    points = getPoints(cmid, driver)
    return jsonify({"polygons": polygons, "points": points})


@app.route('/newuser', methods=['POST'])
def getnewuser():
    try:
        from configparser import ConfigParser
        config = ConfigParser()
        config.read('config.ini')
        mail_default = config['MAIL']['mail_default']
        data = request.get_data()
        data = json.loads(data)
        database = data.get("database")
        firstName = data.get("firstName")
        lastName = data.get("lastName")
        email = data.get("email")
        username = data.get("username")
        password = data.get("password")
        password = password_hash(password)
        intendedUse = data.get("intendedUse")

        if database.lower() == "sociomap":
            database = "SocioMap"
        elif database.lower() == "archamap":
            database = "ArchaMap"
        else:
            raise Exception("database must be 'SocioMap' or 'ArchaMap'")

        driver = getDriver("userdb")

        queryExists = """
MATCH (u:USER {access: 'new',username: $username})
WHERE $database IN u.database
return true as exists
"""
        with driver.session() as session:
            result = session.run(
                queryExists, username=username, database=database)
            data = [dict(record) for record in result]
            driver.close()

        if isinstance(data, list) and data and data[0].get("exists") is not None:
            raise Exception(
                "Username already exists. Please try another username.")

        queryExists = """
match (u:USER {email: $email,access:'new'})
WHERE $database IN u.database
return true as exists
"""
        with driver.session() as session:
            result = session.run(queryExists, email=email, database=database)
            data = [dict(record) for record in result]
            driver.close()

        if isinstance(data, list) and data and data[0].get("exists") is not None:
            raise Exception(
                "Account with this email already exists. Please contact admin@catmapper.org to reset password.")

        query = """
match (p:USER) with toInteger(p.userid) + 1 as id order by id desc limit 1
merge (u:USER {username: $username})
on create set u.username = $username,
u.first = $firstName,
u.last = $lastName,
u.email = $email,
u.access = "pending",
u.log = [toString(datetime()) + ": created user via API", toString(datetime()) + \
                  ": created autoapproved via API during workshop registration"],
u.password = $password,
u.userid = toString(id),
u.role = 'user',
u.intendedUse = $intendedUse,
u.database = split($database,"|")
return u.userid as userid
"""

        with driver.session() as session:
            result = session.run(query, firstName=firstName, lastName=lastName, email=email,
                                 password=password, username=username, intendedUse=intendedUse, database=database)
            data = [dict(record) for record in result]
            driver.close()

        body = f"""
Hello,
A new user has just registered.
Name: {firstName} {lastName}
email: {email}
database: {database}
description: {intendedUse}
"""
        sendEmail(mail, subject="New registered user", recipients=[
            "admin@catmapper.org"], body=body, sender=mail_default)

        return jsonify(data)

    except Exception as e:
        # Check for specific error messages
        error_message = str(e)

        if "Account with this email already exists." in error_message:
            return jsonify({"error": str(e)}), 400    # Return 400 Bad Request

        elif "Username already exists" in error_message:
            return jsonify({"error": str(e)}), 400   # Return 400 Bad Request

        else:
            # Default error message
            return jsonify({"error": "please contact admin@catmapper.org. Error:" + error_message}), 500


@app.route('/admin', methods=['GET'])
def getAdmin():
    """
    Retrieve the 'admin.html' template and return it as a response.

    Returns:
    - Response: A Flask response containing the 'admin.html' template.

    Example:
    ```python
    from flask import Flask

    app = Flask(__name__)

    @app.route('/admin')
    def admin_route():
        return getAdmin()
    ```
    """
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('admin.html'), 200, headers)


@app.route('/admin/edit', methods=['GET', 'POST'])
def getAdminEdit():
    from configparser import ConfigParser
    config = ConfigParser()
    config.read('config.ini')
    apikeyEnv = config['DB']['apikey']
    # will not be documented in swagger at this point
    try:
        if request.method == 'GET':
            data = request.args
        elif request.method == "POST":
            data = request.get_data()
            data = json.loads(data)
        else:
            raise Exception("invalid request method")
        database = unlist(data.get('database'))
        fun = unlist(data.get('fun'))
        user = unlist(data.get('user'))
        pwd = unlist(data.get('pwd'))
        apikey = unlist(data.get('apikey'))
        validated = False
        if apikey == apikeyEnv:
            validated = True
        if not validated:
            credentials = login(database, user, pwd)
            if isinstance(credentials, dict) and credentials.get('role') == "admin":
                validated = True
        if not validated:
            raise Exception("User not authorized")

        driver = getDriver(database)
        result = "Nothing returned"
        # if fun == "getUSESrels":
        #     result = getUSESrels(request,driver)
        if fun == "mergeNodes":
            result = mergeNodes(request, driver)
        elif fun == "addIndexes":
            result = addIndexes(driver)
        elif fun == "processUSES":
            CMID = cleanCMID(data.get('CMID'))
            result = processUSES(database=database, CMID=CMID)
        elif fun == "replaceProperty":
            cmid = unlist(data.get('cmid'))
            property = unlist(data.get('property'))
            old = unlist(data.get('old'))
            new = unlist(data.get('new'))
            result = replaceProperty(cmid, property, old, new, database)
        else:
            raise Exception("Function does not exist")
        return result
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        data = str(e)
        return data, 500


@app.route('/dataset', methods=['GET', 'POST'])
def getDataset():
    # to do: document
    try:

        if request.method == 'GET':
            database = unlist(request.args.get('database'))
            cmid = unlist(request.args.get('cmid'))
            domain = unlist(request.args.get('domain'))
            children = unlist(request.args.get('children'))
        elif request.method == "POST":
            data = request.get_data()
            data = json.loads(data)
            database = unlist(data.get('database'))
            cmid = unlist(data.get('cmid'))
            domain = data.get('domain')
            children = unlist(data.get('children'))
        else:
            raise Exception("invalid request method")

        driver = getDriver(database)

        if isinstance(domain, str):
            domain = [domain]

        if domain is None or "ANY DOMAIN" in domain:
            domain = ["CATEGORY"]

        if domain != ["CATEGORY"]:
            labels = getQuery(
                query="MATCH (l:LABEL) RETURN l.label AS label, l.groupLabel AS groupLabel", driver=driver)
            labels = pd.DataFrame(labels)
            # Checking if any item in domain is in the groupLabel list
            if any(i in labels['groupLabel'].values for i in domain):
                domain = list(labels[labels['groupLabel'].isin(
                    domain)]['label'].values) + domain

        if children is not None:
            children = str(children).lower()

        if children is not None and children == "true":
            query = """
            unwind $cmid as cmid
            match (:DATASET {CMID: cmid})-[:CONTAINS*..5]->(d:DATASET) return distinct d.CMID as CMID
            """
            result = getQuery(query=query, driver=driver, type="list")
            if result is not None:
                cmid = [cmid] + result

        if "CATEGORY" in domain:

            query = """
        unwind $cmid as cmid
        match (a:DATASET)-[r:USES]->(b:CATEGORY)
        where a.CMID = cmid
        unwind keys(r) as property
        return distinct a.CMName as datasetName, a.CMID as datasetID,
        b.CMID as CMID, b.CMName as CMName, elementId(r) as relID, property, r[property] as value, custom.getName(r[property]) as property_name
        """

            data = getQuery(query=query, driver=driver, params={
                "cmid": cmid, "domain": domain})

        else:
            query = """
        unwind $cmid as cmid
        match (a:DATASET)-[r:USES]->(b:CATEGORY)
        where a.CMID = cmid and not isEmpty([i in r.label
        where i in apoc.coll.flatten([$domain],true)])
        unwind keys(r) as property
        return distinct a.CMName as datasetName, a.CMID as datasetID,
        b.CMID as CMID, b.CMName as CMName, elementId(r) as relID, property, r[property] as value, custom.getName(r[property]) as property_name
        """

            data = getQuery(query=query, driver=driver, params={
                "cmid": cmid, "domain": domain})

        df = pd.DataFrame(data)

        df.dropna(axis=1, how='all', inplace=True)

        # result = df.to_json(orient="records")
        # return result

        required_columns = ["datasetID", "CMID",
                            "property", "property_name", "relID"]
        if not all(column in df.columns for column in required_columns):
            result = df.to_json(orient="records")
            return result

        df_names = df[required_columns].copy()

        df = df.drop("property_name", axis=1)

        df_names.dropna(subset=["property_name"], how="all", inplace=True)
        df_names = df_names[df_names['property_name'] != '']
        df_names['property'] = df_names['property'].apply(
            lambda x: f"{x}_name")

        df_names = df_names.pivot_table(
            index=["datasetID", "CMID", "relID"], columns='property', values='property_name', aggfunc='first').reset_index()

        cols = [col for col in df.columns if col not in ['property', 'value']]
        df = df.pivot_table(index=cols, columns='property',
                            values='value', aggfunc='first').reset_index()
        if len(df_names) > 0:
            df = pd.merge(df, df_names, on=[
                          'datasetID', 'CMID', 'relID'], how='left')
        dtypes = df.dtypes.to_dict()
        list_cols = []

        for col_name, typ in dtypes.items():
            if typ == 'object' and not df[col_name].empty and isinstance(df[col_name].iloc[0], list):
                list_cols.append(col_name)

        for col in list_cols:
            df[col] = df[col].apply(lambda x: '; '.join(
                map(str, x)) if isinstance(x, list) else x)

        df = df.astype(str)
        df.replace([np.nan, None, "nan"], '', inplace=True)

        df = df.drop('relID', axis=1).copy()

        print(df)

        return df.to_json(orient='records')

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = "Error: " + str(e)
        return result, 500


@app.route('/CMID', methods=['GET'])
def getCMID():
    try:
        database = request.args.get('database')
        cmid = request.args.get('cmid')

        driver = getDriver(database)

        query1 = """
unwind $cmid as cmid
match (c {CMID: cmid})
unwind keys(c) as nodeProperties
return elementId(c) as nodeID, nodeProperties, c[nodeProperties] as nodeValues
"""
        query2 = """
unwind $cmid as cmid
match (c {CMID: cmid})<-[r:USES]-(d)
unwind keys(r) as relProperties
return elementId(r) as relID, relProperties, r[relProperties] as relValues
"""

        with driver.session() as session:
            result = session.run(query1, cmid=cmid)
            node = [dict(record) for record in result]
            result = session.run(query2, cmid=cmid)
            relations = [dict(record) for record in result]
            driver.close()

        grouped_data = defaultdict(dict)

        for entry in relations:
            rel_id = entry['relID']
            prop = entry['relProperties']
            val = entry['relValues']

            if prop in grouped_data[rel_id]:

                if isinstance(grouped_data[rel_id][prop], list):
                    grouped_data[rel_id][prop].extend(
                        val if isinstance(val, list) else [val])
                else:
                    grouped_data[rel_id][prop] = val
            else:
                grouped_data[rel_id][prop] = val

        relations = dict(grouped_data)

        return {"node": node, "relations": relations}

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@app.route('/allDatasets', methods=['GET'])
def getAllDatasets():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        query = """
match (d:DATASET)
return elementId(d) as nodeID,
d.CMName as CMName,
d.CMID as CMID,
d.shortName as shortName,
d.project as project,
d.Unit as Unit,
d.parent as parent,
d.ApplicableYears as ApplicableYears,
d.DatasetCitation as DatasetCitation,
d.District as District,
d.DatasetLocation as DatasetLocation,
d.DatasetVersion as DatasetVersion,
d.DatasetScope as DatasetScope,
d.Subnational as Subnational,
d.Note as Note
"""

        with driver.session() as session:
            result = session.run(query)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@app.route('/linkfile', methods=['GET'])
def getLinkFile():
    try:
        database = request.args.get('database')
        datasets = request.args.get('datasets')
        intersection = request.args.get('intersection')
        domain = request.args.get('domain')

        if not isinstance(datasets, list):
            raise Exception("datasets must be a list")

        if not isinstance(domain, str):
            raise Exception("domain must be a string")

        if not isinstance(intersection, bool):
            raise Exception("intersection must be a boolean")

        driver = getDriver(database)

        query = f"""
match (c:{domain})<-[r:USES]-(d:DATASET) where d.CMID in $datasets
return distinct d.CMName as DatasetName, r.Key as Key, c.CMName as CMName, c.CMID as CMID, apoc.text.join(r.Name,'; ') as Name order by CMName
"""

        with driver.session() as session:
            result = session.run(query, datasets=datasets)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@app.route('/networknodes', methods=['POST'])
def getnetworknodes():
    try:

        data = request.get_data()
        data = json.loads(data)

        database = unlist(data.get('database'))
        cmid = unlist(data.get('cmid'))
        relation = data.get('relation')
        domains = data.get('domains')

        driver = getDriver(database)

        query = """
        unwind $cmid as cmid
        unwind $relation as relation
        match (a)-[r]-(b)
        where a.CMID = cmid and type(r) = relation and ANY(label IN labels(b)
        WHERE label IN apoc.coll.flatten([$domains],true))
        return b.CMID as CMID, b.CMName as Name order by Name limit 1000
"""

        with driver.session() as session:
            result = session.run(
                query, cmid=cmid, relation=relation, domains=domains)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@app.route('/datasetDomains', methods=['POST'])
def getdatasetDomains():
    try:
        data = request.get_data()
        data = json.loads(data)

        database = unlist(data.get('database'))
        cmid = unlist(data.get('cmid'))
        children = unlist(data.get('children'))

        driver = getDriver(database)

        # combine queries
        if children == True:
            query = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[:CONTAINS*..5]->(:DATASET)-[r:USES]->(c:CATEGORY)
with distinct apoc.coll.toSet(apoc.coll.flatten(collect(r.label), true)) as labels
unwind labels as label
return label
"""
        else:
            query = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY)
with distinct apoc.coll.toSet(apoc.coll.flatten(collect(r.label), true)) as labels
unwind labels as label
return label
"""

        data = getQuery(query=query, driver=driver, params={"cmid": cmid})

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@app.route('/foci', methods=['GET'])
def getFoci():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        query1 = """
match (d:DATASET)
where d.foci is not null
unwind d.foci as foci with d, foci
return custom.getName(foci) as Focus, count(distinct d) as Datasets order by Focus
"""

        query2 = """
match (d:DATASET)
where d.foci is not null
optional match (d)-[:USES]->(c:CATEGORY)
with d, c unwind labels(c) as label
with d,c, label
where label in ["DISTRICT","LANGUOID","ETHNICITY","RELIGION"]
unwind d.foci as foci with foci, label, count(distinct c) as n
return custom.getName(foci) as Focus, custom.getDisplayName(label) as domain, n order by Focus, domain
"""

        with driver.session() as session:
            result1 = session.run(query1)
            result2 = session.run(query2)
            data1 = [dict(record) for record in result1]
            data2 = [dict(record) for record in result2]
            driver.close()

        df1 = pd.DataFrame(data1)

        df1.dropna(axis=1, how='all', inplace=True)

        df2 = pd.DataFrame(data2)

        df2.dropna(axis=1, how='all', inplace=True)

        cols = [col for col in df2.columns if col not in ['domain', 'n']]
        df2 = df2.pivot_table(index=cols, columns='domain',
                              values='n', aggfunc='first').reset_index()

        df = df1.join(df2.set_index('Focus'), on='Focus')

        columns_to_convert = df.columns.difference(['Focus'])
        df[columns_to_convert] = df[columns_to_convert].fillna(0)
        df[columns_to_convert] = df[columns_to_convert].astype(int)

        return df.to_json(orient='records')
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@app.route('/createNodes', methods=['POST'])
def createNodesapi():
    try:

        data = request.get_data()
        data = json.loads(data)
        df = data.get('df')
        database = unlist(data.get('database'))
        user = unlist(data.get('user'))
        pwd = unlist(data.get('password'))

        verify = verifyUser(user, pwd)

        if not verify == "verified":
            raise Exception("User is not verified.")

        if not df or len(df) == 0:
            return jsonify({"error": "Data is empty"}), 400

        df = pd.DataFrame(df)

        results = createNodes(df, database, user)

        return results

    except Exception as e:
        result = str(e)
        return result, 500


@app.route('/login', methods=['POST'])
def getLogin():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get('database'))
        user = unlist(data.get('user'))
        password = unlist(data.get('password'))

        credentials = login(database, user, password)

        return credentials

    except Exception as e:
        result = str(e)
        return result, 500


@app.route('/addFoci', methods=['GET'])
def addFoci():
    try:
        database = request.args.get('database')
        datasetID = request.args.get('datasetID')
        foci = request.args.get('foci')

        driver = getDriver(database)

        query = "MATCH (v:VARIABLE {CMID: $foci}) return v.CMID as CMID"
        verifyFoci = getQuery(query, driver, params={"foci": foci})

        query = "MATCH (d:DATASET {CMID: $datasetID}) return d.CMID as CMID"
        verifydb = getQuery(query, driver, params={"datasetID": datasetID})

        if not datasetID in [item["CMID"] for item in verifydb]:
            raise Exception("datasetID does not exist - please check the CMID")

        if foci in [item["CMID"] for item in verifyFoci]:
            query = "MATCH (d:DATASET {CMID: $datasetID}) with d, apoc.coll.toSet(coalesce(d.foci,[]) + $foci) as result set d.foci = result return d.CMID as datasetID, d.foci as foci"
            result = getQuery(query, driver, params={
                "foci": foci, "datasetID": datasetID})
        else:
            raise Exception("foci does not exist - please check the CMID")

        return result

    except Exception as e:
        result = str(e)
        return result, 500


@app.route('/progress', methods=['GET'])
def getProgress():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        query = """
        match (l:LABEL)
        where l.public = 'TRUE' and l.groupLabel = l.label and not l.label = "CATEGORY"
        return l.label as label, l.displayName as newlabel
        """
        domains = getQuery(query=query, driver=driver)
        domains = pd.DataFrame(domains)

        query = """
        match (a)
        unwind labels(a) as label
        with label, count(*) as current
        where label in $labels
        return label, current, 'nodes' as type
        order by label
        union match ()-[r]->()
        where not type(r) in ["IS","MERGING"]
        with type(r) as label, count(*) as current
        return label, current, 'relations' as type
        order by label
        union match (a:DATASET)-[r:USES]->(b)
        unwind labels(b) as label
        with label, count(r) as current
        where label in $labels
        return distinct label, current, 'encodings' as type
        order by label
        """

        data = getQuery(query=query, driver=driver, params={
            "labels": domains["label"]})

        df = pd.DataFrame(data)

        query = """
        match (n:TRANSLATION) where n.table = "display" return n.table as table, n.from as label, n.to as newlabel order by label
        """
        translations = getQuery(query=query, driver=driver)
        translations = pd.DataFrame(translations)
        translations = pd.concat(
            [translations, domains], axis=0, ignore_index=True)
        translations = translations.drop('table', axis=1)

        df = df.merge(translations, on="label", how="inner")
        df = df.drop('label', axis=1)
        df = df.rename(columns={'newlabel': 'label'})

        nodes = df[df['type'] == 'nodes'].copy()
        nodes = nodes.drop('type', axis=1)
        nodes = nodes.to_dict(orient='records')

        encodings = df[df['type'] == 'encodings'].copy()
        encodings = encodings.drop('type', axis=1)
        encodings = encodings.to_dict(orient='records')

        relations = df[df['type'] == 'relations'].copy()
        relations = relations.drop('type', axis=1)
        relations = relations.to_dict(orient='records')

        return {"nodes": nodes, "encodings": encodings, "relations": relations}

    except Exception as e:
        result = str(e)
        return result, 500


@app.route('/test', methods=['GET'])
def test():

    # database = request.args.get('database')

    # driver = getDriver(database)
    # session = driver.session()
    # data = session.run("match (c) return count(*) as count")

    # data = [dict(record) for record in data]

    return {
        'Zebra': ['Row1_Zebra', 'Row2_Zebra', 'Row3_Zebra'],
        'Apple': ['Row1_Apple', 'Row2_Apple', 'Row3_Apple'],
        'Mountain': ['Row1_Mountain', 'Row2_Mountain', 'Row3_Mountain'],
        'Sunflower': ['Row1_Sunflower', 'Row2_Sunflower', 'Row3_Sunflower'],
        'Kite': ['Row1_Kite', 'Row2_Kite', 'Row3_Kite']
    }


@app.route('/mergeDatasets', methods=['GET'])
def getMergeDatasets():

    database = request.args.get('database')

    driver = getDriver(database)
    query = "match (d:DATASET) return d.CMID as CMID order by CMID"
    data = getQuery(query, driver)

    return data


@app.route('/updateWaitingUSES', methods=['POST'])
def getUpdateWaitingUSES():
    data = request.get_data()
    data = json.loads(data)
    database = data.get("database")
    result = waitingUSES(database)
    return result


@app.route('/updateNewUsers', methods=['POST'])
def updateNewUsers():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get('database'))
        credentials = unlist(data.get('credentials'))
        process = unlist(data.get('process'))
        userid = data.get('userid')
        if database.lower() == "sociomap":
            database = "SocioMap"
        elif database.lower() == "archamap":
            database = "ArchaMap"
        else:
            raise Exception("database must be 'SocioMap' or 'ArchaMap'")

        if isinstance(credentials, dict):
            pass
        else:
            credentials = json.loads(credentials)

        if unlist(credentials.get("role")) != "admin":
            raise Exception("Error: User is not an admin")

        verified = verifyUser(unlist(credentials.get(
            "userid")), unlist(credentials.get("key")))

        if verified != "verified":
            raise Exception("Error: User is not verified")

        approver = unlist(credentials.get("userid"))

        result = enableUser(database=database, process=process,
                            userid=userid, approver=approver)

        users = [user for user in result if user.get("email")]
        mail = Mail()

        for user in users:
            body = f"""
Hello {user.get("first")} {user.get("last")},

Your registration has been approved. You can now access the {'and '.join(user.get("database"))} database. Please see catmapper.org/help or email support@catmapper.org for any questions.

Best,
CatMapper Team
            """
            sendEmail(mail, subject="CatMapper Registration Approved", recipients=[user.get(
                "email"), 'admin@catmapper.org'], body=body, sender="admin@catmapper.org")

        return result
    except Exception as e:
        result = str(e)
        return result, 500


app.add_url_rule('/test/send_test_email', 'send_test_email',
                 send_test_email, methods=['GET'])

app.add_url_rule('/test/testmsg/<database>/<msg>', 'testmsg',
                 testmsg, methods=['GET'])

app.add_url_rule('/routines/<routine>/<database>', 'get_routines',
                 get_routines, methods=['GET'])

app.add_url_rule('/admin/query/<database>', 'getRouteQuery',
                 getRouteQuery, methods=['POST'])


@app.route("/download/test", methods=["GET"])
def test_download():

    filename = "test.txt"
    file_path = os.path.join(TMP_DIR, filename)

    if os.path.exists(file_path):
        return send_from_directory(TMP_DIR, filename, as_attachment=True)
    else:
        abort(404, description="test.txt not found")


@app.route('/download/zip/<hash_id>', methods=['GET'])
def download_zip(hash_id):

    import subprocess

    TMP_DIR = "/app/tmp"

    subprocess.run(["chmod", "-R", "777", TMP_DIR], check=True)

    filename = f"merged_output_{hash_id}.zip"
    file_path = os.path.join(TMP_DIR, filename)

    if os.path.exists(file_path):
        return send_from_directory(TMP_DIR, filename, as_attachment=True)
    else:
        abort(404, description=f"{file_path} not found")


if __name__ == "__main__":
    app.run(debug=True, port=5001)
