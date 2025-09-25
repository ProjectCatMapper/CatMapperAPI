
from flask import request, Blueprint, jsonify
from CM import getDriver, getQuery, getPolygon, getPoints, getDatasetPoints
from bs4 import BeautifulSoup
from collections import defaultdict
import json

explore_bp = Blueprint('explore', __name__)

# gets samples, info data, categories and map data
@explore_bp.route("/category", methods=['GET'])
def catm():

    cmid = request.args.get('cmid')
    database = request.args.get('database')

    driver = getDriver(database)
    session = driver.session()

    '''Gets the list of relations for the node'''
    relnames = []
    visible_relations = ["USES", "CONTAINS", "DISTRICT_OF",
                         "LANGUOID_OF", "RELIGION_OF", "PERIOD_OF", "CULTURE_OF", "POLITY_OF"]
    q = "MATCH (n)-[r]-(n1) WHERE n.CMID='"+cmid + \
        "' RETURN DISTINCT TYPE(r) as relation_name"
    node_relation_types = session.run(q).data()
    for i in node_relation_types:
        if i['relation_name'] in visible_relations:
            relnames.append(i['relation_name'])
    driver.close()

    driver = getDriver(database)

    # this query gets the labels for the node
    q = f"match (a) where a.CMID = '{cmid}' return labels(a) as label"
    labels = getQuery(q,driver=driver,type='list')
    print(labels)
    if "DATASET" in labels[0]:
        label = "DATASET"
    elif "CATEGORY" in labels[0]:
        label = "CATEGORY"
    elif "DELETED" in labels[0]:
        label = "DELETED"

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
                MATCH (a:CATEGORY)<-[r:USES]-(d:DATASET)
                WHERE a.CMID = cmid

                WITH a, d, r, coalesce(d.project,d.CMName) AS Source, d.CMID AS datasetID, d.DatasetVersion AS Version

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
                    apoc.text.join(Name, '; ') AS Name,
                    apoc.text.join([i IN [country.country, district.district] WHERE i IS NOT NULL AND i <> ''], ', ') AS Location,
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

    elif label == "DATASET":
        qInfo = '''
            unwind $cmid as cmid
            match (a:DATASET)
            where a.CMID = cmid
            with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
            'return null as name',{id:a.District}) yield value as Location
            return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID,
            labels(a) as Domains, a.parent as Parent,
            a.DatasetCitation as Citation, "<a href ='" + a.DatasetLocation + "' target='_blank' >" + a.DatasetLocation +"</a>" as `Dataset Location`,
            a.yearPublished as `Year Published`,
            CASE 
                WHEN a.recordStart IS NULL AND a.recordEnd IS NULL THEN null
                WHEN a.recordStart = a.recordEnd THEN a.recordStart
                ELSE coalesce(a.recordStart, '') + '-' + coalesce(a.recordEnd, '')
            END AS `Time Span`,
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
        
    elif label == "DELETED":
        qInfo = '''
            unwind $cmid as cmid
            match (a:DELETED)
            where a.CMID = cmid
            OPTIONAL MATCH (a)-[:IS]->(b)
            return a.CMName as CMName, a.CMID as CMID,
            labels(a) as Domains,
            CASE WHEN b IS NOT NULL THEN b.CMID ELSE NULL END AS Merged_into_CMID
            '''
        
        qSamples = None

        qCategories = None
        

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

            grouped = defaultdict(lambda: {
                'Name': set(),
                'Population est.': 0,
                'Sample size': 0
            })

            for row in samples:
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

                # Aggregate population and sample size (convert to float or int safely)
                try:
                    pop = float(row.get('Population est.', 0))  # or int(), depending on data
                    group['Population est.'] += pop
                except (ValueError, TypeError):
                    pass

                try:
                    sample = float(row.get('Sample size', 0))
                    group['Sample size'] += sample
                except (ValueError, TypeError):
                    pass

            # Construct the final merged output
            aggregated_samples = []

            for key, values in grouped.items():
                (Source, rStart, rEnd, Location, Type, yStart, yEnd, link2, Version, cType, Link) = key
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
                    'Name': '; '.join(sorted(values['Name'])),
                    'Population est.': round(values['Population est.']),
                    'Sample size': round(values['Sample size']),
                })
            
            for row in aggregated_samples:
                if row["Sample size"] == 0:
                    row["Sample size"] = ""
                if row["Population est."] == 0:
                    row["Population est."] = ""
                names_set = set(name.strip() for name in row["Name"].split(";"))
                row["Name"] = ", ".join(names_set)
                
                
            samples = aggregated_samples

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
    dataset_points = getDatasetPoints(cmid, driver)

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
                    {"cood": valid_data[i]['geometry']["coordinates"], "source": valid_data[i]["source"]})
            else:
                temp = valid_data[i]
                source = temp['source']
                for j in range(0, len(temp['geometry']['coordinates'])):
                    point.append(
                        {'cood': temp['geometry']['coordinates'][j], "source": source})
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

    return jsonify({
        "info": info[0],
        "samples": samples,
        "categories": categories,
        "polygons": polygons,
        "points": points,
        "datasetpoints": transformed_points,
        "relnames": relnames,
        "polysource": polysources,
        "badsources": bad_sources
    })
