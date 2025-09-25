
from flask import request, Blueprint, jsonify
from CM import getDriver, getQuery, getPolygon, getPoints, getDatasetPoints, custom_sort, serialize_node, serialize_relationship, flatten_json, unlist, search
from bs4 import BeautifulSoup
from collections import defaultdict
import json
import re

explore_bp = Blueprint('explore', __name__)

# gets samples, info data, categories and map data
@explore_bp.route("/category", methods=['GET'])
def catm():
    try:
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
    except Exception as e:
        return "Error returning results: " + str(e), 500

@explore_bp.route("/network", methods=['GET'])
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


@explore_bp.route("/explore", methods=['GET'])
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

@explore_bp.route('/networks', methods=['GET'])
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

@explore_bp.route('/networksjs', methods=['GET'])
def getNetworkjs():
    try:
        cmid = request.args.get('cmid')
        cmid = re.split(",", cmid)
        domain = request.args.get('domain')
        if domain is not None:
            domain = re.split(",", domain)
        else:
            domain = ["CATEGORY", "DATASET"]
        
        #endcmid = request.args.get('endcmid')
        relation = request.args.get('relation')
        if relation is None:
            relation = "USES"
        database = request.args.get('database')

        driver = getDriver(database)

        #         if endcmid is not None:
        #             cypher_query = """
        # unwind $cmid as cmid unwind $endcmid as endcmid unwind $relation as relation
        # MATCH (a)
        # WHERE a.CMID = cmid
        # optional match (a)-[r]-(e)
        # where type(r) = relation and e.CMID = endcmid and
        # not isEmpty([label IN labels(e)
        # WHERE label IN apoc.coll.flatten([$domain],true)])
        # with collect(distinct a) as a, r, e
        # return a, collect(distinct r) as r, collect(distinct e) as e
        # LIMIT 300
        # """
        #         else:

        cypher_query = """
        unwind $cmid as cmid unwind $relation as relation MATCH (a)
        WHERE a.CMID = cmid
        optional match (a)-[r]-(e)
        where type(r) = relation and
        not isEmpty([label IN labels(e)
        WHERE label IN apoc.coll.flatten([$domain],true)])
        with collect(distinct a) as a, r, e
        LIMIT 300
        return a, collect(distinct r) as r, collect(distinct e) as e
        """

        with driver.session() as session:
            # Execute the Cypher queries
            result = session.run(
                cypher_query, cmid=cmid, relation=relation, domain=domain)
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

        return {"node": node, "relations": rel, "relNodes": end, "params": [{"cmid": cmid, "database": database, "domain": domain, "relation": relation}]}
    except Exception as e:
        return str(e), 500

@explore_bp.route('/search', methods=['GET'])
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
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = request.args.get('domain')
        yearStart = request.args.get('yearStart')
        yearEnd = request.args.get('yearEnd')
        context = request.args.get('context')
        dataset = request.args.get('dataset')
        country = request.args.get('country')
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
            query,
            dataset)
        
        return jsonify(result)

    except Exception as e:
        return str(e), 500
    
@explore_bp.route('/geometry', methods=['GET'])
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

@explore_bp.route('/dataset', methods=['GET', 'POST'])
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
        
        print(cmid)

        driver = getDriver(database)

        if isinstance(domain, str):
            domain = [domain]

        if domain is None or "ANY DOMAIN" in domain:
            domain = ["CATEGORY"]

        if domain != ["CATEGORY"]:
            labels = getQuery(
                query="MATCH (l:LABEL) RETURN l.CMName AS label, l.groupLabel AS groupLabel", driver=driver)
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
            result = getQuery(query=query,params={
                "cmid": cmid}, driver=driver, type="list")
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

        return df.to_json(orient='records')

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = "Error: " + str(e)
        return result, 500


@explore_bp.route('/CMID', methods=['GET'])
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

@explore_bp.route('/allDatasets', methods=['GET'])
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

@explore_bp.route('/networknodes', methods=['POST'])
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


@explore_bp.route('/datasetDomains', methods=['POST'])
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

