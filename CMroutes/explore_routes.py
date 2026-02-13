
from flask import request, Blueprint, jsonify
from CM import *
from collections import defaultdict
import json
import re

explore_bp = Blueprint('explore', __name__)

@explore_bp.route("/info/<database>/<cmid>", methods=['GET'])
def getInfo(database, cmid):
    result = getCategoryInfo(database, cmid)
    return jsonify(result)

# gets samples and categories
@explore_bp.route("/category/<database>/<cmid>", methods=['GET'])
def catm(database, cmid):
    result = getCategoryPage(database, cmid)
    return jsonify(result)

@explore_bp.route("/exploreGeometry/<database>/<cmid>", methods=['GET'])
def getPointGeometry(database, cmid):
    try:
        results = exploreGeometry(database, cmid)
        return jsonify(results)
        
    except Exception as e:
        return "Error returning results: " + str(", ".join(map(str, e.args))), 500

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
            UNWIND $cmid AS cmid
            MATCH (:ADM0 {CMID: cmid})-[:DISTRICT_OF]->(c:CATEGORY)
            WITH c
            UNWIND labels(c) AS Domain
            WITH Domain, count(DISTRICT c) AS Count
            WHERE Domain <> 'CATEGORY' // Optional: filter out the base label if needed
            RETURN Domain, Count 
            ORDER BY Domain
            """

        else:
            qInfo = '''
    unwind $cmid as cmid
    match (a:DATASET {CMID: cmid})
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

        info = getQuery(qInfo, driver, params={'cmid': cmid})
        if qCategories is None:
            categories = []
        else:
            categories = getQuery(qCategories, driver, params={'cmid': cmid})
            categories = [dict(record) for record in categories]
        if qSamples is not None:
            samples = getQuery(qSamples, driver, params={'cmid': cmid, 'database': database})
            samples = [dict(record) for record in samples]
        else:
            samples = []

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

        # Execute the Cypher queries
        result = getQuery(
            cypher_query, driver = driver, cmid=cmid, relation=relation, domain=domain, endcmid=endcmid)
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
        limit = int(request.args.get('limit'))
        if domain is not None:
            domain = re.split(",", domain)
        else:
            if cmid[0].startswith(("SD", "AD")):
                domain = "DATASET"
            else:
                domain = "CATEGORY"

        relation = unlist(request.args.get('relation'))
        if relation is None:
            relation = "USES"
        database = request.args.get('database')

        driver = getDriver(database)
        
        if domain == "DATASET" and relation == "USES":
            cypher_query = """
            unwind $cmid as cmid
            MATCH (a:DATASET {CMID: cmid})
            optional match (a)-[r:USES]->(e:CATEGORY)
            with a, r, e limit $limit
            return collect(distinct a) as a, collect(distinct r) as r, collect(distinct e) as e
            """
        elif relation == "MERGING":
            cypher_query = """
            unwind $cmid as cmid
            MATCH (a {CMID: cmid})
            // For MERGING templates, include the expected chain:
            // (:MERGING)-[:MERGING]->(:STACK)-[:MERGING]->(:VARIABLE)
            OPTIONAL MATCH (a)-[r1:MERGING]->(s:STACK)
            OPTIONAL MATCH (s)-[r2:MERGING]->(v:VARIABLE)
            // Include upstream path for dataset-centered view:
            // (:STACK)-[:MERGING]->(:DATASET) and (:MERGING)-[:MERGING]->(:STACK)
            OPTIONAL MATCH (s2:STACK)-[r5:MERGING]->(a)
            OPTIONAL MATCH (m2:MERGING)-[r6:MERGING]->(s2)
            // Also support stack-centered view:
            // (:MERGING)-[:MERGING]->(:STACK) and (:STACK)-[:MERGING]->(:VARIABLE)
            OPTIONAL MATCH (m:MERGING)-[r3:MERGING]->(a:STACK)
            OPTIONAL MATCH (a)-[r4:MERGING]->(v2:VARIABLE)
            WITH a,
                 [x IN collect(distinct r1) + collect(distinct r2) + collect(distinct r5) + collect(distinct r6) + collect(distinct r3) + collect(distinct r4) WHERE x IS NOT NULL][0..$limit] AS r,
                 [x IN collect(distinct s) + collect(distinct v) + collect(distinct s2) + collect(distinct m2) + collect(distinct m) + collect(distinct v2) WHERE x IS NOT NULL][0..$limit] AS e
            RETURN collect(distinct a) AS a, r, e
            """
        else:
            cypher_query = f"""
            unwind $cmid as cmid
            MATCH (a:CATEGORY|DATASET {{CMID: cmid}})
            optional match (a)-[r:{relation}]-(e:CATEGORY|DATASET)
            with a, r, e limit $limit
            return collect(distinct a) as a, collect(distinct r) as r, collect(distinct e) as e
            """

        # Execute the Cypher queries
        result = getQuery(
            cypher_query, driver = driver, cmid=cmid, limit=limit, type = "records")
        result = unlist(result)
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

        if relation == "USES":

            node = [flatten_json(entry) for entry in node]
            rel = [flatten_json(entry) for entry in rel]
            end = [flatten_json(entry) for entry in end]

            from collections import Counter

            uses_per_dataset = Counter(r['start_node_id'] for r in rel)

            rel_scores = [{"r":r,"score":uses_per_dataset[r["start_node_id"]]} for r in rel]

            rel_scores.sort(key=lambda x: x["score"])

            rel = [i["r"] for i in rel_scores[:limit]]

        else:
            node = [flatten_json(entry) for entry in node]
            rel = [flatten_json(entry) for entry in rel]
            end = [flatten_json(entry) for entry in end]

        return {"node": node, "relations": rel, "relNodes": end, "params": [{"cmid": cmid, "database": database, "domain": domain, "relation": relation}]}
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
    try:
        if request.method == 'GET':
            params = request.args
        elif request.method == 'POST':
            params = json.loads(request.get_data())
        else:
            return "Invalid request method", 400
        
        database = unlist(params.get('database'))
        cmid = unlist(params.get('cmid'))
        domain = params.get('domain')
        children = unlist(params.get('children'))

        result = getDatasetData(database, cmid, domain, children)
        return result

    except Exception as e:
        result = str(e)
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

        node = getQuery(query1, cmid=cmid)
        relations = getQuery(query2, cmid=cmid)

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

        data = getQuery(
            query, driver=driver, cmid=cmid, relation=relation, domains=domains)

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500
