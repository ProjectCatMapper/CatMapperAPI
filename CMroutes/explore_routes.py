from datetime import datetime, timezone

from flask import request, Blueprint, jsonify, current_app
from CM import *
from collections import defaultdict
import colorsys
import json
import re

explore_bp = Blueprint('explore', __name__)
DEFAULT_NODE_PAGE_NETWORK_LIMIT = 500


def _hex_to_rgb(hex_color):
    clean = (hex_color or "").strip().lstrip("#")
    if len(clean) != 6:
        return None
    try:
        return tuple(int(clean[i:i + 2], 16) for i in (0, 2, 4))
    except ValueError:
        return None


def _rgb_to_hex(rgb):
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def _average_hex(colors):
    rgbs = [_hex_to_rgb(c) for c in colors]
    rgbs = [rgb for rgb in rgbs if rgb is not None]
    if not rgbs:
        return None
    n = len(rgbs)
    avg = tuple(int(round(sum(ch[i] for ch in rgbs) / n)) for i in range(3))
    return _rgb_to_hex(avg)


def _desaturate_hex(hex_color, factor=0.88):
    rgb = _hex_to_rgb(hex_color)
    if rgb is None:
        return hex_color

    r, g, b = [c / 255.0 for c in rgb]
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    s = max(0.0, min(1.0, s * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return _rgb_to_hex((int(round(r2 * 255)), int(round(g2 * 255)), int(round(b2 * 255))))


def _get_label_metadata_map(driver):
    query = """
    MATCH (l:LABEL)
    RETURN l.CMName AS label, l.color AS color, l.groupLabel AS groupLabel
    """
    rows = getQuery(query, driver, type="dict")
    return {
        row["label"]: {"color": row.get("color"), "groupLabel": row.get("groupLabel")}
        for row in rows
        if row.get("label")
    }


def _unique_preserve_order(values):
    return list(dict.fromkeys(values))


def _get_effective_labels(labels, label_metadata_map):
    # Exclude generic structural labels from legend/color selection.
    cleaned = _unique_preserve_order(
        [lbl for lbl in labels if lbl and lbl not in ["CATEGORY", "DISTRICT"]]
    )
    if not cleaned:
        return []

    # If a node has both a top-level domain label (groupLabel == CMName)
    # and one of that domain's subdomains, keep only the subdomain labels.
    label_to_group = {}
    for label in cleaned:
        group_label = (label_metadata_map.get(label) or {}).get("groupLabel")
        label_to_group[label] = group_label or label

    groups_with_subdomains = {
        label_to_group[label]
        for label in cleaned
        if label_to_group[label] and label != label_to_group[label]
    }

    effective = []
    for label in cleaned:
        group_label = label_to_group.get(label)
        if label == group_label and group_label in groups_with_subdomains:
            continue
        effective.append(label)

    return effective


def _apply_node_colors(rows, label_metadata_map):
    for row in rows:
        labels = row.get("labels") or []
        if not isinstance(labels, list):
            labels = [labels]

        effective_labels = _get_effective_labels(labels, label_metadata_map)
        color_pairs = [
            (label, (label_metadata_map.get(label) or {}).get("color"))
            for label in effective_labels
        ]
        color_pairs = [(lbl, clr) for lbl, clr in color_pairs if clr]

        if not effective_labels:
            row["color"] = "#cccccc"
            row["legendLabel"] = "UNMAPPED"
            continue

        if not color_pairs:
            row["color"] = "#cccccc"
            row["legendLabel"] = ":".join(effective_labels)
            continue

        labels_with_colors = effective_labels
        unique_colors = list(dict.fromkeys([clr for _, clr in color_pairs]))

        if len(unique_colors) == 1 or len(set(c.lower() for c in unique_colors)) == 1:
            row["color"] = unique_colors[0]
            row["legendLabel"] = labels_with_colors[0] if len(labels_with_colors) == 1 else ":".join(labels_with_colors)
        else:
            avg_color = _average_hex(unique_colors) or "#cccccc"
            row["color"] = _desaturate_hex(avg_color)
            row["legendLabel"] = ":".join(labels_with_colors)


def _empty_network_payload(*, cmid, database, domains, relation):
    return {
        "node": [],
        "relations": [],
        "relNodes": [],
        "params": [{
            "cmid": cmid,
            "database": database,
            "domain": domains,
            "relation": relation,
        }],
    }


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _split_csv_values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = re.split(",", str(value))
    return [str(item).strip() for item in values if str(item).strip()]


def _sanitize_filename_part(value):
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return cleaned or "node"


def _normalize_domains_value(value):
    domains = _split_csv_values(value)
    return _unique_preserve_order(domains)


def _get_networkjs_payload(*, cmid, database, relation=None, domain=None, limit=10):
    cmid_values = _split_csv_values(cmid)
    domains = _normalize_domains_value(domain)

    relation_value = unlist(relation)
    if relation_value is None:
        relation_value = "USES"
    relation_value = sanitize_cypher_identifier(relation_value, "relationship")

    driver = getDriver(database)

    is_dataset_root = bool(cmid_values) and cmid_values[0].startswith(("SD", "AD"))

    if is_dataset_root and relation_value == "USES":
        cypher_query = """
        unwind $cmid as cmid
        MATCH (a:DATASET {CMID: cmid})
        OPTIONAL MATCH (a)-[r:USES]->(e:CATEGORY)
        WHERE $domain_count = 0 OR ANY(label IN labels(e) WHERE label IN $domains)
        WITH a, collect({e: e, r: r})[0..$limit] AS pairs
        RETURN
            collect(distinct a) AS a,
            [p IN pairs WHERE p.r IS NOT NULL | p.r] AS r,
            [p IN pairs WHERE p.e IS NOT NULL | p.e] AS e
        """
    elif relation_value == "MERGING":
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
           // Include stack-to-dataset edges when root is a stack:
           // (:STACK)-[:MERGING]->(:DATASET)
           OPTIONAL MATCH (a:STACK)-[r7:MERGING]->(d2:DATASET)
        WITH a,
               [x IN collect(distinct r1) + collect(distinct r2) + collect(distinct r5) + collect(distinct r6) + collect(distinct r3) + collect(distinct r4) + collect(distinct r7) WHERE x IS NOT NULL][0..$limit] AS r,
               [x IN collect(distinct s) + collect(distinct v) + collect(distinct s2) + collect(distinct m2) + collect(distinct m) + collect(distinct v2) + collect(distinct d2)
              WHERE x IS NOT NULL AND ($domain_count = 0 OR ANY(label IN labels(x) WHERE label IN $domains))][0..$limit] AS e
        RETURN collect(distinct a) AS a, r, e
        """
    else:
        cypher_query = f"""
        unwind $cmid as cmid
        MATCH (a:CATEGORY|DATASET {{CMID: cmid}})
        optional match (a)-[r:{relation_value}]-(e:CATEGORY|DATASET)
        WHERE e IS NULL OR $domain_count = 0 OR ANY(label IN labels(e) WHERE label IN $domains)
        with a, collect(distinct r)[0..$limit] as r, collect(distinct e)[0..$limit] as e
        return collect(distinct a) as a, r, e
        """

    result = getQuery(
        cypher_query,
        driver=driver,
        cmid=cmid_values,
        limit=int(limit),
        domains=domains,
        domain_count=len(domains),
        type="records",
    )
    if not result:
        return _empty_network_payload(cmid=cmid_values, database=database, domains=domains, relation=relation_value)

    result = unlist(result)
    if not result:
        return _empty_network_payload(cmid=cmid_values, database=database, domains=domains, relation=relation_value)

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

    if relation_value == "USES":
        from collections import Counter

        uses_per_dataset = Counter(r['start_node_id'] for r in rel)

        rel_scores = [{"r": r, "score": uses_per_dataset[r["start_node_id"]]} for r in rel]
        rel_scores.sort(key=lambda x: x["score"])
        rel = [item["r"] for item in rel_scores[:limit]]

        connected_node_ids = {
            node_row.get("id")
            for node_row in node
            if node_row.get("id") is not None
        }
        for relation_row in rel:
            connected_node_ids.add(relation_row.get("start_node_id"))
            connected_node_ids.add(relation_row.get("end_node_id"))
        end = [
            node_row for node_row in end
            if node_row.get("id") in connected_node_ids
        ]

    label_metadata_map = _get_label_metadata_map(driver)
    _apply_node_colors(node, label_metadata_map)
    _apply_node_colors(end, label_metadata_map)

    return {
        "node": node,
        "relations": rel,
        "relNodes": end,
        "params": [{
            "cmid": cmid_values,
            "database": database,
            "domain": domains,
            "relation": relation_value,
        }],
    }


def _build_node_page_payload(database, cmid, *, host_url=None):
    info = getCategoryInfo(database, cmid)
    if info is None:
        raise LookupError("Node not found")

    category_page = getCategoryPage(database, cmid) or {}
    geometry = exploreGeometry(database, cmid) or {}

    relnames = _unique_preserve_order(_split_csv_values(category_page.get("relnames")))
    networks = {
        relation: _get_networkjs_payload(
            cmid=cmid,
            database=database,
            relation=relation,
            limit=DEFAULT_NODE_PAGE_NETWORK_LIMIT,
        )
        for relation in relnames
    }

    domain_values = info.get("Domains", [])
    if not isinstance(domain_values, list):
        domain_values = _split_csv_values(domain_values)
    domain_values = _unique_preserve_order(domain_values)

    merge_template_summary = None
    if any(label in {"STACK", "MERGING"} for label in domain_values):
        from .merge_routes import build_merge_template_summary_payload

        try:
            merge_template_summary = build_merge_template_summary_payload(database, cmid)
        except LookupError:
            merge_template_summary = None

    page_path = f"/{str(database or '').lower()}/{cmid}"
    normalized_host = str(host_url or "").rstrip("/")

    return {
        "version": "1.0",
        "resourceType": "nodeExplorePage",
        "database": database,
        "cmid": cmid,
        "canonicalUrl": f"{normalized_host}{page_path}" if normalized_host else page_path,
        "jsonUrl": request.base_url,
        "retrievedAt": _utc_now_iso(),
        "info": info,
        "categoryPage": category_page,
        "geometry": geometry,
        "networks": networks,
        "mergeTemplateSummary": merge_template_summary,
    }

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
    label = validate_domain_label(p0, driver=driver_neo4j)
    rel_type = sanitize_cypher_identifier(p2, "relationship")
    cmid = str(p1).strip()

    with driver_neo4j.session() as session:
        q = f"MATCH (n:{label} {{CMID: $cmid}})-[r:{rel_type}]-(OtherNodes) RETURN n,r,OtherNodes"
        r = session.run(q, cmid=cmid)
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
    where a.CMID = cmid with a,r,d,
    case when r.country is not null and not r.country = [] then custom.getName(r.country) else null end as country,
    case when r.district is not null and not r.district = [] then custom.getName(r.district) else null end as district,
    case when r.language is not null and not r.language = [] then custom.getGlot(r.language) else null end as language,
    case when r.religion is not null and not r.religion = [] then custom.getName(r.religion) else null end as religion,
    case when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is not null then custom.getMinYear(r.yearStart) + '-' + custom.getMaxYear(r.yearEnd)
    when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is null then custom.getMinYear(r.yearStart) + '-present'
    when custom.getMinYear(r.yearStart) is null and custom.getMaxYear(r.yearEnd) is not null then custom.getMaxYear(r.yearEnd)
    else null
    end as timeSpan
    return a.CMName as CMName, apoc.text.join([i in [custom.anytoList(collect(split(country,', ')),true),
    custom.anytoList(collect(split(district,', ')),true)] where not i = ''],', ') as Location,
    a.CMID as CMID, apoc.text.join([i in labels(a) where not i = 'CATEGORY'],', ') as Domains,
    custom.anytoList(collect(split(language,', ')),true) as Languages, custom.anytoList(collect(split(religion,', ')),true) as Religions,
    custom.anytoList(collect(split(timeSpan,', ')),true) as `Date range`
    '''
            qSamples = '''
    unwind $cmid as cmid
    match (a)<-[r:USES]-(d:DATASET)
    where a.CMID = cmid
    with custom.anytoList(collect(r.Name),true) as Name,
    case when r.country is not null then custom.getName(r.country) else null end as country,
    case when r.district is not null then custom.getName(r.district) else null end as district,
    d.project as Source, d.CMID as datasetID, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd,
    toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
    return Name, apoc.text.join([i in [custom.anytoList(collect(country),true),custom.anytoList(collect(district),true)] where not i = ''],', ') as Location, type as Type,
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
    with a, case when a.District is not null then custom.getName(a.District) else null end as Location
    return a.CMName as CMName, custom.anytoList(collect(Location),true) as Location, a.CMID as CMID,
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
        domain = request.args.get('domain')
        limit = int(request.args.get('limit'))
        relation = request.args.get('relation')
        database = request.args.get('database')

        return _get_networkjs_payload(
            cmid=cmid,
            database=database,
            relation=relation,
            domain=domain,
            limit=limit,
        )
    except Exception as e:
        return str(e), 500


@explore_bp.route("/entity/<database>/<cmid>.json", methods=['GET'])
def get_node_page_json(database, cmid):
    try:
        payload = _build_node_page_payload(database, cmid, host_url=request.host_url)
        response = jsonify(payload)
        response.headers["Content-Disposition"] = (
            f'inline; filename="{_sanitize_filename_part(database)}_{_sanitize_filename_part(cmid)}.json"'
        )
        return response
    except LookupError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    
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
        return current_app.response_class(result, mimetype='application/json')

    except Exception as e:
        current_app.logger.exception("dataset endpoint failed")
        return jsonify({"error": str(e)}), 500


@explore_bp.route('/CMID/<database>/<cmid>', methods=['GET'])
def getCMID(database, cmid):
    try:
        database = database
        cmid = cmid
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

        node = getQuery(query1, driver=driver, cmid=cmid)
        relations = getQuery(query2, driver=driver, cmid=cmid)

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
