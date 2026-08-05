import math
import re
from html import escape

from .metadata import getPropertiesMetadata, get_public_subdomains
from .search import search
from .utils import getDriver, getQuery


RECONCILIATION_API_VERSION = "0.2"
RECONCILIATION_BATCH_SIZE = 50
RECONCILIATION_MAX_LIMIT = 100

_DATABASES = {
    "sociomap": "SocioMap",
    "archamap": "ArchaMap",
}

_BASE_PROPERTIES = [
    {"id": "Name", "name": "Name", "description": "CatMapper node name."},
    {"id": "CMID", "name": "CatMapper ID", "description": "Stable CatMapper identifier."},
    {"id": "domain", "name": "Domain", "description": "CatMapper type or domain label."},
    {"id": "Key", "name": "Dataset key", "description": "Encoding key on a USES relationship."},
    {"id": "dataset", "name": "Dataset", "description": "Dataset that uses the category."},
    {"id": "context", "name": "Context", "description": "Context node CMID used to narrow matching."},
    {"id": "country", "name": "Country", "description": "ADM0 country CMID used to narrow matching."},
    {"id": "yearStart", "name": "Year start", "description": "Start year for temporal overlap filtering."},
    {"id": "yearEnd", "name": "Year end", "description": "End year for temporal overlap filtering."},
]


def normalize_database(database):
    key = str(database or "").strip().lower()
    if key not in _DATABASES:
        raise ValueError("Invalid database. Use 'SocioMap' or 'ArchaMap'.")
    return _DATABASES[key]


def build_manifest(database, base_url, frontend_base_url):
    database = normalize_database(database)
    base_url = str(base_url or "").rstrip("/")
    frontend_base_url = str(frontend_base_url or "").rstrip("/")
    frontend_database = database.lower()

    return {
        "versions": [RECONCILIATION_API_VERSION],
        "name": f"CatMapper {database} Reconciliation",
        "identifierSpace": f"https://catmapper.org/{frontend_database}",
        "schemaSpace": "https://catmapper.org/schema/catmapper",
        "documentation": "https://help.catmapper.org/API",
        "serviceVersion": "catmapper-openrefine-0.1",
        "logo": "https://catmapper.org/media/CatMapperLogoAlternate.png",
        "defaultTypes": [
            {"id": "CATEGORY", "name": "Category"},
            {"id": "DATASET", "name": "Dataset"},
        ],
        "view": {"url": f"{frontend_base_url}/{frontend_database}/{{{{id}}}}"},
        "preview": {
            "url": f"{base_url}/preview/{{{{id}}}}",
            "width": 430,
            "height": 300,
        },
        "suggest": {
            "entity": {"service_url": base_url, "service_path": "/suggest/entity"},
            "property": {"service_url": base_url, "service_path": "/suggest/property"},
            "type": {"service_url": base_url, "service_path": "/suggest/type"},
        },
        "extend": {
            "propose_properties": {"service_url": base_url, "service_path": "/properties"},
        },
        "batchSize": RECONCILIATION_BATCH_SIZE,
    }


def get_reconciliation_types(database):
    database = normalize_database(database)
    seen = set()
    types = []

    def add_type(type_id, name=None):
        type_id = str(type_id or "").strip()
        if not type_id or type_id in seen:
            return
        seen.add(type_id)
        types.append({"id": type_id, "name": name or _display_name(type_id)})

    add_type("CATEGORY", "Category")
    add_type("DATASET", "Dataset")

    for row in get_public_subdomains(database):
        if isinstance(row, dict):
            add_type(row.get("domain"))
            for subdomain in row.get("subdomains") or []:
                add_type(subdomain)

    return types


def get_reconciliation_properties(database):
    database = normalize_database(database)
    properties = {entry["id"]: dict(entry) for entry in _BASE_PROPERTIES}

    try:
        driver = getDriver(database)
        rows = getPropertiesMetadata(driver)
    except Exception:
        rows = []

    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        prop_id = str(row.get("property") or "").strip()
        if not prop_id or prop_id in properties:
            continue
        properties[prop_id] = {
            "id": prop_id,
            "name": prop_id,
            "description": str(row.get("description") or "").strip(),
        }

    return list(properties.values())


def suggest_entities(database, prefix="", cursor=0, limit=20):
    database = normalize_database(database)
    cursor = _safe_non_negative_int(cursor, 0)
    limit = _bounded_limit(limit, default=20)
    query_limit = min(RECONCILIATION_MAX_LIMIT, cursor + limit)

    result = search(
        database=database,
        term=str(prefix or "").strip() or None,
        property="Name",
        domain="ALL NODES",
        yearStart=None,
        yearEnd=None,
        context=None,
        country=None,
        query=None,
        dataset=None,
        limit=query_limit,
    )
    rows = _coerce_rows(result)[cursor:cursor + limit]

    return {
        "result": [
            {
                "id": str(row.get("CMID") or ""),
                "name": str(row.get("CMName") or ""),
                "description": _candidate_description(row),
                "notable": _types_from_domain(row.get("domain")),
            }
            for row in rows
            if row.get("CMID")
        ]
    }


def suggest_types(database, prefix="", cursor=0, limit=20):
    cursor = _safe_non_negative_int(cursor, 0)
    limit = _bounded_limit(limit, default=20)
    normalized_prefix = str(prefix or "").strip().lower()
    rows = [row for row in get_reconciliation_types(database) if _matches_prefix(row, normalized_prefix)]
    return {"result": rows[cursor:cursor + limit]}


def suggest_properties(database, prefix="", cursor=0, limit=20):
    cursor = _safe_non_negative_int(cursor, 0)
    limit = _bounded_limit(limit, default=20)
    normalized_prefix = str(prefix or "").strip().lower()
    rows = [row for row in get_reconciliation_properties(database) if _matches_prefix(row, normalized_prefix)]
    return {"result": rows[cursor:cursor + limit]}


def reconcile_query_batch(database, queries):
    database = normalize_database(database)
    if not isinstance(queries, dict):
        raise ValueError("queries must be a JSON object")
    if len(queries) > RECONCILIATION_BATCH_SIZE:
        raise OverflowError(f"queries batch exceeds maximum size {RECONCILIATION_BATCH_SIZE}")

    return {
        str(query_id): {"result": reconcile_single_query(database, query if isinstance(query, dict) else {})}
        for query_id, query in queries.items()
    }


def reconcile_single_query(database, query):
    query_text = str(query.get("query") or "").strip()
    filters = _extract_property_filters(query.get("properties"))

    search_property = "Name"
    search_term = query_text or None
    if filters.get("CMID"):
        search_property = "CMID"
        search_term = filters.get("CMID")
    elif filters.get("Key"):
        search_property = "Key"
        search_term = filters.get("Key")
    elif filters.get("Name") and not search_term:
        search_property = "Name"
        search_term = filters.get("Name")

    domain = _domain_from_type(query.get("type"))
    if search_property == "Key" and domain in {None, "ALL NODES"}:
        domain = "CATEGORY"
    domain = domain or "ALL NODES"

    result = search(
        database=database,
        term=search_term,
        property=search_property,
        domain=domain,
        yearStart=filters.get("yearStart"),
        yearEnd=filters.get("yearEnd"),
        context=filters.get("context"),
        country=filters.get("country"),
        query=None,
        dataset=filters.get("dataset"),
        limit=_bounded_limit(query.get("limit"), default=10),
    )

    rows = _coerce_rows(result)
    return [_candidate_from_search_row(row, query_text, search_property, search_term) for row in rows]


def propose_properties(database, type_id=None, limit=None):
    type_id = str(type_id or "CATEGORY").strip() or "CATEGORY"
    properties = get_reconciliation_properties(database)
    if limit is not None:
        properties = properties[:_bounded_limit(limit, default=len(properties))]
    return {
        "type": type_id,
        "properties": properties,
        **({"limit": _safe_non_negative_int(limit, 0)} if limit is not None else {}),
    }


def build_data_extension_response(database, extension_query):
    database = normalize_database(database)
    if not isinstance(extension_query, dict):
        raise ValueError("extend payload must be a JSON object")

    ids = [str(value).strip() for value in extension_query.get("ids") or [] if str(value or "").strip()]
    raw_properties = extension_query.get("properties") or []
    properties = [
        str(item.get("id") if isinstance(item, dict) else item).strip()
        for item in raw_properties
        if str(item.get("id") if isinstance(item, dict) else item).strip()
    ]

    property_map = {entry["id"]: entry for entry in get_reconciliation_properties(database)}
    meta = [{"id": prop_id, "name": property_map.get(prop_id, {}).get("name") or prop_id} for prop_id in properties]
    rows = {entity_id: {prop_id: [] for prop_id in properties} for entity_id in ids}
    if not ids or not properties:
        return {"meta": meta, "rows": rows}

    driver = getDriver(database)
    query = """
    UNWIND $ids AS cmid
    MATCH (n {CMID: cmid})
    OPTIONAL MATCH (n)<-[:AREA_OF]-(country:ADM0)
    OPTIONAL MATCH (n)<-[uses:USES]-(dataset:DATASET)
    WITH cmid, n,
         collect(DISTINCT country.CMName) AS countries,
         collect(DISTINCT uses.Key) AS keys,
         collect(DISTINCT {id: dataset.CMID, name: dataset.CMName}) AS datasets
    RETURN cmid AS requestedId,
           n.CMID AS CMID,
           n.CMName AS CMName,
           labels(n) AS labels,
           properties(n) AS properties,
           [country IN countries WHERE country IS NOT NULL] AS country,
           [key IN keys WHERE key IS NOT NULL] AS Key,
           [dataset IN datasets WHERE dataset.id IS NOT NULL] AS dataset
    """
    data = getQuery(query, driver=driver, params={"ids": ids}, type="dict")

    for row in data:
        entity_id = str(row.get("requestedId") or row.get("CMID") or "")
        if entity_id not in rows:
            continue
        for prop_id in properties:
            rows[entity_id][prop_id] = _extension_values(_extension_value(row, prop_id))

    return {"meta": meta, "rows": rows}


def build_preview_html(database, cmid, frontend_base_url):
    database = normalize_database(database)
    cmid = str(cmid or "").strip()
    if not cmid:
        raise LookupError("Node not found")

    driver = getDriver(database)
    query = """
    MATCH (n {CMID: $cmid})
    OPTIONAL MATCH (n)<-[:AREA_OF]-(country:ADM0)
    RETURN n.CMID AS CMID,
           n.CMName AS CMName,
           labels(n) AS labels,
           properties(n) AS properties,
           [country_name IN collect(DISTINCT country.CMName) WHERE country_name IS NOT NULL] AS country
    """
    rows = getQuery(query, driver=driver, params={"cmid": cmid}, type="dict")
    if not rows:
        raise LookupError("Node not found")

    row = rows[0]
    properties = row.get("properties") or {}
    name = str(row.get("CMName") or cmid)
    labels = _domain_values(row.get("labels"))
    countries = _listify(row.get("country"))
    description = str(properties.get("description") or properties.get("Definition") or "").strip()
    alt_names = _listify(properties.get("names"))[:5]
    frontend_url = f"{str(frontend_base_url or '').rstrip('/')}/{database.lower()}/{cmid}"

    details = [
        ("CMID", cmid),
        ("Domain", ", ".join(labels)),
        ("Country", ", ".join(str(country) for country in countries)),
        ("Alternative names", ", ".join(str(value) for value in alt_names)),
    ]
    detail_html = "\n".join(
        f"<dt>{escape(label)}</dt><dd>{escape(str(value))}</dd>"
        for label, value in details
        if value
    )
    description_html = f"<p>{escape(description)}</p>" if description else ""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <base target="_blank">
  <style>
    body {{ font-family: Arial, sans-serif; margin: 14px; color: #1f2933; }}
    h1 {{ font-size: 18px; margin: 0 0 8px; }}
    p {{ font-size: 13px; line-height: 1.4; }}
    dl {{ display: grid; grid-template-columns: 120px 1fr; gap: 6px 10px; font-size: 13px; }}
    dt {{ font-weight: 700; color: #52606d; }}
    dd {{ margin: 0; }}
    a {{ color: #0b65c2; }}
  </style>
</head>
<body>
  <h1>{escape(name)}</h1>
  {description_html}
  <dl>{detail_html}</dl>
  <p><a href="{escape(frontend_url)}">Open in CatMapper</a></p>
</body>
</html>"""


def _extract_property_filters(raw_properties):
    filters = {}
    if not isinstance(raw_properties, list):
        return filters

    aliases = {
        "cmid": "CMID",
        "catmapper id": "CMID",
        "catmapper id (cmid)": "CMID",
        "name": "Name",
        "cmname": "Name",
        "key": "Key",
        "dataset": "dataset",
        "datasetid": "dataset",
        "context": "context",
        "country": "country",
        "yearstart": "yearStart",
        "year_start": "yearStart",
        "yearend": "yearEnd",
        "year_end": "yearEnd",
    }

    for prop in raw_properties:
        if not isinstance(prop, dict):
            continue
        raw_pid = str(prop.get("pid") or prop.get("id") or "").strip()
        pid = aliases.get(raw_pid.lower(), raw_pid)
        if pid not in {"CMID", "Name", "Key", "dataset", "context", "country", "yearStart", "yearEnd"}:
            continue
        value = _first_property_value(prop.get("v"))
        if value not in {None, ""}:
            filters[pid] = value

    return filters


def _first_property_value(value):
    if isinstance(value, list):
        for item in value:
            cleaned = _first_property_value(item)
            if cleaned not in {None, ""}:
                return cleaned
        return None
    if isinstance(value, dict):
        return str(value.get("id") or value.get("name") or "").strip() or None
    if value is None:
        return None
    return str(value).strip()


def _candidate_from_search_row(row, query_text, search_property, search_term):
    distance = _safe_float(row.get("matchingDistance"), default=0)
    score = max(0.0, 100.0 - min(distance, 100.0))
    exact_name = bool(query_text) and _normalize_text(query_text) in {
        _normalize_text(row.get("CMName")),
        _normalize_text(row.get("matching")),
    }
    exact_id = search_property == "CMID" and str(row.get("CMID") or "") == str(search_term or "")

    return {
        "id": str(row.get("CMID") or ""),
        "name": str(row.get("CMName") or row.get("CMID") or ""),
        "description": _candidate_description(row),
        "type": _types_from_domain(row.get("domain")),
        "score": round(score, 3),
        "match": bool(exact_name or exact_id),
        "features": [
            {"id": "name_distance", "value": distance},
            {"id": "exact_name", "value": exact_name},
        ],
    }


def _candidate_description(row):
    domains = ", ".join(_domain_values(row.get("domain")))
    countries = ", ".join(str(value) for value in _listify(row.get("country")) if value)
    if domains and countries:
        return f"{domains}; {countries}"
    return domains or countries or "CatMapper entity"


def _domain_from_type(value):
    if isinstance(value, list):
        for item in value:
            domain = _domain_from_type(item)
            if domain:
                return domain
        return None
    if isinstance(value, dict):
        value = value.get("id")
    value = str(value or "").strip()
    if not value:
        return None
    if value.upper() in {"CATEGORY", "ENTITY"}:
        return "ALL NODES"
    return value


def _coerce_rows(result):
    if isinstance(result, dict):
        rows = result.get("data") or []
        return rows if isinstance(rows, list) else []
    return result if isinstance(result, list) else []


def _types_from_domain(value):
    return [{"id": domain, "name": _display_name(domain)} for domain in _domain_values(value)]


def _domain_values(value):
    values = _listify(value)
    cleaned = []
    for item in values:
        text = str(item or "").strip()
        if text and text not in {"CATEGORY", "DELETED"}:
            cleaned.append(text)
    return list(dict.fromkeys(cleaned)) or ["CATEGORY"]


def _extension_value(row, prop_id):
    props = row.get("properties") or {}
    if prop_id in {"Name", "CMName"}:
        return row.get("CMName")
    if prop_id == "CMID":
        return row.get("CMID")
    if prop_id == "domain":
        return _domain_values(row.get("labels"))
    if prop_id == "country":
        return row.get("country")
    if prop_id == "Key":
        return row.get("Key")
    if prop_id == "dataset":
        return row.get("dataset")
    return props.get(prop_id)


def _extension_values(value):
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(_extension_values(item))
        return values
    if isinstance(value, dict) and value.get("id") and value.get("name"):
        return [{"id": str(value.get("id")), "name": str(value.get("name"))}]
    if isinstance(value, bool):
        return [{"str": "true" if value else "false"}]
    if isinstance(value, int):
        return [{"int": value}]
    if isinstance(value, float):
        if math.isfinite(value):
            return [{"float": value}]
        return []
    return [{"str": str(value)}]


def _listify(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (tuple, set)):
        return list(value)
    return [value]


def _matches_prefix(row, normalized_prefix):
    if not normalized_prefix:
        return True
    return (
        str(row.get("id") or "").lower().startswith(normalized_prefix)
        or str(row.get("name") or "").lower().startswith(normalized_prefix)
    )


def _display_name(value):
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"[_-]+", " ", text).title()


def _bounded_limit(value, default=10):
    try:
        limit = int(value)
    except (TypeError, ValueError):
        limit = int(default)
    return max(1, min(RECONCILIATION_MAX_LIMIT, limit))


def _safe_non_negative_int(value, default=0):
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())
