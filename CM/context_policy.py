"""Domain permissions for source-backed contextual links.

PROPERTY.groupLabel is the referenced domain (the generated edge's start).
The category receiving a USES assertion is the generated edge's destination.
"""
import ast
import os
import re

from .utils import getQuery


STRUCTURAL = {"CATEGORY", "DATASET", "USER", "ALL NODES", "ANY DOMAIN"}


def enabled():
    mode = os.environ.get("CATMAPPER_CONTEXT_POLICY", "legacy")
    if mode not in {"legacy", "metadata"}:
        raise ValueError("CATMAPPER_CONTEXT_POLICY must be legacy or metadata")
    return mode == "metadata"


def parse_policy(labels, properties):
    domains = {}
    label_domains = {}
    for row in labels:
        label, domain = row.get("label"), row.get("groupLabel")
        if not label or not domain or domain in STRUCTURAL:
            continue
        previous = label_domains.setdefault(label, domain)
        if previous != domain:
            raise ValueError(f"Ambiguous domain metadata for {label}")
        if label != domain:
            if row.get("relationship"):
                raise ValueError(f"Subtype {label} must inherit contextual permissions from {domain}")
            continue
        if domain in domains:
            raise ValueError(f"Duplicate domain metadata for {domain}")
        mode, targets = row.get("contextualTargetMode"), row.get("contextualTargetDomains")
        relationship = row.get("relationship")
        if mode not in {"ALL_EXCEPT", "ONLY", "NONE"} or not isinstance(targets, list):
            raise ValueError(f"Missing or invalid contextual policy for {domain}")
        if any(not isinstance(value, str) for value in targets) or len(set(targets)) != len(targets):
            raise ValueError(f"Invalid contextual target domains for {domain}")
        if mode == "NONE" and (targets or relationship):
            raise ValueError(f"NONE policy for {domain} must have no relationship or targets")
        if mode != "NONE" and (not isinstance(relationship, str) or not re.fullmatch(r"[A-Z_]+_OF", relationship)):
            raise ValueError(f"Missing contextual relationship for {domain}")
        domains[domain] = {"relationship": relationship, "mode": mode, "targets": targets,
                           "order": row.get("contextualOrder")}
    if not domains:
        raise ValueError("Contextual policy metadata is missing")
    if set(label_domains.values()) - domains.keys():
        raise ValueError("A subtype references an unknown primary domain")
    relationships = {}
    for domain, rule in domains.items():
        if set(rule["targets"]) - domains.keys():
            raise ValueError(f"Unknown contextual target domain in {domain}")
        relation = rule["relationship"]
        if relation:
            if relation in relationships:
                raise ValueError(f"Duplicate contextual relationship {relation}")
            relationships[relation] = domain
        rule["allowed"] = sorted(
            target for target in domains
            if (rule["mode"] == "ALL_EXCEPT" and target not in rule["targets"])
            or (rule["mode"] == "ONLY" and target in rule["targets"])
        )
    mappings = {}
    for row in properties:
        relation = row.get("relationship")
        if not relation and row.get("groupLabel"):
            raise ValueError(f"Missing contextual PROPERTY mapping for {row.get('property')}")
        if not relation or relation == "CONTAINS":
            continue
        prop, domain = row.get("property"), row.get("groupLabel")
        if domain not in domains or relationships.get(relation) != domain:
            raise ValueError(f"Invalid contextual PROPERTY mapping for {prop}: {relation}")
        rule = {"sourceDomain": domain, "relationship": relation, "allowed": domains[domain]["allowed"]}
        if prop in mappings and mappings[prop] != rule:
            raise ValueError(f"Conflicting contextual PROPERTY mapping for {prop}")
        mappings[prop] = rule
    return {"domains": domains, "labels": label_domains, "properties": mappings}


def load_policy(driver):
    labels = getQuery("""MATCH (l:LABEL) RETURN l.CMName AS label, l.groupLabel AS groupLabel,
        l.relationship AS relationship, l.contextualTargetMode AS contextualTargetMode,
        l.contextualTargetDomains AS contextualTargetDomains,l.contextualOrder AS contextualOrder""", driver)
    properties = getQuery("""MATCH (p:PROPERTY {type:'relationship'})
        WHERE p.relationship IS NOT NULL OR p.groupLabel IS NOT NULL RETURN p.CMName AS property,
        p.groupLabel AS groupLabel,p.relationship AS relationship""", driver)
    return parse_policy(labels, properties)


def values(value):
    if value is None or (isinstance(value, float) and value != value):
        return []
    if isinstance(value, (list, tuple, set)):
        return [token for item in value for token in values(item)]
    text = str(value).strip()
    if text.startswith("["):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return values(parsed)
        except (ValueError, SyntaxError):
            pass
    return [token.strip() for token in re.split(r"\s*(?:;|,|\|{2,})\s*", text) if token.strip()]


def node_domains(labels, policy):
    return {policy["labels"][label] for label in labels if label in policy["labels"]}


def token(value):
    if value is None or (isinstance(value, float) and value != value):
        return ""
    return str(value).strip()


def property_allowed(domain, prop, policy):
    rule = policy["properties"].get(prop)
    return rule is None or domain in rule["allowed"]


def validate_rows(driver, rows, policy=None):
    if not enabled():
        return
    rows = list(rows)
    if not rows:
        return
    policy = policy or load_policy(driver)
    rows = [{**row, "CMID": token(row.get("CMID")), "label": token(row.get("label")).upper()}
            for row in rows]
    metadata = {}

    def fetch_metadata(records):
        needed = {ref for row in records for prop in policy["properties"] for ref in values(row.get(prop))}
        needed.update(token(row.get("CMID")) for row in records)
        needed = sorted(needed - metadata.keys() - {""})
        for start in range(0, len(needed), 1500):
            found = getQuery("""UNWIND $cmids AS cmid OPTIONAL MATCH (n:CATEGORY {CMID:cmid})
                RETURN cmid, labels(n) AS labels""", driver,
                params={"cmids": needed[start:start + 1500]})
            metadata.update({row["cmid"]: node_domains(row.get("labels") or [], policy) for row in found})

    fetch_metadata(rows)
    # Only actual primary-domain changes need to inspect other source records.
    relabeled = {}
    for row in rows:
        cmid, label = row["CMID"], row["label"]
        if cmid.startswith(("AM", "SM")) and label:
            proposed = {policy["labels"].get(label)}
            if metadata.get(cmid) != proposed:
                if cmid in relabeled and relabeled[cmid] != label:
                    raise ValueError(f"Conflicting proposed domains for {cmid}")
                relabeled[cmid] = label
    if relabeled:
        # Check assertions received by the category and assertions referencing it.
        stored = getQuery("""MATCH (:DATASET)-[u:USES]->(c:CATEGORY)
            WHERE c.CMID IN $cmids RETURN c.CMID AS CMID, properties(u) AS properties""",
            driver, params={"cmids": list(relabeled)})
        references = getQuery("""MATCH (:DATASET)-[u:USES]->(c:CATEGORY)
            UNWIND $properties AS prop
            WITH c,u,prop WHERE any(ref IN apoc.coll.flatten([coalesce(u[prop],[])],true) WHERE ref IN $cmids)
            RETURN DISTINCT c.CMID AS CMID,properties(u) AS properties""", driver,
            params={"cmids": list(relabeled), "properties": list(policy["properties"])})
        affected = [{**row["properties"], "CMID": row["CMID"],
                     "label": relabeled.get(row["CMID"], "")} for row in stored + references]
        fetch_metadata(affected)
        rows += affected
        for cmid, label in relabeled.items():
            metadata[cmid] = {policy["labels"].get(label)}
    errors = []
    for number, row in enumerate(rows, 1):
        cmid = str(row.get("CMID") or "").strip()
        if cmid.startswith(("AD", "SD")) or row.get("label") in {"DATASET", "STACK", "MERGING"}:
            continue
        destination = ({policy["labels"].get(row["label"])} if row.get("label")
                       else metadata.get(cmid, set()))
        for prop, rule in policy["properties"].items():
            for ref in values(row.get(prop)):
                if (len(destination) != 1 or not destination.issubset(set(rule["allowed"]))
                        or metadata.get(ref) != {rule["sourceDomain"]}):
                    errors.append(f"row {row.get('_spreadsheet_row', number)} {cmid or '<new category>'}: "
                                  f"{prop}={ref} would create {rule['relationship']} "
                                  f"from {rule['sourceDomain']} to {', '.join(sorted(str(x) for x in destination)) or 'unknown domain'}")
    if errors:
        raise ValueError("Prohibited contextual relationship: " + "; ".join(errors[:20]))


def stored_conflicts(driver, cmids=None, properties=None, policy=None):
    policy = policy or load_policy(driver)
    rules = {key: rule for key, rule in policy["properties"].items()
             if properties is None or key in properties}
    return getQuery("""MATCH (d:DATASET)-[u:USES]->(c:CATEGORY)
        WHERE $cmids IS NULL OR c.CMID IN $cmids
        UNWIND keys($rules) AS prop
        UNWIND apoc.coll.flatten([coalesce(u[prop],[])],true) AS ref
        WITH d,u,c,prop,ref WHERE ref IS NOT NULL AND trim(toString(ref))<>''
        OPTIONAL MATCH (source:CATEGORY {CMID:ref})
        WITH d,u,c,prop,ref,source,
            apoc.coll.toSet([label IN labels(c) WHERE $labelDomains[label] IS NOT NULL | $labelDomains[label]]) AS targets,
            apoc.coll.toSet([label IN coalesce(labels(source),[]) WHERE $labelDomains[label] IS NOT NULL | $labelDomains[label]]) AS sources
        WHERE size(targets)<>1 OR NOT targets[0] IN $rules[prop].allowed
            OR size(sources)<>1 OR sources[0]<>$rules[prop].sourceDomain
        RETURN d.CMID AS datasetID,u.Key AS Key,elementId(u) AS usesID,
            c.CMID AS CMID,c.CMName AS categoryName,prop AS property,ref AS reference,
            source.CMName AS referenceName,
            targets AS targetDomains,sources AS sourceDomains,$rules[prop].relationship AS relationship
        LIMIT 10000""", driver, params={"cmids": cmids, "rules": rules, "labelDomains": policy["labels"]})


def validate_stored(driver, cmids=None, properties=None):
    if not enabled():
        return
    if cmids is not None and not isinstance(cmids, list):
        cmids = [cmids]
    conflicts = stored_conflicts(driver, cmids, properties)
    if conflicts:
        raise ValueError("Stored contextual relationships require review before reconciliation: "
                         + "; ".join(f"{row['datasetID']} {row['CMID']} {row['property']}={row['reference']}"
                                     for row in conflicts[:20]))
